# src/smpp_server.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import asyncio
import struct
import random
import ipaddress
import time
from typing import Iterable, List, Optional, Set, Tuple

from sqlalchemy import text
from smpplib import smpp, pdu

from src.logging_setup import ConnAdapter, configure_smpp_logging
from src.database import SessionLocal
from src import models
from src.smpp_worker import (
    _handle_deliver_sm, get_decoded_text,
    start_concatenation_worker, stop_concatenation_worker,
    run_smpp_provider_loop, ESME_ROK, ESME_RSUBMITFAIL, ESME_RSYSERR,
)
from src.settings import settings

log = configure_smpp_logging(logger_name="smpp.server")

# ---------------- Sequence helper ----------------
class SequenceGenerator:
    def __init__(self) -> None:
        self.sequence = 1
    def next_sequence(self) -> int:
        cur = self.sequence
        self.sequence = (self.sequence + 1) & 0x7FFFFFFF
        return cur

# ---------------- Whitelist helpers ----------------
def _parse_cidrs(raw: str) -> List[ipaddress._BaseNetwork]:
    nets: List[ipaddress._BaseNetwork] = []
    for token in (raw or "").replace(";", ",").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            if "/" not in token:
                ip = ipaddress.ip_address(token)
                token = f"{token}/32" if isinstance(ip, ipaddress.IPv4Address) else f"{token}/128"
            nets.append(ipaddress.ip_network(token, strict=False))
        except ValueError:
            log.debug(f"[yellow]WHITELIST ENV[/]: игнор '{token}'")
    return nets

def _extract_ipv4_from_host(host: str) -> Optional[str]:
    host = (host or "").strip()
    if not host:
        return None
    if ":" in host:
        host = host.split(":", 1)[0].strip()
    try:
        ipaddress.ip_address(host)
        return host
    except ValueError:
        return None

def _load_whitelist_from_env() -> List[ipaddress._BaseNetwork]:
    raw = settings.ALLOWED_SMPP_IPS_RAW or os.getenv("ALLOWED_SMPP_IPS", "")
    nets = _parse_cidrs(raw)
    if settings.SMPP_WHITELIST_ALLOW_LOCALHOST:
        nets.append(ipaddress.ip_network("127.0.0.1/32"))
        try:
            nets.append(ipaddress.ip_network("::1/128"))
        except Exception:
            pass
    return nets

def _load_whitelist_from_db() -> List[ipaddress._BaseNetwork]:
    nets: List[ipaddress._BaseNetwork] = []
    db = SessionLocal()
    try:
        try:
            rows = db.execute(text(
                "SELECT ip_cidr FROM provider_ips WHERE is_active = true AND ip_cidr IS NOT NULL AND ip_cidr <> ''"
            )).fetchall()
            for (cidr,) in rows:
                try:
                    nets.append(ipaddress.ip_network(str(cidr).strip(), strict=False))
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"[yellow]WHITELIST DB[/]: не прочитал provider_ips: {e}")

        try:
            rows2 = db.execute(text(
                "SELECT smpp_host FROM providers WHERE smpp_host IS NOT NULL AND smpp_host <> ''"
            )).fetchall()
            for (host,) in rows2:
                ip = _extract_ipv4_from_host(host)
                if ip:
                    nets.append(ipaddress.ip_network(f"{ip}/32"))
        except Exception as e:
            log.warning(f"[yellow]WHITELIST DB[/]: не прочитал providers: {e}")
    finally:
        db.close()
    return nets

_WHITELIST_CACHE: Tuple[float, List[ipaddress._BaseNetwork]] = (0.0, [])
def build_effective_whitelist() -> List[ipaddress._BaseNetwork]:
    now = time.time()
    ts, cached = _WHITELIST_CACHE
    ttl = max(5, int(settings.SMPP_WHITELIST_REFRESH_SECONDS or 60))
    if now - ts < ttl and cached:
        return cached
    env_nets = _load_whitelist_from_env()
    db_nets = _load_whitelist_from_db() if settings.SMPP_WHITELIST_FROM_DB else []
    seen: Set[str] = set()
    result: List[ipaddress._BaseNetwork] = []
    for n in env_nets + db_nets:
        s = str(n)
        if s not in seen:
            result.append(n)
            seen.add(s)
    short = ", ".join(str(n) for n in result[:5]) + ("..." if len(result) > 5 else "")
    log.info(f"[cyan]WHITELIST[/]: loaded {len(result)} rule(s): {short}")
    globals()["_WHITELIST_CACHE"] = (now, result)
    return result

def is_ip_allowed(ip: str, whitelist: Iterable[ipaddress._BaseNetwork]) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except Exception:
        return False
    for net in whitelist:
        try:
            if addr in net:
                return True
        except Exception:
            pass
    return False

def resolve_provider_id_for_ip(db, ip: str) -> Optional[int]:
    """Строгое сопоставление: IP ∈ CIDR из provider_ips, затем точное совпадение с providers.smpp_host."""
    try:
        addr = ipaddress.ip_address(ip)
        rows = db.execute(text(
            "SELECT provider_id, ip_cidr FROM provider_ips WHERE is_active = true AND ip_cidr IS NOT NULL AND ip_cidr <> ''"
        )).fetchall()
        for pid, cidr in rows:
            try:
                net = ipaddress.ip_network(str(cidr).strip(), strict=False)
                if addr in net:
                    return int(pid)
            except Exception:
                continue
        rows2 = db.execute(text(
            "SELECT id, smpp_host FROM providers WHERE smpp_host IS NOT NULL AND smpp_host <> ''"
        )).fetchall()
        for pid, host in rows2:
            h_ip = _extract_ipv4_from_host(host)
            if h_ip == ip:
                return int(pid)
    except Exception as e:
        log.warning(f"[yellow]RESOLVE PROVIDER[/]: ошибка чтения providers: {e}")
    return None

# ---------------- SMPP low-level helpers ----------------

# Карта: request command -> response command_id
RESP_CMD_ID = {
    "bind_transmitter": 0x80000002,
    "bind_receiver": 0x80000001,
    "bind_transceiver": 0x80000009,
    "enquire_link": 0x80000015,
    "unbind": 0x80000006,
    "submit_sm": 0x80000004,
    "deliver_sm": 0x80000005,
}

def _c_octet(b: bytes) -> bytes:
    """C-Octet String: ASCII bytes + NUL."""
    if not isinstance(b, (bytes, bytearray)):
        b = str(b or "").encode("ascii", "ignore")
    return bytes(b) + b"\x00"

def make_resp_bytes(req: pdu.PDU, status: int, message_id: Optional[bytes] = None, system_id: Optional[bytes] = None) -> bytes:
    """
    РУЧНАЯ сборка RESP PDU: правильный command_id, тот же sequence, корректный command_status.
    Для submit_sm_resp при OK добавляем message_id (C-Octet).
    Для bind_*_resp добавляем system_id (C-Octet).
    Для остальных — тело пустое.
    """
    resp_cmd_id = RESP_CMD_ID.get(req.command)
    if resp_cmd_id is None:
        # неизвестное — отвечаем generic_nack (0x80000000)
        resp_cmd_id = 0x80000000

    seq = getattr(req, "sequence", 0) or 0

    body = b""
    if resp_cmd_id == 0x80000004:  # submit_sm_resp
        if status == ESME_ROK:
            if message_id is None:
                message_id = str(random.randint(10000, 99999)).encode("ascii")
            body = _c_octet(message_id)
        else:
            body = b""
    elif resp_cmd_id in (0x80000001, 0x80000002, 0x80000009):  # bind_*_resp
        if not system_id:
            system_id = b"SMSService"
        body = _c_octet(system_id)

    length = 16 + len(body)
    header = struct.pack(">LLLL", length, resp_cmd_id, int(status) & 0xFFFFFFFF, int(seq) & 0xFFFFFFFF)
    return header + body

# ---------------- DLR helpers ----------------
def _dlr_text(message_id: bytes | str, stat: str = "DELIVRD", sample: str = "") -> bytes:
    """
    Стандартный текст DLR (SMPP 3.4): id:... sub:001 dlvrd:001 submit date:YYMMDDhhmm done date:YYMMDDhhmm stat:... err:000 text:...
    """
    if isinstance(message_id, (bytes, bytearray)):
        mid = message_id.decode("ascii", "ignore")
    else:
        mid = str(message_id or "")
    now = time.strftime("%y%m%d%H%M", time.gmtime())
    preview = (sample or "").replace("\r", " ").replace("\n", " ")[:20]
    s = f"id:{mid} sub:001 dlvrd:001 submit date:{now} done date:{now} stat:{stat} err:000 text:{preview}"
    return s.encode("ascii", "ignore")

def make_dlr_deliver_sm_bytes(req: pdu.PDU, seq_gen: SequenceGenerator, message_id: bytes, stat: str, sample: str) -> bytes:
    """
    Собирает deliver_sm (DLR) и возвращает его сырые байты.
    Источник/получатель меняем местами: src <- destination_addr, dst <- source_addr.
    Добавляем TLV: receipted_message_id, message_state=2 (DELIVERED).
    """
    try:
        # receipted_message_id должен быть строкой (C-Octet String) по спецификации
        if isinstance(message_id, (bytes, bytearray)):
            rmid = message_id.decode("ascii", "ignore")
        else:
            rmid = str(message_id or "0")

        dlr_pdu = smpp.make_pdu(
            "deliver_sm",
            client=seq_gen,
            service_type=b"",
            source_addr_ton=getattr(req, "dest_addr_ton", 0),
            source_addr_npi=getattr(req, "dest_addr_npi", 0),
            source_addr=getattr(req, "destination_addr", b""),
            dest_addr_ton=getattr(req, "source_addr_ton", 0),
            dest_addr_npi=getattr(req, "source_addr_npi", 0),
            destination_addr=getattr(req, "source_addr", b""),
            esm_class=0x04,  # Delivery Receipt
            protocol_id=0,
            priority_flag=0,
            schedule_delivery_time=b"",
            validity_period=b"",
            registered_delivery=0,
            replace_if_present_flag=0,
            data_coding=0,
            sm_default_msg_id=0,
            short_message=_dlr_text(message_id, stat=stat, sample=sample),
            optional_parameters={
                "receipted_message_id": rmid,
                "message_state": 2,  # DELIVERED
            },
        )
        return dlr_pdu.generate()
    except Exception as e:
        log.error(f"Не удалось собрать DLR deliver_sm: {e}", exc_info=True)
        return b""
# -------------- end DLR helpers --------------

# ---------------- SMPP PDU read ----------------
async def read_pdu_from_stream(reader: asyncio.StreamReader, seq_gen: SequenceGenerator) -> pdu.PDU:
    len_bytes = await reader.readexactly(4)
    length = struct.unpack(">L", len_bytes)[0]
    if not (16 <= length <= 65536):
        raise ValueError(f"Некорректная длина PDU: {length}")
    body = await reader.readexactly(length - 4)
    return smpp.parse_pdu(len_bytes + body, client=seq_gen, allow_unknown_opt_params=True)

# ---------------- SMPP session handler ----------------
async def smpp_session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    client_ip = peer[0] if peer else "?"
    whitelist = build_effective_whitelist()

    if not is_ip_allowed(client_ip, whitelist):
        log.warning(f"[red]Отклонено соединение[/] с {client_ip}: IP не в whitelist")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        return

    L = ConnAdapter(log, {"conn": client_ip})
    seq = SequenceGenerator()

    L.info("Новое подключение. Ожидаю BIND...")
    bound = False
    system_id_str = "?"
    provider_id_hint: Optional[int] = None
    provider_name_hint = "Unknown" # <<< ДОБАВЛЕНО

    try:
        while True:
            try:
                current_pdu = await read_pdu_from_stream(reader, seq)
            except (ValueError, asyncio.IncompleteReadError) as e:
                if isinstance(e, asyncio.IncompleteReadError):
                    L.info("Клиент отключился.")
                    break
                L.info("[yellow]Получен некорректный пакет. Жду следующий...[/]")
                continue

            if not bound:
                if current_pdu.command in ("bind_transceiver", "bind_transmitter", "bind_receiver"):
                    try:
                        system_id_str = current_pdu.system_id.decode("ascii", "ignore")
                    except Exception:
                        system_id_str = "?"
                    L.info(f"Получен [cyan]BIND[/] от system_id='{system_id_str}' ([magenta]{current_pdu.command}[/])")
                    writer.write(make_resp_bytes(current_pdu, ESME_ROK, system_id=b"SMSService"))
                    await writer.drain()
                    L.info("Авторизация успешна ([green]BOUND[/]). Слушаю входящие PDU...")
                    bound = True
                    with SessionLocal() as db_bind:
                        provider_id_hint = resolve_provider_id_for_ip(db_bind, client_ip)
                        if provider_id_hint:
                            L.info(f"[cyan]RESOLVED[/] provider_id={provider_id_hint} по IP {client_ip}")
                            # <<< НАЧАЛО ДОБАВЛЕННОГО БЛОКА
                            prov_obj = db_bind.query(models.Provider.name).filter(models.Provider.id == provider_id_hint).first()
                            if prov_obj:
                                provider_name_hint = prov_obj.name
                            # <<< КОНЕЦ ДОБАВЛЕННОГО БЛОКА
                        else:
                            L.warning(f"[yellow]RESOLVED[/]: не удалось определить provider_id по IP {client_ip}")
                elif current_pdu.command == "enquire_link":
                    L.warning(f"[yellow]Получен {current_pdu.command} до BIND[/] — отвечаю OK и жду bind_*")
                    writer.write(make_resp_bytes(current_pdu, ESME_ROK))
                    await writer.drain()
                else:
                    L.warning(f"[yellow]Команда {current_pdu.command} до BIND[/] — игнорирую")
                continue

            if current_pdu.command in ("enquire_link",):
                writer.write(make_resp_bytes(current_pdu, ESME_ROK))
                await writer.drain()
                continue
            if current_pdu.command in ("unbind",):
                writer.write(make_resp_bytes(current_pdu, ESME_ROK))
                await writer.drain()
                L.info("Получен UNBIND — закрываю соединение")
                break

            p = current_pdu
            if p.command not in ("submit_sm", "deliver_sm"):
                continue

            # лог для человека
            try:
                src = (p.source_addr or b"").decode("ascii", "ignore")
                dst = (p.destination_addr or b"").decode("ascii", "ignore")
                clean = get_decoded_text(p).replace("\r", " ").replace("\n", " ")
            except Exception:
                src, dst, clean = "?", "?", ""

            with SessionLocal() as db:
                try:
                    ctx = {"client_ip": client_ip, "system_id": system_id_str, "provider_id": provider_id_hint}
                    res = _handle_deliver_sm(p, db, ctx)
                    if isinstance(res, dict):
                        status = int(res.get("status", ESME_RSYSERR))
                        is_orphan = bool(res.get("is_orphan", False))
                    else:
                        status = int(res)
                        is_orphan = (status == ESME_RSUBMITFAIL)
                except Exception as e:
                    L.error(f"[red]Ошибка обработки в воркере[/]: {e}")
                    status, is_orphan = ESME_RSYSERR, False

            # ЖЁСТКО: если это осиротевшее, всегда отдаём 69
            if is_orphan:
                status = ESME_RSUBMITFAIL

            verdict = (
                "[green]OUR (0) OK[/]" if status == ESME_ROK else
                "[yellow]ORPHAN (69) REJECT[/]" if status == ESME_RSUBMITFAIL else
                "[red]SYSERR (8)[/]" if status == ESME_RSYSERR else
                f"[magenta]STATUS {status}[/]"
            )
            # <<< ЕДИНСТВЕННОЕ ИЗМЕНЕНИЕ ЛОГА >>>
            L.info(
                f"{verdict}: {p.command} src='{src}' dst='{dst}' | "
                f"provider={provider_name_hint}({provider_id_hint if provider_id_hint is not None else 'NULL'}) "
                f"ip={client_ip} sid={system_id_str} text='{clean[:200]}'"
            )

            # Ответ (вручную, с правильным sequence)
            body_msg_id = None
            if p.command == "submit_sm" and status == ESME_ROK:
                body_msg_id = str(random.randint(10000, 99999)).encode("ascii")
            resp_bytes = make_resp_bytes(p, status, message_id=body_msg_id)
            try:
                writer.write(resp_bytes)
                await writer.drain()
            except Exception as e:
                L.error(f"ОШИБКА ПРИ ОТПРАВКЕ ОТВЕТА: {e}", exc_info=True)

            # НОВОЕ: если submit_sm принят (OK) — сразу шлём DLR (deliver_sm с esm_class=0x04 + TLV)
            try:
                if p.command == "submit_sm" and status == ESME_ROK:
                    dlr_bytes = make_dlr_deliver_sm_bytes(
                        req=p,
                        seq_gen=seq,
                        message_id=body_msg_id or b"0",
                        stat="DELIVRD",
                        sample=clean,
                    )
                    if dlr_bytes:
                        writer.write(dlr_bytes)
                        await writer.drain()
                        L.info("[green]DLR отправлен[/]: stat=DELIVRD id=%s",
                               (body_msg_id or b'0').decode('ascii', 'ignore'))
            except Exception as e:
                L.error(f"ОШИБКА ПРИ ОТПРАВКЕ DLR: {e}", exc_info=True)

    except Exception as e:
        log.exception(f"[red]Критическая ошибка соединения[/]: {e}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        L.info("Сессия закрыта")

# ---------------- Outbound (мы к ним) ----------------
def _is_outbound(p: models.Provider) -> bool:
    t = (p.connection_type or "").strip().lower()
    return t in ("outbound", "out", "client", "мы к ним")

def start_outbound_from_db():
    import threading
    stop_evt = threading.Event()
    threads = []
    with SessionLocal() as db:
        providers = db.query(models.Provider).filter(models.Provider.is_active == True).all()
    for p in providers:
        if _is_outbound(p):
            t = threading.Thread(target=run_smpp_provider_loop, args=(p, stop_evt), daemon=True)
            t.start()
            threads.append((p.name, t))
            log.info(f"[OUTBOUND] started thread for provider {p.name}")
    return stop_evt, threads

def stop_outbound(stop_evt, threads):
    try:
        stop_evt.set()
        for name, t in threads:
            t.join(timeout=2.0)
            if t.is_alive():
                log.warning(f"[OUTBOUND] thread did not stop timely: {name}")
            else:
                log.info(f"[OUTBOUND] thread stopped: {name}")
    except Exception:
        pass

# ---------------- Entrypoint ----------------
async def main():
    host = settings.SMPP_BIND_HOST or "0.0.0.0"
    ports = settings.get_smpp_bind_ports()

    servers: List[asyncio.AbstractServer] = []
    try:
        stop_concatenation_worker()
    except Exception:
        pass
    start_concatenation_worker()

    outbound_stop_evt = None
    outbound_threads = []
    try:
        for port in ports:
            srv = await asyncio.start_server(smpp_session, host, port)
            servers.append(srv)
            addr = ", ".join(str(s.getsockname()) for s in srv.sockets)
            log.info(f"[bold]SMPP inbound слушает[/]: {addr}")

        try:
            outbound_stop_evt, outbound_threads = start_outbound_from_db()
        except Exception as e:
            log.warning(f"[yellow]OUTBOUND[/]: не удалось запустить: {e}")

        await asyncio.gather(*[s.serve_forever() for s in servers])

    except asyncio.CancelledError:
        pass
    finally:
        for srv in servers:
            srv.close()
            await srv.wait_closed()
        stop_concatenation_worker()
        if outbound_stop_evt is not None:
            stop_outbound(outbound_stop_evt, outbound_threads)
        log.info("[bold]Сервер полностью остановлен[/]")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Получен KeyboardInterrupt — останавливаемся...")
