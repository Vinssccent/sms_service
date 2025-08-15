# src/smpp_server.py
# -*- coding: utf-8 -*-
"""
SMPP-сервер c цветным логом (Rich):
- Слушает 40000 (или список из SMPP_BIND_PORTS).
- Белый список IP: ENV (ALLOWED_SMPP_IPS_RAW/ALLOWED_SMPP_IPS) + БД (provider_ips, providers.smpp_host).
- На каждую SMS — одна строка лога: OUR(0) / ORPHAN(69) / SYSERR(8).
- Бизнес-ошибки не рвут сессию: отвечаем 8 и продолжаем.
"""

import os
import asyncio
import struct
import random
import ipaddress
import logging
from typing import Iterable, List, Optional, Set

from sqlalchemy import text
from smpplib import smpp, pdu

from src.database import SessionLocal
from src.smpp_worker import (
    _handle_deliver_sm,
    get_decoded_text,
    start_concatenation_worker,
    stop_concatenation_worker,
    ESME_ROK,          # 0
    ESME_RSUBMITFAIL,  # 69
    ESME_RSYSERR,      # 8
)
from src.logging_setup import setup_logging, ConnAdapter

# Цветной лог (Rich)
setup_logging()
log = logging.getLogger("smpp.server")
# Убрать дубли: глушим инфо-логи воркера внутри этого процесса
logging.getLogger("src.smpp_worker").setLevel(logging.WARNING)


# -------------------------
# SMPP helpers
# -------------------------
class SequenceGenerator:
    def __init__(self) -> None:
        self.sequence = 1
    def next_sequence(self) -> int:
        cur = self.sequence
        self.sequence = (self.sequence + 1) & 0x7FFFFFFF
        return cur


async def read_pdu_from_stream(reader: asyncio.StreamReader, seq_gen: SequenceGenerator) -> pdu.PDU:
    len_bytes = await reader.readexactly(4)
    length = struct.unpack(">L", len_bytes)[0]
    if not (16 <= length <= 65536):
        raise ValueError(f"Некорректная длина PDU: {length}")
    body = await reader.readexactly(length - 4)
    return smpp.parse_pdu(len_bytes + body, client=seq_gen)


def make_resp(req: pdu.PDU, status: int, seq_gen: SequenceGenerator) -> pdu.PDU:
    resp_cmd = req.command + "_resp"
    params = {"sequence": req.sequence, "status": status, "client": seq_gen}
    if resp_cmd == "submit_sm_resp":
        params["message_id"] = str(random.randint(10000, 99999)).encode()
    resp = smpp.make_pdu(resp_cmd, **params)
    if resp_cmd == "bind_transceiver_resp":
        resp.system_id = b"SMSService"
    return resp


# -------------------------
# Whitelist helpers
# -------------------------
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
        host = host.split(":", 1)[0]
    parts = host.split(".")
    if len(parts) == 4 and all(p.isdigit() and 0 <= int(p) <= 255 for p in parts):
        return host
    return None


def _load_whitelist_from_env() -> List[ipaddress._BaseNetwork]:
    raw = os.getenv("ALLOWED_SMPP_IPS_RAW")
    if raw is None:
        raw = os.getenv("ALLOWED_SMPP_IPS", "")
    nets = _parse_cidrs(raw)
    if os.getenv("SMPP_WHITELIST_ALLOW_LOCALHOST", "true").lower() in ("1", "true", "yes"):
        nets.append(ipaddress.ip_network("127.0.0.1/32"))
    return nets


def _load_whitelist_from_db() -> List[ipaddress._BaseNetwork]:
    nets: List[ipaddress._BaseNetwork] = []
    db = SessionLocal()
    try:
        try:
            rows = db.execute(text("SELECT ip_cidr FROM provider_ips WHERE is_active = TRUE")).fetchall()
            for (cidr,) in rows:
                try:
                    nets.append(ipaddress.ip_network(cidr, strict=False))
                except ValueError:
                    log.debug(f"[yellow]WHITELIST DB[/]: игнор '{cidr}'")
        except Exception as e:
            log.warning(f"[yellow]WHITELIST DB[/]: не прочитал provider_ips: {e}")
        try:
            rows2 = db.execute(
                text("SELECT smpp_host FROM providers WHERE smpp_host IS NOT NULL AND smpp_host <> ''")
            ).fetchall()
            for (host,) in rows2:
                ip = _extract_ipv4_from_host(host)
                if ip:
                    nets.append(ipaddress.ip_network(f"{ip}/32"))
        except Exception as e:
            log.warning(f"[yellow]WHITELIST DB[/]: не прочитал providers: {e}")
    finally:
        db.close()
    return nets


def build_effective_whitelist() -> List[ipaddress._BaseNetwork]:
    env_nets = _load_whitelist_from_env()
    db_nets = _load_whitelist_from_db()
    seen: Set[str] = set()
    result: List[ipaddress._BaseNetwork] = []
    for n in env_nets + db_nets:
        s = str(n)
        if s not in seen:
            result.append(n)
            seen.add(s)
    short = ", ".join(str(n) for n in result[:5]) + ("..." if len(result) > 5 else "")
    log.info(f"[cyan]WHITELIST[/]: loaded {len(result)} rule(s): {short}")
    return result


def is_ip_allowed(ip: str, whitelist: Iterable[ipaddress._BaseNetwork]) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(ip_obj in net for net in whitelist)


# -------------------------
# SMPP session
# -------------------------
async def smpp_session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    client_ip = (peer[0] if isinstance(peer, tuple) else str(peer)).split(":")[0]
    conn_id = f"{client_ip}:{peer[1] if isinstance(peer, tuple) else '-'}"
    L = ConnAdapter(log, {"conn": conn_id})

    L.info(f"[bold]Новое подключение[/]: {peer}")

    whitelist = build_effective_whitelist()
    if whitelist and not is_ip_allowed(client_ip, whitelist):
        L.warning("[red]WHITELIST[/]: отклонено подключение")
        writer.close()
        try:
            await writer.wait_closed()
        finally:
            return

    seq = SequenceGenerator()
    bound = False

    try:
        first_pdu = await read_pdu_from_stream(reader, seq)
        if first_pdu.command not in ("bind_transceiver", "bind_transmitter", "bind_receiver"):
            raise ValueError("Первый PDU не bind_*")

        try:
            system_id = first_pdu.system_id.decode("ascii", "ignore")
        except Exception:
            system_id = "?"
        L.info(f"Получен [cyan]BIND[/] от system_id='{system_id}' ([magenta]{first_pdu.command}[/])")

        bound = True
        writer.write(make_resp(first_pdu, ESME_ROK, seq).generate())
        await writer.drain()
        L.info("Авторизация успешна ([green]BOUND[/]). Слушаю входящие PDU...")

        while bound:
            p = await read_pdu_from_stream(reader, seq)

            if p.command == "unbind":
                bound = False
                writer.write(make_resp(p, ESME_ROK, seq).generate())
                await writer.drain()
                L.info("[yellow]UNBIND[/] от клиента")
                continue

            if p.command == "enquire_link":
                writer.write(make_resp(p, ESME_ROK, seq).generate())
                await writer.drain()
                L.debug("Ответил на ENQUIRE_LINK")
                continue

            if p.command not in ("submit_sm", "deliver_sm"):
                continue

            # RX summary
            try:
                src = (p.source_addr or b"").decode("ascii", "ignore")
                dst = (p.destination_addr or b"").decode("ascii", "ignore")
                clean = get_decoded_text(p).replace("\r", " ").replace("\n", " ")
            except Exception as e:
                src, dst, clean = "?", "?", ""
                L.warning(f"Ошибка при формировании RX: {e}")

            # Обработка с защитой: любые наши ошибки -> SYSERR(8), но сессию не рвём
            db = SessionLocal()
            try:
                try:
                    status = _handle_deliver_sm(p, db)
                except Exception as e:
                    L.error(f"[red]Ошибка обработки в воркере[/]: {e}")
                    status = ESME_RSYSERR
            finally:
                db.close()

            # ЕДИНЫЙ итоговый лог
            if status == ESME_ROK:
                verdict = "[green]OUR (0) OK[/]"
            elif status == ESME_RSUBMITFAIL:
                verdict = "[yellow]ORPHAN (69) REJECT[/]"
            elif status == ESME_RSYSERR:
                verdict = "[red]SYSERR (8)[/]"
            else:
                verdict = f"[magenta]STATUS {status}[/]"

            L.info(f"{verdict}: {p.command} src='{src}' dst='{dst}' text='{clean[:200]}'")

            # Ответ клиенту
            writer.write(make_resp(p, status, seq).generate())
            await writer.drain()

    except asyncio.IncompleteReadError:
        L.info("Клиент отключился")
    except ValueError as e:
        # Например, «Некорректная длина PDU»: восстановить фрейминг невозможно — закрываем.
        L.warning(f"[yellow]Некорректный пакет[/]: {e}. Соединение закрыто.")
    except Exception as e:
        L.exception(f"[red]Непредвиденная ошибка[/]: {e}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        finally:
            L.info("Сессия закрыта")


# -------------------------
# Server bootstrap
# -------------------------
async def main():
    host = os.getenv("SMPP_BIND_HOST", "0.0.0.0")

    ports: List[int] = []
    env_ports = os.getenv("SMPP_BIND_PORTS", "")
    if env_ports.strip():
        for t in env_ports.replace(";", ",").split(","):
            t = t.strip()
            if t.isdigit():
                ports.append(int(t))
    else:
        p = os.getenv("SMPP_BIND_PORT", "40000")
        try:
            ports.append(int(p))
        except ValueError:
            ports.append(40000)

    servers: List[asyncio.AbstractServer] = []
    for port in ports:
        srv = await asyncio.start_server(smpp_session, host, port)
        log.info(f"[bold cyan]SMPP-сервер слушает[/] {host}:{port}")
        servers.append(srv)

    start_concatenation_worker()

    try:
        await asyncio.gather(*(srv.serve_forever() for srv in servers))
    finally:
        for srv in servers:
            srv.close()
            await srv.wait_closed()
        stop_concatenation_worker()
        log.info("[bold]Сервер полностью остановлен[/]")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Получен KeyboardInterrupt — останавливаемся...")
