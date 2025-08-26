# src/smpp_worker.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time
import threading
import re
from typing import Optional, Any, Dict, Tuple, Callable

# --- optional Redis (используем, если доступен) ---
try:
    import redis  # type: ignore
    _redis_import_ok = True
except Exception:
    redis = None  # type: ignore
    _redis_import_ok = False

import smpplib.client  # type: ignore
import smpplib.exceptions  # type: ignore

from sqlalchemy.orm import Session as SASession, selectinload
from sqlalchemy.exc import SQLAlchemyError

from src.database import SessionLocal
from src import models
from src.utils import normalize_phone_number, build_phone_candidates

log = logging.getLogger("src.smpp_worker")

# ===============================
# SMPP status constants (subset)
# ===============================
ESME_ROK = 0                  # OK
ESME_RSYSERR = 8              # System error
ESME_RSUBMITFAIL = 69         # submit_sm failed (для REJECT живого трафика)

# ===============================
# Redis client (optional)
# ===============================
redis_client = None
if _redis_import_ok:
    try:
        redis_client = redis.Redis(decode_responses=True)  # type: ignore
        redis_client.ping()
        log.info("SMPP Worker: Redis connected")
    except Exception as e:
        log.warning("SMPP Worker: Redis not available (%s) — continue without it", e)
        redis_client = None

# ===============================
# Concat buffer (in-memory)
# ===============================
_concat_buf: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
_concat_lock = threading.Lock()
_CONCAT_TTL_SEC = 300  # 5 минут

# GC поток для буфера конкатенации
_gc_stop = threading.Event()
_gc_thread: Optional[threading.Thread] = None


# ===============================
# Decoding helpers
# ===============================
def _sanitize_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\x00", "")
    return "".join(ch for ch in s if (ch >= " " or ch in "\n\r\t"))


def _decode_bytes(msg: bytes | memoryview | None, data_coding: int) -> str:
    if not msg:
        return ""
    if isinstance(msg, memoryview):
        msg = msg.tobytes()

    try:
        if data_coding == 8:  # UCS2 (UTF-16BE)
            return _sanitize_text(bytes(msg).decode("utf-16be", errors="ignore"))
        if data_coding == 0:  # GSM 7-bit / ASCII fallback
            try:
                import smpplib.gsm  # type: ignore
                return _sanitize_text(smpplib.gsm.decode(bytes(msg)))
            except Exception:
                return _sanitize_text(bytes(msg).decode("ascii", errors="ignore"))
        return _sanitize_text(bytes(msg).decode("latin1", errors="ignore"))
    except Exception:
        return _sanitize_text(bytes(msg).decode("latin1", errors="ignore"))


def _parse_udh(sm: bytes) -> Tuple[bytes, Optional[Tuple[int, int, int]]]:
    if not sm:
        return sm, None
    try:
        udhl = sm[0]
        if udhl == 0 or (1 + udhl) > len(sm):
            return sm, None
        udh = sm[1:1+udhl]
        i = 0
        ref = total = seq = None
        while i < len(udh):
            iei = udh[i]; i += 1
            if i >= len(udh): break
            ielen = udh[i]; i += 1
            if i + ielen > len(udh): break
            iedata = udh[i:i+ielen]; i += ielen
            if iei == 0x00 and ielen == 3:
                ref = iedata[0]; total = iedata[1]; seq = iedata[2]
            elif iei == 0x08 and ielen == 4:
                ref = (iedata[0] << 8) | iedata[1]; total = iedata[2]; seq = iedata[3]
        if ref is not None and total and seq:
            return sm[1+udhl:], (int(ref), int(total), int(seq))
        return sm, None
    except Exception:
        return sm, None


def _store_concat_piece(src: str, dst: str, ref: int, total: int, seq: int, piece_text: str) -> Optional[str]:
    key = (src or "", dst or "", int(ref))
    now = time.time()
    with _concat_lock:
        bucket = _concat_buf.get(key)
        if not bucket:
            bucket = {"total": int(total), "parts": {}, "ts": now}
            _concat_buf[key] = bucket
        bucket["ts"] = now
        bucket["total"] = max(int(total), int(bucket["total"] or total))
        bucket["parts"][int(seq)] = piece_text

        if bucket["total"] and len(bucket["parts"]) >= bucket["total"]:
            full = "".join(bucket["parts"][i] for i in sorted(bucket["parts"]))
            _concat_buf.pop(key, None)
            return full
    return None


def _maybe_reassemble_concat(pdu, src: str, dst: str) -> Tuple[str, bool]:
    data_coding = getattr(pdu, "data_coding", 0) or 0
    try:
        mp = getattr(pdu, "message_payload", None)
        if mp:
            return _decode_bytes(mp, data_coding), True
    except Exception:
        pass
    try:
        sm = getattr(pdu, "short_message", b"") or b""
        if isinstance(sm, memoryview):
            sm = sm.tobytes()
    except Exception:
        sm = b""
    esm_class = getattr(pdu, "esm_class", 0) or 0
    has_udh = bool(esm_class & 0x40) and sm
    if has_udh:
        payload, concat = _parse_udh(sm)
        if concat:
            ref, total, seq = concat
            piece = _decode_bytes(payload, data_coding)
            full = _store_concat_piece(src, dst, ref, total, seq, piece)
            if full is not None:
                return full, True
            return piece, False
        return _decode_bytes(payload, data_coding), True
    try:
        ref = getattr(pdu, "sar_msg_ref_num", None)
        total = getattr(pdu, "sar_total_segments", None)
        seq = getattr(pdu, "sar_segment_seqnum", None)
        if ref and total and seq:
            piece = _decode_bytes(sm, data_coding)
            full = _store_concat_piece(src, dst, int(ref), int(total), int(seq), piece)
            if full is not None:
                return full, True
            return piece, False
    except Exception:
        pass
    return _decode_bytes(sm, data_coding), True


def _extract_src_dst(pdu) -> Tuple[str, str]:
    try:
        s = getattr(pdu, "source_addr", b"") or b""
        src = s.decode("latin1", "ignore") if isinstance(s, (bytes, bytearray, memoryview)) else str(s)
    except Exception:
        src = ""
    try:
        d = getattr(pdu, "destination_addr", b"") or b""
        dst = d.decode("latin1", "ignore") if isinstance(d, (bytes, bytearray, memoryview)) else str(d)
    except Exception:
        dst = ""
    return (src or "").strip(), (dst or "").strip()


_COUNTRY_CACHE: Optional[Dict[str, int]] = None

def _load_country_cache(db: SASession) -> Dict[str, int]:
    global _COUNTRY_CACHE
    if _COUNTRY_CACHE is not None:
        return _COUNTRY_CACHE
    mapping: Dict[str, int] = {}
    try:
        rows = db.query(models.Country.id, models.Country.phone_code).all()
        for cid, pcode in rows:
            if not pcode: continue
            code = str(pcode).strip().lstrip("+")
            if code.isdigit(): mapping[code] = cid
    except Exception as e:
        log.warning("Не удалось загрузить справочник стран: %s", e)
    _COUNTRY_CACHE = mapping
    return mapping

def _resolve_country_id_by_msisdn(db: SASession, number_str: str) -> Optional[int]:
    if not number_str: return None
    digits = "".join(ch for ch in number_str if ch.isdigit())
    if not digits: return None
    m = _load_country_cache(db)
    for ln in (4, 3, 2, 1):
        if len(digits) >= ln:
            prefix = digits[:ln]
            if prefix in m: return m[prefix]
    return None


def _find_active_session(db: SASession, dst_raw: str) -> Optional[models.Session]:
    if not dst_raw: return None
    cands = build_phone_candidates(dst_raw) or [dst_raw]
    try:
        return (
            db.query(models.Session)
              .options(selectinload(models.Session.service))
              .filter(models.Session.phone_number_str.in_(cands))
              .filter(models.Session.status.in_([1, 3]))
              .order_by(models.Session.id.desc())
              .first()
        )
    except Exception as e:
        log.exception("DB: _find_active_session failed: %s", e)
        return None


_code_re = re.compile(r'(?<!\d)(\d{4,8})(?!\d)')

def _extract_code(text: str) -> Optional[str]:
    m = _code_re.search(text or "")
    return m.group(1) if m else None

def _store_sms_for_session(db: SASession, sess: models.Session, src: str, text: str) -> bool:
    try:
        msg = models.SmsMessage(session_id=sess.id, source_addr=(src or "")[:255], text=text or "", code=_extract_code(text))
        db.add(msg)
        try:
            sess.status = 2
        except Exception: pass
        db.commit()
        return True
    except Exception as e:
        log.error("DB: failed to store SmsMessage for session_id=%s: %s", getattr(sess, "id", None), e, exc_info=True)
        try: db.rollback()
        except Exception: pass
        return False

def _store_orphan(db: SASession, dst: str, src: str, text: str, ctx: Optional[Dict[str, Any]]) -> bool:
    try:
        norm_dst = normalize_phone_number(dst) or dst or ""
        prov_id = (ctx or {}).get("provider_id")
        country_id, operator_id = None, None
        pn = db.query(models.PhoneNumber).filter(models.PhoneNumber.number_str == norm_dst).first()
        if pn:
            prov_id = prov_id or pn.provider_id
            country_id, operator_id = pn.country_id, pn.operator_id
        if not country_id:
            cid = _resolve_country_id_by_msisdn(db, norm_dst)
            if cid: country_id = cid
        rec = models.OrphanSms(
            phone_number_str=norm_dst, source_addr=(src or "")[:255], text=text or "",
            provider_id=prov_id, country_id=country_id, operator_id=operator_id,
            client_ip=(ctx or {}).get("client_ip"), system_id=(ctx or {}).get("system_id"),
        )
        db.add(rec)
        db.commit()
        return True
    except Exception as e:
        log.error("DB: failed to store OrphanSms for dst=%s: %s", dst, e, exc_info=True)
        try: db.rollback()
        except Exception: pass
        return False

def get_decoded_text(pdu) -> str:
    src, dst = _extract_src_dst(pdu)
    text, _ = _maybe_reassemble_concat(pdu, src, dst)
    return text


# =================================================================
# >>> НАЧАЛО ОБНОВЛЕННОЙ ЛОГИКИ С УЧЕТОМ NITRO <<<
# =================================================================
def is_sender_allowed_for_service(sender: str, service: models.Service) -> bool:
    """
    Обновленная функция для гибкой проверки отправителя.
    """
    if not service:
        return False

    sender_lower = sender.lower()
    
    if service.allowed_senders:
        # Если в поле стоит одна звездочка, сервис "всеядный" - разрешаем всё.
        if service.allowed_senders.strip() == '*':
            return True

        # Проверка по списку разрешенных отправителей
        allowed_list = {s.strip().lower() for s in service.allowed_senders.split(',') if s.strip()}
        return sender_lower in allowed_list

    # Если списка нет, проверка по основному имени сервиса
    service_name_lower = (service.name or "").lower()
    return sender_lower == service_name_lower
# =================================================================
# >>> КОНЕЦ ОБНОВЛЕННОЙ ЛОГИКИ <<<
# =================================================================


def _handle_deliver_sm(pdu, db: SASession, ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        src, dst_raw = _extract_src_dst(pdu)
    except Exception as e:
        log.exception("[WORKЕР] Ошибка извлечения src/dst: %s", e)
        return {"status": ESME_RSYSERR, "is_orphan": False}

    sess = None
    try:
        sess = _find_active_session(db, dst_raw)
    except Exception as e:
        log.exception("[WORKЕР] Ошибка поиска сессии: %s", e)
        try: db.rollback()
        except Exception: pass
        sess = None

    try:
        if not sess and redis_client:
            candidates = build_phone_candidates(dst_raw) or []
            norm_plus = next((c for c in candidates if c.startswith('+')), None)
            if norm_plus and redis_client.exists(f"pending_session:{norm_plus}"):
                log.info("Гонка: %s в pending_session — ждём 200мс и повторяем поиск...", norm_plus)
                time.sleep(0.2)
                db.expire_all()
                sess = _find_active_session(db, dst_raw)
    except Exception:
        pass

    try:
        text, is_complete = _maybe_reassemble_concat(pdu, src, dst_raw)
    except Exception as e:
        log.exception("[WORKЕР] Ошибка сборки/декода: %s", e)
        text, is_complete = ("", True)

    if sess:
        if is_sender_allowed_for_service(src, sess.service):
            # Отправитель разрешен! Обрабатываем как "наше" SMS.
            if is_complete:
                ok = _store_sms_for_session(db, sess, src, text)
                return {"status": ESME_ROK if ok else ESME_RSYSERR, "is_orphan": False}
            return {"status": ESME_ROK, "is_orphan": False}
        else:
            # Отправитель НЕ разрешен! Считаем SMS "осиротевшим".
            service_name = sess.service.name if sess.service else "N/A"
            log.warning(
                f"SENDER MISMATCH: SMS от '{src}' для номера {dst_raw} "
                f"не разрешен для сервиса '{service_name}' (ID сессии: {sess.id}). "
                f"СМС будет отклонено."
            )
            if is_complete:
                _store_orphan(db, dst_raw, src, text, ctx)
            return {"status": ESME_RSUBMITFAIL, "is_orphan": True}

    if is_complete:
        ok = _store_orphan(db, dst_raw, src, text, ctx)
        return {"status": ESME_RSUBMITFAIL if ok else ESME_RSYSERR, "is_orphan": True}

    part_text = f"[PART] {text}" if text else "[PART]"
    ok = _store_orphan(db, dst_raw, src, part_text, ctx)
    return {"status": ESME_RSUBMITFAIL if ok else ESME_RSYSERR, "is_orphan": True}

def _handle_submit_sm(pdu, db: SASession, ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _handle_deliver_sm(pdu, db, ctx)

def _cstr(x: Optional[str]) -> bytes:
    return (x or "").encode("latin1", errors="ignore")

def _bind_trx(client: smpplib.client.Client, system_id: str, password: str, system_type: Optional[str] = None) -> bool:
    try:
        client.bind_transceiver(system_id=_cstr(system_id), password=_cstr(password), system_type=_cstr(system_type))
        log.info("BIND TRX OK (system_id=%s, system_type=%s)", system_id, system_type or "")
        return True
    except smpplib.exceptions.PDUError as e:
        log.error("BIND TRX PDUError: %s", e)
    except Exception as e:
        log.exception("BIND TRX failed: %s", e)
    return False

def _bind_tx(client: smpplib.client.Client, system_id: str, password: str, system_type: Optional[str] = None) -> bool:
    try:
        client.bind_transmitter(system_id=_cstr(system_id), password=_cstr(password), system_type=_cstr(system_type))
        log.info("BIND TX OK (system_id=%s)", system_id)
        return True
    except Exception as e:
        log.exception("BIND TX failed: %s", e)
        return False

def _bind_rx(client: smpplib.client.Client, system_id: str, password: str, system_type: Optional[str] = None) -> bool:
    try:
        client.bind_receiver(system_id=_cstr(system_id), password=_cstr(password), system_type=_cstr(system_type))
        log.info("BIND RX OK (system_id=%s)", system_id)
        return True
    except Exception as e:
        log.exception("BIND RX failed: %s", e)
        return False

def _safe_set_handler(client: smpplib.client.Client, *args) -> None:
    if len(args) == 1: handler = args[0]
    elif len(args) >= 2: handler = args[1]
    else: return
    def wrapper(pdu: Any):
        try: handler(pdu)
        except Exception: log.exception("Unhandled exception in message handler")
    try: client.set_message_received_handler(wrapper)
    except Exception: log.exception("Failed to set message handler")

def run_smpp_provider_loop(provider: models.Provider, stop_evt: threading.Event):
    host = (provider.smpp_host or "").split(":")[0].strip()
    try: port = int(provider.smpp_port or 2775)
    except Exception: port = 2775
    sid, pwd, stype = provider.system_id or "", provider.password or "", provider.system_type or ""
    while not stop_evt.is_set():
        client = None
        try:
            log.info("[OUTBOUND] Подключаюсь к %s:%s...", host, port)
            client = smpplib.client.Client(host, port)
            client.connect()
            if not _bind_trx(client, sid, pwd, stype): raise RuntimeError("BIND failed")
            def _on_msg(pdu_obj: Any) -> None:
                if getattr(pdu_obj, "command", "").lower() != "deliver_sm": return
                db = SessionLocal()
                try:
                    ctx = {"provider_id": provider.id, "system_id": provider.system_id, "client_ip": None}
                    res = _handle_deliver_sm(pdu_obj, db, ctx)
                    log.debug("[OUTBOUND] deliver_sm handled: %s", res)
                finally: db.close()
            _safe_set_handler(client, "message_received", _on_msg)
            last_any_io = time.time()
            while not stop_evt.is_set():
                if client.read_once(): last_any_io = time.time()
                if time.time() - last_any_io > 180:
                    log.warning("[OUTBOUND] Нет активности >180 сек. Переподключаюсь.")
                    break
                time.sleep(0.05)
        except Exception as e:
            log.error("[OUTBOUND] Ошибка в главном цикле: %s", e, exc_info=False)
        finally:
            try:
                if client:
                    try: client.unbind()
                    except Exception: pass
                    client.disconnect()
            except Exception: pass
            if not stop_evt.is_set():
                log.info("[OUTBOUND] Пауза 5 секунд перед переподключением.")
                time.sleep(5)

def _gc_concat_loop():
    while not _gc_stop.is_set():
        now = time.time()
        with _concat_lock:
            keys, buf = list(_concat_buf.keys()), _concat_buf
            for k in keys:
                if now - (buf.get(k) or {}).get("ts", 0) > _CONCAT_TTL_SEC:
                    buf.pop(k, None)
        _gc_stop.wait(10.0)

def start_concatenation_worker():
    global _gc_thread
    if _gc_thread and _gc_thread.is_alive(): return
    _gc_stop.clear()
    _gc_thread = threading.Thread(target=_gc_concat_loop, name="concat-gc", daemon=True)
    _gc_thread.start()

def stop_concatenation_worker():
    _gc_stop.set()
    if _gc_thread and _gc_thread.is_alive():
        _gc_thread.join(timeout=1.0)