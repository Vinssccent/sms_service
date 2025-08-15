# src/smpp_worker.py
# -*- coding: utf-8 -*-
"""
Надёжный обработчик входящих SMPP submit_sm/deliver_sm:
- Декодирование текста с учётом UDH и data_coding
- Привязка к активной сессии по номеру (и, при возможности, по сервису)
- Строгая транзакция: либо пишем в БД и возвращаем 0, либо НЕ пишем и возвращаем 69/8
"""

from __future__ import annotations
import logging
import threading
import re
from typing import Optional

from sqlalchemy.orm import Session, selectinload

from . import models
from .utils import normalize_phone_number

log = logging.getLogger("src.smpp_worker")

# SMPP статусы (десятичные)
ESME_ROK = 0          # OK
ESME_RSYSERR = 8      # System error
ESME_RSUBMITFAIL = 69 # Мы используем как «REJECT/живой трафик»

# -----------------------------
# Конкатенация (заглушки / совместимость)
# -----------------------------
_concat_thread: Optional[threading.Thread] = None
_concat_stop = threading.Event()

def start_concatenation_worker():
    """Совместимость с существующим сервером. При необходимости можно расширить."""
    global _concat_thread, _concat_stop
    if _concat_thread and _concat_thread.is_alive():
        return
    _concat_stop.clear()

    def _worker():
        log.info("▶︎ Запущен поток-склейщик SMS.")
        try:
            while not _concat_stop.is_set():
                _concat_stop.wait(1.0)
        finally:
            log.info("◀︎ Поток-склейщик остановлен.")

    _concat_thread = threading.Thread(target=_worker, name="smpp_concatenation_worker", daemon=True)
    _concat_thread.start()

def stop_concatenation_worker():
    global _concat_thread, _concat_stop
    _concat_stop.set()
    if _concat_thread and _concat_thread.is_alive():
        _concat_thread.join(timeout=3.0)
    _concat_thread = None

# -----------------------------
# Утилиты
# -----------------------------
_CODE_RE = re.compile(r"\b(\d{4,8})\b")

def extract_code(text: str) -> Optional[str]:
    """Достаёт код 4–8 цифр (первое вхождение)."""
    m = _CODE_RE.search(text or "")
    return m.group(1) if m else None

def get_decoded_text(pdu) -> str:
    """
    Аккуратно извлекает текст:
    - если установлен UDH (esm_class & 0x40) — отрезаем заголовок
    - учитываем data_coding: 0 -> ascii/latin1, 8 -> utf-16be
    """
    message_bytes = getattr(pdu, "short_message", b"") or b""
    esm_class = getattr(pdu, "esm_class", 0)
    data_coding = getattr(pdu, "data_coding", 0)

    # UDH?
    if message_bytes and (esm_class & 0x40):
        udhl = message_bytes[0]  # длина заголовка (без этого первого байта)
        payload = message_bytes[udhl + 1 :]
    else:
        payload = message_bytes

    # Порядок попыток по кодировкам
    encs: list[str]
    if data_coding == 8:
        encs = ["utf-16be", "latin1", "ascii"]
    elif data_coding == 0:
        encs = ["ascii", "latin1"]
    else:
        encs = ["latin1", "ascii"]

    for enc in encs:
        try:
            s = payload.decode(enc)
            return s.replace("\x00", "")
        except UnicodeDecodeError:
            continue
    # крайний случай
    return payload.decode("ascii", errors="replace").replace("\x00", "")

def _brand_like(service_name: str, sender: str) -> bool:
    """Грубая «похожесть» имени сервиса на sender (Binance ↔︎ BINANCE / BinanceCode)."""
    if not service_name or not sender:
        return False
    a = service_name.lower()
    b = sender.lower()
    return a in b or b in a

# -----------------------------
# Главный обработчик входящего SMS
# -----------------------------
def _handle_deliver_sm(pdu, db: Session) -> int:
    """
    Возвращает SMPP статус:
      0  — если СМС сохранено и сессия обновлена
      69 — если это «живой»/осиротевший трафик (сессии нет) — мы его записали в orphan_sms
       8 — системная ошибка
    """
    try:
        src_raw = (getattr(pdu, "source_addr", b"") or b"").decode("ascii", errors="ignore") or ""
        dst_raw = (getattr(pdu, "destination_addr", b"") or b"").decode("ascii", errors="ignore") or ""
        text = get_decoded_text(pdu).strip()

        # Номер — это destination (наш MSISDN)
        number_norm = normalize_phone_number(dst_raw)
        if not number_norm:
            log.warning(f"ORPHAN (invalid dst): src='{src_raw}', dst='{dst_raw}', text='{text[:120]}'")
            db.add(models.OrphanSms(phone_number_str=dst_raw or "?", source_addr=src_raw, text=text))
            db.commit()
            return ESME_RSUBMITFAIL

        # Ищем активные сессии по номеру
        sessions_q = (
            db.query(models.Session)
            .options(selectinload(models.Session.service))
            .filter(models.Session.phone_number_str == number_norm, models.Session.status == 1)
            .order_by(models.Session.created_at.desc())
        )
        sessions = sessions_q.all()

        if not sessions:
            # Сессии нет -> orphan
            provider_id = country_id = operator_id = None
            phone_obj = (
                db.query(models.PhoneNumber)
                .filter(models.PhoneNumber.number_str == number_norm)
                .first()
            )
            if phone_obj:
                provider_id = phone_obj.provider_id
                country_id = phone_obj.country_id
                operator_id = phone_obj.operator_id

            db.add(
                models.OrphanSms(
                    phone_number_str=number_norm,
                    source_addr=src_raw,
                    text=text,
                    provider_id=provider_id,
                    country_id=country_id,
                    operator_id=operator_id,
                )
            )
            db.commit()
            log.info(f"ORPHAN: src='{src_raw}', dst={number_norm}, text='{text[:120]}'")
            return ESME_RSUBMITFAIL

        # Если сессий несколько — выбираем приоритетно по близости sender к сервису
        matched = None
        for s in sessions:
            if s.service and _brand_like(s.service.name, src_raw):
                matched = s
                break
        if matched is None:
            matched = sessions[0]  # самая свежая

        # Пишем SMS и закрываем сессию в одной транзакции
        code = extract_code(text)
        db.add(
            models.SmsMessage(
                session_id=matched.id,
                source_addr=src_raw or "",
                text=text,
                code=code,
            )
        )
        matched.status = 2
        db.commit()

        log.info(
            f"INBOX OK: session={matched.id} service={matched.service.name if matched.service else 'N/A'} "
            f"dst={number_norm} src='{src_raw}' code={code or '-'}"
        )
        return ESME_ROK

    except Exception as exc:
        log.error(f"Критическая ошибка при обработке SMS: {exc}", exc_info=True)
        db.rollback()
        return ESME_RSYSERR

# Совместимость — не используется в нашем сервере
def run_smpp_provider_loop(provider: models.Provider, stop_evt: threading.Event) -> None:
    pass
