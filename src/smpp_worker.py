# src/smpp_worker.py
# -*- coding: utf-8 -*-
"""
SMPP-воркер: декодирование, парсинг кода, привязка к сессии.
При отсутствии активной сессии — сохраняем в orphan_sms и шлём REJECT 0x45 (69).
"""

import logging
import time
import threading
import re
from typing import Optional

import smpplib.client
import smpplib.exceptions
from sqlalchemy.orm import Session, selectinload
import redis

from src.database import SessionLocal
from src import models
from src.utils import normalize_phone_number

log = logging.getLogger(__name__)

# --- Настройка Redis ---
try:
    redis_client = redis.Redis(decode_responses=True)
    redis_client.ping()
    log.info("✓ SMPP Worker: Успешное подключение к Redis.")
except redis.exceptions.ConnectionError as e:
    log.error(f"🔥 SMPP Worker: Не удалось подключиться к Redis: {e}.")
    redis_client = None


# SMPP командные статусы
ESME_ROK = 0x00000000
ESME_RINVSENDERID = 0x00000045  # 69
ESME_RSYSERR = 0x00000008


def save_orphan_sms(db: Session, phone_number_str: str, source_addr: str, text: str):
    """
    Сохраняем входящее SMS без активной сессии (или не совпавшее с сервисом)
    в таблицу orphan_sms. Пытаемся подтянуть provider/country/operator из phone_numbers.
    """
    try:
        provider_id = country_id = operator_id = None

        phone_obj = (
            db.query(models.PhoneNumber)
            .filter(models.PhoneNumber.number_str == phone_number_str)
            .first()
        )
        if phone_obj:
            provider_id = phone_obj.provider_id
            country_id = phone_obj.country_id
            operator_id = phone_obj.operator_id

        orphan = models.OrphanSms(
            phone_number_str=phone_number_str,
            source_addr=source_addr,
            text=text,
            provider_id=provider_id,
            country_id=country_id,
            operator_id=operator_id,
        )
        db.add(orphan)
        db.commit()
        log.info(
            f"[ORPHAN_SMS] Saved id={orphan.id} src='{source_addr}' dst='{phone_number_str}' "
            f"(prov={provider_id}, country={country_id}, op={operator_id})"
        )
    except Exception as e:
        db.rollback()
        log.error(f"[ORPHAN_SMS] Save error: {e}", exc_info=True)


def _parse_code(text: str) -> Optional[str]:
    """Извлекаем код (best-effort)."""
    if not text:
        return None
    # ### 123 456 или 123-456
    m = re.search(r'\b(\d{3,4})[\s-]+(\d{3,4})\b', text)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    # слова-триггеры
    patterns = [
        r'(?:code|код|пароль|code is|is your code|code:|кода:|кодом:)\s*(\d{4,8})',
        r'(\d{4,8})\s*(?:is your|ваш|твой)\s*(?:code|код|пароль)',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1)
    # любое “отдельное” число
    m = re.search(r'\b(\d{4,8})\b', text)
    if m:
        return m.group(1)
    # fallback: любые цифры подряд
    only = re.sub(r'\D+', '', text or '')
    m = re.search(r'(\d{4,8})', only)
    return m.group(1) if m else None


def _get_decoded_text(pdu) -> str:
    """Пытаемся декодировать short_message с учётом data_coding."""
    message_bytes = pdu.short_message
    data_coding = getattr(pdu, 'data_coding', 0)
    encs = []
    if data_coding == 8:
        encs = ['utf-16be', 'latin1', 'ascii']
    elif data_coding == 0:
        encs = ['ascii', 'latin1']
    else:
        encs = ['latin1', 'ascii']
    for enc in encs:
        try:
            return message_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    return message_bytes.decode('ascii', errors='replace')


def _handle_deliver_sm(pdu, db: Session) -> int:
    """
    Главный обработчик входящей SMS.
    Возвращает SMPP status для ответа сервером обратно клиенту.
    """
    try:
        src_raw = pdu.source_addr.decode(errors="replace").strip()
        dst_raw = pdu.destination_addr.decode(errors="replace")
        text = _get_decoded_text(pdu)

        clean_text = text.replace('\n', ' ').replace('\r', '')
        log.info(f"Входящая SMS от '{src_raw}' на '{dst_raw}' с текстом: '{clean_text}'")

        # нормализуем номер в +E.164
        dst = normalize_phone_number(dst_raw)

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Усиленная и исправленная защита от гонки состояний ---
        retries = 5 
        active_sessions = []
        for i in range(retries):
            # Пытаемся найти активные сессии
            active_sessions = (
                db.query(models.Session)
                .options(selectinload(models.Session.service))
                .filter(
                    models.Session.phone_number_str == dst,
                    models.Session.status.in_([1, 3]),
                )
                .order_by(models.Session.created_at.desc())
                .all()
            )
            
            if active_sessions:
                if i > 0:
                    log.info(f"✓ Сессия для '{dst}' найдена с попытки №{i+1}")
                break
            
            if i < retries - 1 and redis_client and redis_client.exists(f"pending_session:{dst}"):
                log.warning(f"Сессия для '{dst}' не найдена (попытка {i+1}/{retries}), но есть сигнал в Redis. Жду 200ms...")
                time.sleep(0.2)
                db.expire_all()
            else:
                break
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        if not active_sessions:
            log.warning(
                f"REJECT: Не найдена ни одна активная сессия для номера '{dst}' после всех попыток. "
                f"Сохраняю как orphan и возвращаю 69."
            )
            save_orphan_sms(db, dst, src_raw, text)
            return ESME_RINVSENDERID

        matched_session = None
        text_lower, src_lower = text.lower(), src_raw.lower()

        for sess in active_sessions:
            service_obj = sess.service
            if not service_obj:
                continue

            if service_obj.code == 'nitro':
                log.info(f"Обнаружена сессия для спец. сервиса '{service_obj.name}'. Принимаю любое SMS.")
                matched_session = sess
                break

            all_keywords = set()
            if service_obj.name:
                parts = re.findall(r'\w+', service_obj.name.lower())
                all_keywords.update(p for p in parts if p)

            if not all_keywords:
                log.warning(
                    f"REJECT: Для сервиса '{service_obj.name}' не удалось получить ключевые слова из имени."
                )
                continue

            is_match_found = any(
                kw in text_lower or kw in src_lower
                for kw in all_keywords
                if kw
            )
            if is_match_found:
                matched_session = sess
                break

        if not matched_session:
            log.warning(
                f"REJECT: SMS от '{src_raw}' не соответствует ни одной из активных сессий для номера '{dst}'. "
                f"Сохраняю как orphan и возвращаю 69."
            )
            save_orphan_sms(db, dst, src_raw, text)
            return ESME_RINVSENDERID

        code = _parse_code(text)
        db.add(
            models.SmsMessage(
                session_id=matched_session.id,
                source_addr=src_raw,
                text=text,
                code=code,
            )
        )
        matched_session.status = 2
        db.commit()
        log.info(
            f"SUCCESS: SMS от '{src_raw}' привязана к сессии {matched_session.id} "
            f"(сервис: {matched_session.service.name}), код={code}"
        )
        return ESME_ROK

    except Exception as exc:
        log.error(f"Критическая ошибка при обработке SMS: {exc}", exc_info=True)
        db.rollback()
        return ESME_RSYSERR


def run_smpp_provider_loop(provider: models.Provider, stop_evt: threading.Event) -> None:
    """
    Цикл подключения к внешнему SMPP-провайдеру (bind TRX) и приёма deliver_sm.
    """
    client = None
    log.info(f"Запуск воркера для провайдера [{provider.id}] {provider.name}")

    while not stop_evt.is_set():
        try:
            client = smpplib.client.Client(
                host=provider.smpp_host,
                port=provider.smpp_port,
                allow_unknown_opt_params=True,
            )

            def on_message(pdu):
                db = SessionLocal()
                try:
                    status = _handle_deliver_sm(pdu, db)
                finally:
                    db.close()
                try:
                    if client and client.state == 'BOUND_TRX':
                        client.send_pdu(pdu.make_response(command_status=status))
                except Exception as e:
                    log.error(f"[{provider.name}] Не удалось отправить ответ на PDU: {e}")

            client.set_message_received_handler(on_message)

            log.info(f"[{provider.name}] Подключение к {provider.smpp_host}:{provider.smpp_port}…")
            client.connect()
            client.bind_transceiver(system_id=provider.system_id, password=provider.password, system_type=provider.system_type)
            log.info(f"[{provider.name}] BOUND_TRX OK, слушаю…")

            client.listen(auto_send_enquire_link=True)

            log.warning(f"[{provider.name}] Соединение потеряно. Переподключение через 15 сек.")

        except smpplib.exceptions.PDUError as pdu_exc:
            error_code = pdu_exc.args[1] if len(pdu_exc.args) > 1 else 0
            if error_code == 14:
                log.error(f"[{provider.name}] Ошибка аутентификации: {pdu_exc}. Повтор через 30 сек.")
                if not stop_evt.wait(30):
                    continue
            else:
                log.error(f"[{provider.name}] Ошибка PDU: {pdu_exc}. Повтор через 15 сек.")

        except (ConnectionRefusedError, OSError, smpplib.exceptions.ConnectionError) as conn_exc:
            log.error(f"[{provider.name}] Ошибка соединения: {conn_exc}. Повтор через 15 сек.")

        except Exception as exc:
            log.error(f"[{provider.name}] Неизвестная ошибка в цикле SMPP: {exc}. Повтор через 15 сек.")

        finally:
            try:
                if client and client.state != 'CLOSED':
                    log.info(f"[{provider.name}] Разрыв соединения (finally).")
                    client.disconnect()
            except Exception:
                pass

            if not stop_evt.is_set():
                time.sleep(15)

    log.info(f"Воркер для провайдера [{provider.id}] {provider.name} остановлен.")


# точка входа, которую дергает smpp_server
handle_incoming_sms = _handle_deliver_sm