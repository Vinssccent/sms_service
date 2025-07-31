# src/smpp_worker.py
# -*- coding: utf-8 -*-
"""
SMPP-воркер: подключается к провайдерам transceiver-ом, читает deliver_sm
и сохраняет SMS в БД. Рассчитан на высокую производительность.
"""

import logging
import time, threading, re
from typing import Optional

import smpplib.client, smpplib.consts
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.database import SessionLocal
from src import models
from src.utils import normalize_phone_number

# Настраиваем логгер для этого модуля
log = logging.getLogger(__name__)

# --- SMPP статус-коды -------------------------------------------------------
import smpplib.consts as _C

# Делает код совместимым с разными версиями smpplib
ESME_ROK          = getattr(_C, "ESME_ROK",          getattr(_C, "SMPP_ESME_ROK",          0x00000000))
ESME_RINVSENDERID = getattr(_C, "ESME_RINVSENDERID", getattr(_C, "SMPP_ESME_RINVSENDERID", 0x00000069))

# ---------------------------------------------------------------------------#
#                   Обработка входящего deliver_sm                           #
# ---------------------------------------------------------------------------#
def _parse_code(txt: str) -> Optional[str]:
    """Извлечь 3-9-значный код из текста."""
    m = re.search(r"\d{3,9}", txt or "")
    return m.group(0) if m else None


def _handle_deliver_sm(pdu, db: Session) -> int:
    """
    Обрабатывает входящую SMS. Возвращает smpp-статус (0 — ОК, 0x69 — REJECT).
    Эта функция вызывается для каждой SMS и оптимизирована для скорости.
    """
    try:
        dst_raw = pdu.destination_addr.decode(errors="replace")
        src_raw = pdu.source_addr.decode(errors="replace").strip()
        text    = pdu.short_message.decode(errors="replace")

        log.info(f"Входящая SMS от '{src_raw}' на '{dst_raw}' с текстом: '{text}'")

        # 1️⃣ Отправитель в белом списке?
        # Этот запрос быстрый, так как `AllowedSender.name` индексировано.
        sender = (db.query(models.AllowedSender)
                    .filter(func.lower(models.AllowedSender.name)
                            == func.lower(src_raw))
                    .first())
        if not sender:
            log.warning(f"REJECT: Отправитель '{src_raw}' не в белом списке.")
            return ESME_RINVSENDERID

        # 2️⃣ Есть активная сессия по номеру + сервису?
        dst = normalize_phone_number(dst_raw)
        
        # Этот запрос - ключевой для производительности. Он быстрый, потому что:
        # - phone_number_str, service_id, status - все эти поля индексированы в models.py.
        # - .order_by() и .first() эффективно используют эти индексы.
        sess = (db.query(models.Session)
                  .filter(models.Session.phone_number_str == dst,
                          models.Session.service_id == sender.service_id,
                          models.Session.status.in_([1, 3]))
                  .order_by(models.Session.created_at.desc()) # <-- ИСПРАВЛЕНО: Берем самую новую сессию
                  .first())
        if not sess:
            log.warning(f"REJECT: Не найдена активная сессия для номера '{dst}' и сервиса '{sender.service.name}'.")
            return ESME_RINVSENDERID

        # 3️⃣ Сохраняем SMS. Это быстрые операции INSERT и UPDATE.
        code = _parse_code(text)
        db.add(models.SmsMessage(session_id=sess.id,
                                 source_addr=src_raw,
                                 text=text,
                                 code=code))
        sess.status = 2
        db.commit()
        log.info(f"SUCCESS: SMS для сессии {sess.id} сохранена, код: {code}")
        return ESME_ROK

    except Exception as exc:
        log.error(f"Критическая ошибка при обработке SMS: {exc}", exc_info=True)
        db.rollback()
        return ESME_RINVSENDERID


# ---------------------------------------------------------------------------#
#                Главный цикл подключения к провайдеру                       #
# ---------------------------------------------------------------------------#
def run_smpp_provider_loop(provider: models.Provider,
                           stop_evt: threading.Event) -> None:
    """
    Постоянно держим SMPP-соединение; auto-enquire_link делает smpplib.
    """
    client = smpplib.client.Client(host=provider.smpp_host,
                                   port=provider.smpp_port,
                                   allow_unknown_opt_params=True)

    def on_message(pdu):
        db = SessionLocal()
        try:
            status = _handle_deliver_sm(pdu, db)
        finally:
            db.close()
        try:
            client.send_pdu(pdu.make_response(command_status=status))
        except Exception:
            pass

    client.set_message_received_handler(on_message)

    log.info(f"Запуск воркера для провайдера [{provider.id}] {provider.name}")
    while not stop_evt.is_set():
        try:
            log.info(f"[{provider.name}] Попытка подключения к {provider.smpp_host}:{provider.smpp_port}...")
            client.connect()
            client.bind_transceiver(system_id=provider.system_id,
                                    password=provider.password)
            log.info(f"[{provider.name}] Успешно подключен (BOUND_TRX).")

            listen_thr = threading.Thread(
                target=client.listen,
                kwargs={"auto_send_enquire_link": True},
                daemon=True,
                name=f"SMPP-Listener-{provider.id}"
            )
            listen_thr.start()

            while listen_thr.is_alive() and not stop_evt.is_set():
                time.sleep(0.5)

        except Exception as exc:
            log.error(f"[{provider.name}] Ошибка в цикле SMPP: {exc}. Повтор через 15 секунд.")
        finally:
            try:
                if client.state and client.state != smpplib.consts.SMPP_CLIENT_STATE_CLOSED:
                    log.info(f"[{provider.name}] Разрыв соединения.")
                    client.disconnect()
            except Exception:
                pass
            
            if not stop_evt.is_set():
                time.sleep(15)

    log.info(f"Воркер для провайдера [{provider.id}] {provider.name} остановлен.")

handle_incoming_sms = _handle_deliver_sm # Для обратной совместимости