# src/main.py
# -*- coding: utf-8 -*-
"""
src/main.py — полностью собранный файл
--------------------------------------

• FastAPI-приложение с админкой SQLAdmin  
• Lifespan-менеджер, автоматически поднимающий SMPP-воркеры  
• Полная реализация handler_api (аналог API)
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI
from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload
from sqladmin import Admin, ModelView
from starlette.responses import JSONResponse, Response

# ────────────────────────────────────────────
# Логирование
# ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("main")

# ────────────────────────────────────────────
# Локальные импорты (после настройки logging)
# ────────────────────────────────────────────
from src.database import SessionLocal, engine
from src import importer, models, smpp_worker
from src.utils import normalize_phone_number

# ────────────────────────────────────────────
# Lifespan: автозапуск SMPP-воркеров
# ────────────────────────────────────────────
smpp_threads: dict[int, threading.Thread] = {}
stop_events: dict[int, threading.Event] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("=== Lifespan startup: запускаю SMPP workers ===")
    db = SessionLocal()
    try:
        active_providers = (
            db.query(models.Provider).filter(models.Provider.is_active.is_(True)).all()
        )
        for prov in active_providers:
            log.info(" → worker для провайдера %s", prov.name)
            stop_event = threading.Event()
            thread = threading.Thread(
                target=smpp_worker.run_smpp_provider_loop,
                args=(prov, stop_event),
                daemon=True,
                name=f"SMPP-Worker-{prov.id}",
            )
            thread.start()
            smpp_threads[prov.id] = thread
            stop_events[prov.id] = stop_event
    finally:
        db.close()

    yield  # ───── приложение работает ─────

    log.info("=== Lifespan shutdown: останавливаю SMPP workers ===")
    for prov_id, ev in stop_events.items():
        log.info(" ← останавливаю worker провайдера %s", prov_id)
        ev.set()
    for prov_id, thr in smpp_threads.items():
        thr.join(timeout=2.0)
    log.info("Все SMPP workers остановлены")

# ────────────────────────────────────────────
# FastAPI - экземпляр
# ────────────────────────────────────────────
app = FastAPI(
    title="SMS Activation API",
    description="Универсальный API для активаций",
    version="FINAL-WITH-SENDERS-v3-stable-admin",
    lifespan=lifespan,
)
app.include_router(importer.router)

# ────────────────────────────────────────────
# SQLAdmin (стабильная версия без фильтров)
# ────────────────────────────────────────────
admin = Admin(app, engine)

class AllowedSenderAdmin(ModelView, model=models.AllowedSender):
    name = "Отправитель"
    name_plural = "Разрешённые отправители"
    icon = "fa-solid fa-check-double"
    column_list = [models.AllowedSender.id, models.AllowedSender.name, models.AllowedSender.service]
    column_searchable_list = [models.AllowedSender.name]

class CountryAdmin(ModelView, model=models.Country):
    name_plural = "Страны"
    icon = "fa-solid fa-flag"
    column_list = ["id", "name", "iso_code", "phone_code"]
    column_searchable_list = [models.Country.name, models.Country.iso_code]

class ServiceAdmin(ModelView, model=models.Service):
    name_plural = "Сервисы"
    icon = "fa-solid fa-tag"
    column_list = ["id", "name", "code"]
    column_searchable_list = [models.Service.name, models.Service.code]

class OperatorAdmin(ModelView, model=models.Operator):
    name_plural = "Операторы"
    icon = "fa-solid fa-wifi"
    column_list = "__all__"
    column_searchable_list = [models.Operator.name]

class ProviderAdmin(ModelView, model=models.Provider):
    name_plural = "SMPP-провайдеры"
    icon = "fa-solid fa-server"
    column_list = [
        models.Provider.id, models.Provider.name, models.Provider.smpp_host,
        "phone_numbers_count", models.Provider.is_active,
    ]
    column_details_list = [
        models.Provider.id, models.Provider.name, models.Provider.smpp_host,
        models.Provider.smpp_port, models.Provider.system_id,
        models.Provider.password, models.Provider.is_active,
        models.Provider.phone_numbers,
    ]
    column_labels = {"phone_numbers_count": "Кол-во номеров"}
    column_searchable_list = [models.Provider.name, models.Provider.smpp_host]

    async def list_model(self, request, *a, **kw):
        self.session.query_options = [selectinload(models.Provider.phone_numbers)]
        resp = await super().list_model(request, *a, **kw)
        for prov in resp.rows:
            prov.phone_numbers_count = len(prov.phone_numbers)
        return resp

class PhoneNumberAdmin(ModelView, model=models.PhoneNumber):
    name = "Номер"
    name_plural = "Номера телефонов"
    icon = "fa-solid fa-phone"
    column_searchable_list = [models.PhoneNumber.number_str]
    # column_filters удалены, чтобы избежать ошибки
    column_list = [
        models.PhoneNumber.id, models.PhoneNumber.number_str, models.PhoneNumber.provider,
        models.PhoneNumber.country, models.PhoneNumber.is_active, models.PhoneNumber.is_in_use
    ]
    column_details_list = [c.name for c in models.PhoneNumber.__table__.c]

class SessionAdmin(ModelView, model=models.Session):
    name = "Активация"
    name_plural = "Активации (Сессии)"
    icon = "fa-solid fa-clock"
    column_searchable_list = [models.Session.phone_number_str]
    column_list = [
        models.Session.id, models.Session.phone_number_str, models.Session.service,
        models.Session.status, models.Session.created_at
    ]
    # Явно указываем поля для детального просмотра
    column_details_list = [
        models.Session.id,
        models.Session.phone_number_str,
        models.Session.status,
        models.Session.service,
        models.Session.phone_number,
        models.Session.api_key,
        models.Session.created_at,
        models.Session.sms_messages, # <-- Теперь это поле будет отображаться красиво
    ]
    can_create = can_edit = False

class SmsMessageAdmin(ModelView, model=models.SmsMessage):
    name_plural = "SMS-сообщения"
    icon = "fa-solid fa-comment-sms"
    column_searchable_list = [models.SmsMessage.text, models.SmsMessage.code]
    column_list = [
        models.SmsMessage.id, models.SmsMessage.session_id, models.SmsMessage.text,
        models.SmsMessage.code, models.SmsMessage.received_at,
    ]
    can_create = False
    can_edit = False

class ApiKeyAdmin(ModelView, model=models.ApiKey):
    name_plural = "API-ключи"
    icon = "fa-solid fa-key"
    column_list = [c.name for c in models.ApiKey.__table__.c if c.name != "id"]
    column_searchable_list = [models.ApiKey.key, models.ApiKey.description]
    form_excluded_columns = [models.ApiKey.key, models.ApiKey.created_at, "id"]

admin.add_view(PhoneNumberAdmin)
admin.add_view(SessionAdmin)
admin.add_view(SmsMessageAdmin)
admin.add_view(ProviderAdmin)
admin.add_view(ServiceAdmin)
admin.add_view(AllowedSenderAdmin)
admin.add_view(CountryAdmin)
admin.add_view(OperatorAdmin)
admin.add_view(ApiKeyAdmin)

# ────────────────────────────────────────────
# Вспомогательная зависимость
# ────────────────────────────────────────────
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ────────────────────────────────────────────
# handler_api.php — полный аналог
# ────────────────────────────────────────────
@app.get("/stubs/handler_api.php")
def handle_api(
    action: str,
    api_key: str,
    service: Optional[str] = None,
    country: Optional[int] = None,
    operator: Optional[str] = None,
    id: Optional[int] = None,
    status: Optional[int] = None,
    number: Optional[str] = None,
    db: Session = Depends(get_db),
):
    # ---------- проверка ключа ----------
    db_key = (
        db.query(models.ApiKey)
        .filter(models.ApiKey.key == api_key, models.ApiKey.is_active.is_(True))
        .first()
    )
    if not db_key:
        return Response("BAD_KEY", media_type="text/plain")

    # ---------- getBalance ----------
    if action == "getBalance":
        return Response("ACCESS_BALANCE:9999", media_type="text/plain")

    # ---------- getNumbersStatus ------
    if action == "getNumbersStatus":
        if country is None:
            return Response("BAD_ACTION", media_type="text/plain")

        q = (
            db.query(models.Service.code, func.count(models.PhoneNumber.id))
            .select_from(models.Service)
            .outerjoin(
                models.PhoneNumber,
                (models.PhoneNumber.is_active.is_(True))
                & (models.PhoneNumber.is_in_use.is_(False))
                & (models.PhoneNumber.country_id == country),
            )
            .group_by(models.Service.code)
        )
        return JSONResponse({f"{code}_0": cnt for code, cnt in q.all()})

    # ---------- getNumber ----------
    if action == "getNumber":
        if service is None or country is None:
            return Response("BAD_ACTION", media_type="text/plain")

        db_service = (
            db.query(models.Service).filter(models.Service.code == service).first()
        )
        if not db_service:
            return Response("BAD_SERVICE", media_type="text/plain")

        q = (
            db.query(models.PhoneNumber)
            .filter(
                models.PhoneNumber.country_id == country,
                models.PhoneNumber.is_active.is_(True),
                models.PhoneNumber.is_in_use.is_(False),
            )
            .order_by(models.PhoneNumber.sort_order)
            .with_for_update()
        )

        if operator and operator != "any":
            db_op = (
                db.query(models.Operator)
                .filter(
                    models.Operator.name == operator,
                    models.Operator.country_id == country,
                )
                .first()
            )
            if db_op:
                q = q.filter(models.PhoneNumber.operator_id == db_op.id)

        num_obj = q.first()
        if not num_obj:
            return Response("NO_NUMBERS", media_type="text/plain")

        num_obj.is_in_use = True
        norm = normalize_phone_number(num_obj.number_str)
        sess = models.Session(
            phone_number_str=norm,
            service_id=db_service.id,
            phone_number_id=num_obj.id,
            api_key_id=db_key.id,
            status=1,
        )
        db.add(sess)
        db.commit()
        db.refresh(sess)
        return Response(
            f"ACCESS_NUMBER:{sess.id}:{norm.replace('+', '')}", media_type="text/plain"
        )

    # ---------- getRepeatNumber ----------
    if action == "getRepeatNumber":
        if not number:
            return Response("BAD_ACTION", media_type="text/plain")
        norm = normalize_phone_number(number)
        if not norm:
            return Response("BAD_NUMBER", media_type="text/plain")

        num_obj = (
            db.query(models.PhoneNumber)
            .filter(models.PhoneNumber.number_str == norm)
            .with_for_update()
            .first()
        )
        if not num_obj:
            return Response("BAD_NUMBER", media_type="text/plain")
        if num_obj.is_in_use:
            return Response("NUMBER_BUSY", media_type="text/plain")

        default_service = db.query(models.Service).first()
        if not default_service:
            return Response("NO_SERVICES_IN_DB", media_type="text/plain")

        num_obj.is_in_use = True
        sess = models.Session(
            phone_number_str=norm,
            service_id=default_service.id,
            phone_number_id=num_obj.id,
            api_key_id=db_key.id,
            status=1,
        )
        db.add(sess)
        db.commit()
        db.refresh(sess)
        return Response(
            f"ACCESS_NUMBER:{sess.id}:{norm.replace('+', '')}",
            media_type="text/plain",
        )

    # ---------- getStatus ----------
    if action == "getStatus":
        if id is None:
            return Response("BAD_ACTION", media_type="text/plain")

        sess = (
            db.query(models.Session)
            .filter(models.Session.id == id, models.Session.api_key_id == db_key.id)
            .first()
        )
        if not sess:
            return Response("NO_ACTIVATION", media_type="text/plain")

        if sess.status in (6, 8):
            return Response("STATUS_CANCEL", media_type="text/plain")

        last_sms = (
            db.query(models.SmsMessage)
            .filter(models.SmsMessage.session_id == id)
            .order_by(models.SmsMessage.received_at.desc())
            .first()
        )
        if last_sms and last_sms.code:
            return Response(f"STATUS_OK:{last_sms.code}", media_type="text/plain")
        if sess.status == 3:
            return Response("STATUS_WAIT_RETRY", media_type="text/plain")
        return Response("STATUS_WAIT_CODE", media_type="text/plain")

    # ---------- setStatus ----------
    if action == "setStatus":
        if id is None or status is None:
            return Response("BAD_ACTION", media_type="text/plain")

        sess = (
            db.query(models.Session)
            .filter(models.Session.id == id, models.Session.api_key_id == db_key.id)
            .first()
        )
        if not sess:
            return Response("NO_ACTIVATION", media_type="text/plain")

        num = sess.phone_number

        if status == 3:
            sess.status = 3
            db.commit()
            return Response("ACCESS_RETRY_GET", media_type="text/plain")

        if status == 6:
            sess.status = 6
            if num:
                num.is_in_use = False
            db.commit()
            return Response("ACCESS_ACTIVATION", media_type="text/plain")

        if status == 8:
            sess.status = 8
            if num:
                num.is_in_use = False
            db.commit()
            return Response("ACCESS_CANCEL", media_type="text/plain")

    # ---------- неизвестное действие ----------
    return Response("BAD_ACTION", media_type="text/plain")