# src/main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import threading
import datetime
import random
import time
from contextlib import asynccontextmanager
from typing import Optional, Any, List

from fastapi import Depends, FastAPI, Request
from sqlalchemy import func, cast, Date, or_, and_
from sqlalchemy.orm import Session, selectinload
from starlette.responses import JSONResponse, Response, RedirectResponse
from starlette_admin.contrib.sqla import Admin, ModelView
from starlette_admin.auth import AuthProvider
from starlette_admin.views import Link
from starlette_admin.fields import StringField
from starlette.middleware.sessions import SessionMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")
from src.database import SessionLocal, engine
from src import models, smpp_worker, tools
from src.utils import normalize_phone_number

# --- Настройка Redis ---
try:
    redis_client = redis.Redis(decode_responses=True)
    redis_client.ping()
    log.info("✓ Main: Успешное подключение к Redis.")
except redis.exceptions.ConnectionError as e:
    log.error(f"🔥 Main: Не удалось подключиться к Redis: {e}.")
    redis_client = None


background_threads: dict[str, threading.Thread] = {}
stop_events: dict[str, threading.Event] = {}

def cleanup_expired_sessions(stop_event: threading.Event):
    log.info("▶︎ Запущен поток-уборщик старых сессий.")
    if not stop_event.wait(timeout=60):
        while not stop_event.wait(timeout=300):
            log.info("🧹 Выполняю очистку сессий старше 20 минут...")
            db = SessionLocal()
            try:
                expiration_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=20)
                sessions_to_cancel = (
                    db.query(models.Session)
                    .filter(models.Session.status.in_([1, 3]), models.Session.created_at < expiration_time)
                    .options(selectinload(models.Session.phone_number)).all()
                )
                if sessions_to_cancel:
                    log.warning(f"🧹 Найдено {len(sessions_to_cancel)} сессий без SMS. Отменяю...")
                    for sess in sessions_to_cancel:
                        sess.status = 8
                        if sess.phone_number:
                            sess.phone_number.is_in_use = False
                    db.commit()
                sessions_to_complete = (
                    db.query(models.Session)
                    .filter(models.Session.status == 2, models.Session.created_at < expiration_time)
                    .options(selectinload(models.Session.phone_number)).all()
                )
                if sessions_to_complete:
                    log.warning(f"🧹 Найдено {len(sessions_to_complete)} незакрытых сессий с SMS. Завершаю...")
                    for sess in sessions_to_complete:
                        sess.status = 6
                        if sess.phone_number:
                            sess.phone_number.is_in_use = False
                    db.commit()
                if not sessions_to_cancel and not sessions_to_complete:
                    log.info("🧹 Не найдено просроченных сессий.")
            except Exception as e:
                log.error(f"🔥 Ошибка в потоке-уборщике: {e}", exc_info=True)
                db.rollback()
            finally:
                db.close()
    log.info("◀︎ Поток-уборщик остановлен.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("=== Lifespan startup: запускаю фоновые процессы ===")
    db = SessionLocal()
    try:
        active_providers = db.query(models.Provider).filter(models.Provider.is_active.is_(True)).all()
        for prov in active_providers:
            log.info(" → worker для провайдера %s", prov.name)
            stop_event = threading.Event()
            thread = threading.Thread(target=smpp_worker.run_smpp_provider_loop, args=(prov, stop_event), daemon=True, name=f"SMPP-Worker-{prov.id}")
            thread.start()
            background_threads[f"smpp_{prov.id}"] = thread
            stop_events[f"smpp_{prov.id}"] = stop_event
    finally:
        db.close()
    
    stop_cleanup_event = threading.Event()
    cleanup_thread = threading.Thread(target=cleanup_expired_sessions, args=(stop_cleanup_event,), daemon=True, name="Session-Cleaner")
    cleanup_thread.start()
    background_threads["cleaner"] = cleanup_thread
    stop_events["cleaner"] = stop_cleanup_event
    yield
    log.info("=== Lifespan shutdown: останавливаю фоновые процессы ===")
    for name, ev in stop_events.items():
        log.info(" ← останавливаю процесс %s", name)
        ev.set()
    for name, thr in background_threads.items():
        thr.join(timeout=2.0)
    log.info("Все фоновые процессы остановлены.")

class SimpleAuthProvider(AuthProvider):
    ADMIN_USERNAME = "admin"
    ADMIN_PASSWORD = "gfdtkcvccthdbc@"
    async def is_authenticated(self, request: Request) -> bool:
        return "is_authenticated" in request.session
    async def get_display_name(self, request: Request) -> str:
        return request.session.get("username", "Admin")
    async def get_photo_url(self, request: Request, **kwargs) -> Optional[str]:
        return None
    async def login(self, username: str, password: str, remember_me: bool, request: Request, response: Response) -> Response:
        if username.strip() == self.ADMIN_USERNAME and password == self.ADMIN_PASSWORD.strip():
            request.session["is_authenticated"] = True
            request.session["username"] = username
            next_url = request.query_params.get("next", request.url_for("admin:index"))
            return RedirectResponse(url=next_url, status_code=303)
        return response
    async def logout(self, request: Request, response: Response) -> Response:
        request.session.clear()
        return RedirectResponse(url=request.url_for("admin:index"), status_code=303)

auth_provider = SimpleAuthProvider()

app = FastAPI(title="SMS Activation API", version="FINAL-auth-only", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)
app.add_middleware(SessionMiddleware, secret_key="change-this-to-a-long-random-string")

class ProviderView(ModelView):
    fields = ["id", "name", "smpp_host", "smpp_port", "system_id", "password", "is_active", "daily_limit"]
    searchable_fields = ["name", "smpp_host"]
    sortable_fields = ["id", "name"]

class ServiceView(ModelView):
    fields = ["id", "name", "code", "icon_class", "daily_limit"]
    searchable_fields = ["name", "code"]

class PhoneNumberView(ModelView):
    fields = ["id", "number_str", "provider", "country", "operator", "is_active", "is_in_use", "sort_order"]
    searchable_fields = ["number_str"]
    sortable_fields = ["id", "number_str", "sort_order"]
    fields_default_sort = "-id"
    page_size = 100
    actions = ["delete"]

class SessionView(ModelView):
    fields = ["id", "phone_number_str", "status", "created_at", "service", "phone_number", "api_key"]
    searchable_fields = ["phone_number_str"]
    sortable_fields = ["id", "created_at", "status"]
    fields_default_sort = "-id"
    page_size = 100
    actions = ["delete"]


class SmsMessageView(ModelView):
    preloads = ["session"]
    fields = [
        "id",
        "session",
        StringField("phone_number", label="Phone Number", exclude_from_create=True, exclude_from_edit=True),
        "source_addr",
        "text",
        "code",
        "received_at",
    ]
    searchable_fields = [
        "phone_number",
        "text",
        "code",
        "source_addr",
    ]
    sortable_fields = ["id", "received_at", "phone_number"]
    fields_default_sort = "-id"
    page_size = 100
    actions = ["delete"]


class ApiKeyView(ModelView):
    fields = ["id", "key", "description", "is_active", "created_at", "sessions"]
    exclude_fields_from_list = ["sessions"]
    searchable_fields = ["key", "description"]
    sortable_fields = ["id", "description", "is_active", "created_at"]
    fields_default_sort = "-created_at"

class OperatorView(ModelView):
    fields = ["id", "name", "country", "provider"]
    searchable_fields = ["name"]
    sortable_fields = ["id", "name", "country", "provider"]

class ServiceLimitView(ModelView):
    fields = ["id", "service", "provider", "country", "daily_limit"]
    searchable_fields = ["service.name", "provider.name", "country.name"]
    sortable_fields = ["id", "service", "provider", "country", "daily_limit"]
    page_size = 100
    actions = ["delete"]

admin = Admin(engine, title="SMS Service", auth_provider=auth_provider)
admin.add_view(PhoneNumberView(models.PhoneNumber, icon="fa fa-phone"))
admin.add_view(SessionView(models.Session, icon="fa fa-clock"))
admin.add_view(SmsMessageView(models.SmsMessage, icon="fa fa-comment-sms"))
admin.add_view(ProviderView(models.Provider, icon="fa fa-server"))
admin.add_view(ServiceView(models.Service, icon="fa fa-tag"))
admin.add_view(ApiKeyView(models.ApiKey, icon="fa fa-key"))
admin.add_view(ModelView(models.Country, icon="fa fa-flag"))
admin.add_view(OperatorView(models.Operator, icon="fa fa-wifi"))
admin.add_view(ServiceLimitView(models.ServiceLimit, icon="fa fa-balance-scale", label="Service Limits"))
admin.add_view(Link(label="Инструменты", icon="fa fa-tools", url="/tools"))
admin.mount_to(app)
app.include_router(tools.router)

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@app.get("/stubs/handler_api.php")
def handle_api(action: str, api_key: str, service: Optional[str] = None, country: Optional[int] = None, operator: Optional[str] = None, id: Optional[int] = None, status: Optional[int] = None, number: Optional[str] = None, db: Session = Depends(get_db)):
    db_key = db.query(models.ApiKey).filter(models.ApiKey.key == api_key, models.ApiKey.is_active.is_(True)).first()
    if not db_key: return Response("BAD_KEY", media_type="text/plain")
    if action == "getBalance": return Response("ACCESS_BALANCE:9999", media_type="text/plain")
    if action == "getNumbersStatus":
        if country is None: return Response("BAD_ACTION", media_type="text/plain")
        q = db.query(models.Service.code, func.count(models.PhoneNumber.id)).select_from(models.Service).outerjoin(models.PhoneNumber, (models.PhoneNumber.is_active.is_(True)) & (models.PhoneNumber.is_in_use.is_(False)) & (models.PhoneNumber.country_id == country)).group_by(models.Service.code)
        return JSONResponse({f"{code}_0": cnt for code, cnt in q.all()})
    
    if action == "getNumber":
        if service is None or country is None: return Response("BAD_ACTION", media_type="text/plain")
        db_service = db.query(models.Service).filter(models.Service.code == service).first()
        if not db_service: return Response("BAD_SERVICE", media_type="text/plain")
        
        today_db_date = db.query(func.current_date()).scalar()
        
        available_providers_q = (
            db.query(models.Provider)
            .join(models.PhoneNumber)
            .filter(
                models.PhoneNumber.country_id == country, 
                models.PhoneNumber.is_active.is_(True), 
                models.PhoneNumber.is_in_use.is_(False)
            )
            .distinct()
        )
        
        providers_within_limit_ids = []
        for provider in available_providers_q:
            
            limit_to_check = None
            limit_source = "None"

            specific_limit_rule = db.query(models.ServiceLimit).filter_by(
                service_id=db_service.id, 
                provider_id=provider.id, 
                country_id=country
            ).first()
            if specific_limit_rule:
                limit_to_check = specific_limit_rule.daily_limit
                limit_source = f"Правило (S+P+C): {limit_to_check}"
            
            elif db_service.daily_limit is not None:
                limit_to_check = db_service.daily_limit
                limit_source = f"Сервис '{db_service.name}': {limit_to_check}"
                
            elif provider.daily_limit is not None:
                limit_to_check = provider.daily_limit
                limit_source = f"Провайдер '{provider.name}': {limit_to_check}"

            if limit_to_check is None:
                providers_within_limit_ids.append(provider.id)
                continue
            
            provider_sms_count = (
                db.query(models.SmsMessage)
                .join(models.Session)
                .join(models.PhoneNumber)
                .filter(
                    models.PhoneNumber.provider_id == provider.id,
                    models.Session.service_id == db_service.id,
                    models.PhoneNumber.country_id == country,
                    cast(models.SmsMessage.received_at, Date) == today_db_date
                )
                .count()
            )

            if provider_sms_count < limit_to_check:
                providers_within_limit_ids.append(provider.id)
            else:
                log.warning(f"ЛИМИТ: Провайдер '{provider.name}' исключен. Источник лимита: {limit_source}. Текущее кол-во SMS: {provider_sms_count}.")
        
        if not providers_within_limit_ids:
            log.warning("ЛИМИТЫ: Не найдено провайдеров, у которых не исчерпан суточный лимит по SMS.")
            return Response("NO_NUMBERS", media_type="text/plain")
        
        base_query = db.query(models.PhoneNumber.id).filter(
            models.PhoneNumber.provider_id.in_(providers_within_limit_ids), 
            models.PhoneNumber.country_id == country, 
            models.PhoneNumber.is_active.is_(True), 
            models.PhoneNumber.is_in_use.is_(False)
        )
        
        if operator and operator != "any":
            db_op = db.query(models.Operator).filter(
                models.Operator.name == operator, 
                models.Operator.country_id == country
            ).first()
            
            if db_op:
                base_query = base_query.filter(models.PhoneNumber.operator_id == db_op.id)
            else:
                log.warning(f"Запрошен несуществующий оператор '{operator}' для страны ID {country}.")
                return Response("NO_NUMBERS", media_type="text/plain")

        query_with_usage = base_query.outerjoin(
            models.PhoneNumberUsage, 
            (models.PhoneNumber.id == models.PhoneNumberUsage.phone_number_id) & (models.PhoneNumberUsage.service_id == db_service.id)
        )
        
        best_number_query = query_with_usage.order_by(
            models.PhoneNumberUsage.usage_count.is_(None).desc(), 
            models.PhoneNumberUsage.usage_count.asc(), 
            models.PhoneNumber.sort_order.asc()
        ).limit(100)

        candidate_ids = [row[0] for row in best_number_query.all()]

        if not candidate_ids:
            return Response("NO_NUMBERS", media_type="text/plain")

        random.shuffle(candidate_ids) 
        
        num_obj = None

        for candidate_id in candidate_ids:
            try:
                num_to_lock = db.query(models.PhoneNumber).filter(
                    models.PhoneNumber.id == candidate_id
                ).with_for_update(nowait=True).first()
                
                if num_to_lock and not num_to_lock.is_in_use:
                    num_obj = num_to_lock
                    break 
            except Exception:
                continue 

        if not num_obj:
            log.warning("Не удалось заблокировать ни один из %d номеров-кандидатов, все оказались заняты.", len(candidate_ids))
            return Response("NO_NUMBERS", media_type="text/plain")
        
        num_obj.is_in_use = True
        norm = normalize_phone_number(num_obj.number_str)

        # --- НАЧАЛО ИЗМЕНЕНИЙ: Усиленная защита от гонки состояний ---
        if redis_client:
            # Устанавливаем "сигнальный" ключ в Redis на 10 секунд (увеличено)
            redis_client.set(f"pending_session:{norm}", 1, ex=10)
            log.info(f"Установлен сигнальный ключ в Redis для {norm}")
            # Добавляем крошечную задержку для гарантии записи перед commit'ом в БД
            time.sleep(0.1) 
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        sess = models.Session(phone_number_str=norm, service_id=db_service.id, phone_number_id=num_obj.id, api_key_id=db_key.id, status=1)
        db.add(sess); db.commit(); db.refresh(sess)
        return Response(f"ACCESS_NUMBER:{sess.id}:{norm.replace('+', '')}", media_type="text/plain")

    if action == "getRepeatNumber":
        if not number or not service: return Response("BAD_ACTION", media_type="text/plain")
        
        norm = normalize_phone_number(number)
        if not norm: return Response("BAD_NUMBER", media_type="text/plain")
        
        num_obj = db.query(models.PhoneNumber).options(selectinload(models.PhoneNumber.provider)).filter(models.PhoneNumber.number_str == norm).with_for_update().first()
        
        if not num_obj: return Response("NO_ACTIVATION", media_type="text/plain")
        if num_obj.is_in_use: return Response("NUMBER_BUSY", media_type="text/plain")
        
        target_service = db.query(models.Service).filter(models.Service.code == service).first()
        if not target_service: return Response("BAD_SERVICE", media_type="text/plain")
        
        today_db_date = db.query(func.current_date()).scalar()
        
        if target_service.daily_limit is not None:
            service_sms_count = (db.query(models.SmsMessage).join(models.Session).filter(models.Session.service_id == target_service.id, cast(models.SmsMessage.received_at, Date) == today_db_date).count())
            if service_sms_count >= target_service.daily_limit:
                log.warning(f"ЛИМИТ СЕРВИСА (getRepeatNumber): Лимит для '{target_service.name}' ({target_service.daily_limit}) достигнут.")
                return Response("NO_NUMBERS", media_type="text/plain")
                
        provider = num_obj.provider
        if provider and provider.daily_limit is not None:
            provider_sms_count = (db.query(models.SmsMessage).join(models.Session).join(models.PhoneNumber).filter(models.PhoneNumber.provider_id == provider.id, cast(models.SmsMessage.received_at, Date) == today_db_date).count())
            if provider_sms_count >= provider.daily_limit:
                log.warning(f"ОБЩИЙ ЛИМИТ (getRepeatNumber): Провайдер '{provider.name}' ({provider.daily_limit}) достиг лимита.")
                return Response("NO_NUMBERS", media_type="text/plain")

        num_obj.is_in_use = True
        
        # --- НАЧАЛО ИЗМЕНЕНИЙ: Усиленная защита от гонки состояний для getRepeatNumber ---
        if redis_client:
            redis_client.set(f"pending_session:{norm}", 1, ex=10)
            log.info(f"Установлен сигнальный ключ в Redis для {norm} (getRepeatNumber)")
            time.sleep(0.05)
        # --- КОНЕЦ ИЗМЕНЕНИЙ ---

        sess = models.Session(phone_number_str=norm, service_id=target_service.id, phone_number_id=num_obj.id, api_key_id=db_key.id, status=1)
        db.add(sess); db.commit(); db.refresh(sess)
        return Response(f"ACCESS_NUMBER:{sess.id}:{norm.replace('+', '')}", media_type="text/plain")

    if action == "getStatus":
        if id is None: return Response("BAD_ACTION", media_type="text/plain")
        sess = db.query(models.Session).filter(models.Session.id == id, models.Session.api_key_id == db_key.id).first()
        if not sess: return Response("NO_ACTIVATION", media_type="text/plain")
        if sess.status in (6, 8): return Response("STATUS_CANCEL", media_type="text/plain")
        last_sms = db.query(models.SmsMessage).filter(models.SmsMessage.session_id == id).order_by(models.SmsMessage.received_at.desc()).first()
        if last_sms and last_sms.code: return Response(f"STATUS_OK:{last_sms.code}", media_type="text/plain")
        if sess.status == 3: return Response("STATUS_WAIT_RETRY", media_type="text/plain")
        return Response("STATUS_WAIT_CODE", media_type="text/plain")

    if action == "setStatus":
        if id is None or status is None: return Response("BAD_ACTION", media_type="text/plain")
        sess = db.query(models.Session).options(selectinload(models.Session.phone_number)).filter(models.Session.id == id, models.Session.api_key_id == db_key.id).first()
        if not sess: return Response("NO_ACTIVATION", media_type="text/plain")
        
        num = sess.phone_number
        if status == 3:
            sess.status = 3
            db.commit()
            return Response("ACCESS_RETRY_GET", media_type="text/plain")

        if status in (6, 8):
            sess.status = status
            if num:
                num.is_in_use = False
                if status == 6:
                    usage_record = db.query(models.PhoneNumberUsage).filter_by(phone_number_id=num.id, service_id=sess.service_id).with_for_update().first()
                    if usage_record:
                        usage_record.usage_count += 1
                    else:
                        new_usage = models.PhoneNumberUsage(phone_number_id=num.id, service_id=sess.service_id, usage_count=1)
                        db.add(new_usage)
            db.commit()
            return Response("ACCESS_ACTIVATION" if status == 6 else "ACCESS_CANCEL", media_type="text/plain")
            
    return Response("BAD_ACTION", media_type="text/plain")