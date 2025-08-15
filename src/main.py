# src/main.py - ФИНАЛЬНАЯ ВЕРСИЯ С ИСПРАВЛЕНИЯМИ
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import threading
import datetime
import random
import time
import asyncio
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

from fastapi import Depends, FastAPI, Request, HTTPException, APIRouter
from sqlalchemy import func, cast, Date, text, select
from sqlalchemy.orm import Session, selectinload
from starlette.responses import JSONResponse, Response, RedirectResponse
from starlette_admin.contrib.sqla import Admin, ModelView
from starlette_admin.auth import AuthProvider
from starlette_admin.views import Link
from starlette_admin.fields import StringField, EnumField
from starlette.middleware.sessions import SessionMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import redis

from src.database import SessionLocal, engine
from src import models, smpp_worker, tools
from src import api_stats
from src.utils import normalize_phone_number
from src.logging_setup import setup_logging
setup_logging()


# --- Настройка логирования и Redis ---
#logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")

try:
    redis_client = redis.Redis(decode_responses=True)
    redis_client.ping()
    log.info("✓ Main: Успешное подключение к Redis.")
except redis.exceptions.ConnectionError as e:
    log.error(f"🔥 Main: Не удалось подключиться к Redis: {e}.")
    redis_client = None

# --- Фоновые процессы (без изменений) ---
background_threads: Dict[str, threading.Thread] = {}
stop_events: Dict[str, threading.Event] = {}

def delete_numbers_in_background(provider_id_str: str, country_id_str: str, is_in_use_str: str):
    db = SessionLocal()
    try:
        base_delete_sql = "DELETE FROM phone_numbers WHERE ctid IN (SELECT ctid FROM phone_numbers {where_clause} LIMIT :batch_size)"
        where_conditions, params = [], {}
        if provider_id_str:
            where_conditions.append("provider_id = :provider_id")
            params['provider_id'] = int(provider_id_str)
        if country_id_str:
            where_conditions.append("country_id = :country_id")
            params['country_id'] = int(country_id_str)
        if is_in_use_str:
            where_conditions.append("is_in_use = :is_in_use")
            params['is_in_use'] = (is_in_use_str == 'true')
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        delete_stmt = text(base_delete_sql.format(where_clause=where_clause))
        total_deleted_count, batch_size = 0, 20000
        log.info(f"[BG TASK] Начинаю массовое удаление номеров порциями по {batch_size}...")
        while True:
            params['batch_size'] = batch_size
            result = db.execute(delete_stmt, params)
            db.commit()
            deleted_in_batch = result.rowcount
            if deleted_in_batch == 0: break
            total_deleted_count += deleted_in_batch
            log.info(f"[BG TASK] Удалена порция из {deleted_in_batch} номеров. Всего удалено: {total_deleted_count}")
            time.sleep(0.05)
        log.info(f"[BG TASK] Массовое удаление завершено. Всего удалено: {total_deleted_count}")
    except Exception as e:
        log.error(f"[BG TASK] Ошибка при фоновом удалении: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()

def cleanup_expired_sessions(stop_event: threading.Event):
    log.info("▶︎ Запущен поток-уборщик старых сессий.")
    if not stop_event.wait(timeout=60):
        while not stop_event.wait(timeout=300):
            log.info("🧹 Выполняю очистку сессий старше 20 минут...")
            db = SessionLocal()
            try:
                expiration_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=20)
                sessions_to_cancel = db.query(models.Session).filter(models.Session.status.in_([1, 3]), models.Session.created_at < expiration_time).options(selectinload(models.Session.phone_number)).all()
                if sessions_to_cancel:
                    log.warning(f"🧹 Найдено {len(sessions_to_cancel)} сессий без SMS. Отменяю...")
                    for sess in sessions_to_cancel:
                        sess.status = 8
                        if sess.phone_number: sess.phone_number.is_in_use = False
                    db.commit()
                sessions_to_complete = db.query(models.Session).filter(models.Session.status == 2, models.Session.created_at < expiration_time).options(selectinload(models.Session.phone_number)).all()
                if sessions_to_complete:
                    log.warning(f"🧹 Найдено {len(sessions_to_complete)} незакрытых сессий с SMS. Завершаю...")
                    for sess in sessions_to_complete:
                        sess.status = 6
                        if sess.phone_number: sess.phone_number.is_in_use = False
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

# --- Настройка FastAPI и админ-панели ---
app = FastAPI(title="SMS Activation API", version="FINAL-REFACTORED", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key="change-this-to-a-long-random-string")
Instrumentator().instrument(app).expose(app)

# --- АУТЕНТИФИКАЦИЯ В АДМИНКЕ (без изменений) ---
class SimpleAuthProvider(AuthProvider):
    ADMIN_USERNAME = "admin"
    ADMIN_PASSWORD = "gfdtkcvccthdbc@"
    async def is_authenticated(self, request: Request) -> bool: return "is_authenticated" in request.session
    async def get_display_name(self, request: Request) -> str: return request.session.get("username", "Admin")
    async def get_photo_url(self, request: Request, **kwargs) -> Optional[str]: return None
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

# --- ПРЕДСТАВЛЕНИЯ АДМИНКИ (ИСПРАВЛЕНИЕ: ВОЗВРАЩЕНО К ОРИГИНАЛЬНОМУ, РАБОТАЮЩЕМУ СИНТАКСИСУ) ---
class ProviderView(ModelView):
    fields = ["id", "name", EnumField("connection_type", label="Connection Type", choices=[("outbound", "Outbound (Мы к ним)"), ("inbound", "Inbound (Они к нам)")]), "smpp_host", "smpp_port", "system_id", "password", "system_type", "is_active", "daily_limit"]
class ServiceView(ModelView): fields = ["id", "name", "code", "icon_class", "daily_limit"]
class PhoneNumberView(ModelView): fields = ["id", "number_str", "provider", "country", "operator", "is_active", "is_in_use", "sort_order"]; page_size = 100
class SessionView(ModelView): fields = ["id", "phone_number_str", "status", "created_at", "service", "phone_number", "api_key"]; page_size = 100; fields_default_sort = "-id"
class SmsMessageView(ModelView):
    preloads = ["session"]
    fields = ["id", "session", StringField("phone_number", label="Phone Number", exclude_from_create=True, exclude_from_edit=True), "source_addr", "text", "code", "received_at"]
    page_size = 100
    fields_default_sort = "-id"
class ApiKeyView(ModelView): fields = ["id", "key", "description", "is_active", "created_at"]; fields_default_sort = "-created_at"
class OperatorView(ModelView): fields = ["id", "name", "country", "provider"]
class ServiceLimitView(ModelView): fields = ["id", "service", "provider", "country", "daily_limit"]; page_size = 100

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
app.include_router(api_stats.router)

# --- API ЭНДПОИНТЫ (НОВЫЙ, РЕФАКТОРЕННЫЙ ПОДХОД) ---
api_router = APIRouter()

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

async def get_valid_api_key(api_key: str, db: Session = Depends(get_db)) -> models.ApiKey:
    db_key = db.query(models.ApiKey).filter(models.ApiKey.key == api_key, models.ApiKey.is_active.is_(True)).first()
    if not db_key:
        raise HTTPException(status_code=401, detail="BAD_KEY")
    return db_key

async def _create_session_for_number(db: Session, num_obj: models.PhoneNumber, service: models.Service, api_key: models.ApiKey) -> Response:
    num_obj.is_in_use = True
    norm = normalize_phone_number(num_obj.number_str)
    sess = models.Session(phone_number_str=norm, service_id=service.id, phone_number_id=num_obj.id, api_key_id=api_key.id, status=1)
    db.add(sess)
    db.commit()
    db.refresh(sess)
    log.warning(f"!!! DEBUG: СОЗДАНА СЕССИЯ ID={sess.id} ДЛЯ НОМЕРА '{sess.phone_number_str}' СТАТУС={sess.status} !!!")
    if redis_client:
        redis_client.set(f"pending_session:{norm}", 1, ex=20)
        log.info(f"Установлен сигнальный ключ в Redis для {norm} ПОСЛЕ создания сессии {sess.id}")
    return Response(f"ACCESS_NUMBER:{sess.id}:{norm.replace('+', '')}", media_type="text/plain")

@api_router.get("/stubs/handler_api.php")
async def handle_api(
    action: str,
    api_key_obj: models.ApiKey = Depends(get_valid_api_key),
    service: Optional[str] = None, country: Optional[int] = None, operator: Optional[str] = None,
    id: Optional[int] = None, status: Optional[int] = None, number: Optional[str] = None,
    db: Session = Depends(get_db)
):
    try:
        if action == "getBalance": return Response("ACCESS_BALANCE:9999", media_type="text/plain")
        if action == "getNumbersStatus":
            if country is None: return Response("BAD_ACTION", media_type="text/plain")
            q = db.query(models.Service.code, func.count(models.PhoneNumber.id)).select_from(models.Service).outerjoin(models.PhoneNumber, (models.PhoneNumber.is_active.is_(True)) & (models.PhoneNumber.is_in_use.is_(False)) & (models.PhoneNumber.country_id == country)).group_by(models.Service.code)
            return JSONResponse({f"{code}_0": cnt for code, cnt in q.all()})
        if action == "getNumber":
            if service is None or country is None: return Response("BAD_ACTION", media_type="text/plain")
            return await get_number(db, api_key_obj, service, country, operator)
        if action == "getRepeatNumber":
            if not number or not service: return Response("BAD_ACTION", media_type="text/plain")
            return await get_repeat_number(db, api_key_obj, service, number)
        if action == "getStatus":
            if id is None: return Response("BAD_ACTION", media_type="text/plain")
            sess = db.query(models.Session).filter(models.Session.id == id, models.Session.api_key_id == api_key_obj.id).first()
            if not sess: return Response("NO_ACTIVATION", media_type="text/plain")
            if sess.status in (6, 8): return Response("STATUS_CANCEL", media_type="text/plain")
            last_sms = db.query(models.SmsMessage).filter(models.SmsMessage.session_id == id).order_by(models.SmsMessage.received_at.desc()).first()
            if last_sms and last_sms.code: return Response(f"STATUS_OK:{last_sms.code}", media_type="text/plain")
            return Response("STATUS_WAIT_CODE" if sess.status != 3 else "STATUS_WAIT_RETRY", media_type="text/plain")
        if action == "setStatus":
            if id is None or status is None: return Response("BAD_ACTION", media_type="text/plain")
            sess = db.query(models.Session).options(selectinload(models.Session.phone_number)).filter(models.Session.id == id, models.Session.api_key_id == api_key_obj.id).first()
            if not sess: return Response("NO_ACTIVATION", media_type="text/plain")
            if status == 3: sess.status = 3; db.commit(); return Response("ACCESS_RETRY_GET", media_type="text/plain")
            if status in (6, 8):
                sess.status = status
                if sess.phone_number:
                    sess.phone_number.is_in_use = False
                    if status == 6:
                        usage_record = db.query(models.PhoneNumberUsage).filter_by(phone_number_id=sess.phone_number.id, service_id=sess.service_id).with_for_update().first()
                        if usage_record: usage_record.usage_count += 1
                        else: db.add(models.PhoneNumberUsage(phone_number_id=sess.phone_number.id, service_id=sess.service_id, usage_count=1))
                db.commit()
                return Response("ACCESS_ACTIVATION" if status == 6 else "ACCESS_CANCEL", media_type="text/plain")
        return Response("BAD_ACTION", media_type="text/plain")
    except HTTPException as e:
        return Response(e.detail, media_type="text/plain", status_code=200) # Возвращаем 200 OK для совместимости

async def get_number(db: Session, api_key_obj: models.ApiKey, service_code: str, country_id: int, operator_name: Optional[str]):
    db_service = db.query(models.Service).filter(models.Service.code == service_code).first()
    if not db_service: return Response("BAD_SERVICE", media_type="text/plain")
    today_db_date = db.query(func.current_date()).scalar()
    available_providers_q = db.query(models.Provider).join(models.PhoneNumber).filter(models.PhoneNumber.country_id == country_id, models.PhoneNumber.is_active.is_(True), models.PhoneNumber.is_in_use.is_(False)).distinct()
    providers_within_limit_ids = []
    for provider in available_providers_q:
        limit_to_check, limit_source = None, "None"
        specific_limit_rule = db.query(models.ServiceLimit).filter_by(service_id=db_service.id, provider_id=provider.id, country_id=country_id).first()
        if specific_limit_rule: limit_to_check, limit_source = specific_limit_rule.daily_limit, f"Правило (S+P+C): {specific_limit_rule.daily_limit}"
        elif db_service.daily_limit is not None: limit_to_check, limit_source = db_service.daily_limit, f"Сервис '{db_service.name}': {db_service.daily_limit}"
        elif provider.daily_limit is not None: limit_to_check, limit_source = provider.daily_limit, f"Провайдер '{provider.name}': {provider.daily_limit}"
        if limit_to_check is None: providers_within_limit_ids.append(provider.id); continue
        provider_sms_count = db.query(models.SmsMessage).join(models.Session).join(models.PhoneNumber).filter(models.PhoneNumber.provider_id == provider.id, models.Session.service_id == db_service.id, models.PhoneNumber.country_id == country_id, cast(models.SmsMessage.received_at, Date) == today_db_date).count()
        if provider_sms_count < limit_to_check: providers_within_limit_ids.append(provider.id)
        else: log.warning(f"ЛИМИТ: Провайдер '{provider.name}' исключен. Источник лимита: {limit_source}. Текущее кол-во SMS: {provider_sms_count}.")
    if not providers_within_limit_ids: log.warning("ЛИМИТЫ: Не найдено провайдеров, у которых не исчерпан суточный лимит по SMS."); return Response("NO_NUMBERS", media_type="text/plain")
    base_query = db.query(models.PhoneNumber.id).filter(models.PhoneNumber.provider_id.in_(providers_within_limit_ids), models.PhoneNumber.country_id == country_id, models.PhoneNumber.is_active.is_(True), models.PhoneNumber.is_in_use.is_(False))
    if operator_name and operator_name != "any":
        db_op = db.query(models.Operator).filter(models.Operator.name == operator_name, models.Operator.country_id == country_id).first()
        if db_op: base_query = base_query.filter(models.PhoneNumber.operator_id == db_op.id)
        else: log.warning(f"Запрошен несуществующий оператор '{operator_name}' для страны ID {country_id}."); return Response("NO_NUMBERS", media_type="text/plain")
    best_number_query = base_query.outerjoin(models.PhoneNumberUsage, (models.PhoneNumber.id == models.PhoneNumberUsage.phone_number_id) & (models.PhoneNumberUsage.service_id == db_service.id)).order_by(models.PhoneNumberUsage.usage_count.is_(None).desc(), models.PhoneNumberUsage.usage_count.asc(), models.PhoneNumber.sort_order.asc()).limit(100)
    candidate_ids = [row[0] for row in best_number_query.all()]
    if not candidate_ids: return Response("NO_NUMBERS", media_type="text/plain")
    random.shuffle(candidate_ids)
    num_obj = None
    for candidate_id in candidate_ids:
        try:
            num_to_lock = db.query(models.PhoneNumber).filter(models.PhoneNumber.id == candidate_id).with_for_update(nowait=True).first()
            if num_to_lock and not num_to_lock.is_in_use: num_obj = num_to_lock; break
        except Exception: continue
    if not num_obj: log.warning("Не удалось заблокировать ни один из %d номеров-кандидатов, все оказались заняты.", len(candidate_ids)); return Response("NO_NUMBERS", media_type="text/plain")
    return await _create_session_for_number(db, num_obj, db_service, api_key_obj)

async def get_repeat_number(db: Session, api_key_obj: models.ApiKey, service_code: str, number_str: str):
    norm = normalize_phone_number(number_str)
    if not norm: return Response("BAD_NUMBER", media_type="text/plain")
    num_obj = db.query(models.PhoneNumber).options(selectinload(models.PhoneNumber.provider)).filter(models.PhoneNumber.number_str == norm).with_for_update().first()
    if not num_obj: return Response("NO_ACTIVATION", media_type="text/plain")
    if num_obj.is_in_use: return Response("NUMBER_BUSY", media_type="text/plain")
    target_service = db.query(models.Service).filter(models.Service.code == service_code).first()
    if not target_service: return Response("BAD_SERVICE", media_type="text/plain")
    today_db_date = db.query(func.current_date()).scalar()
    if target_service.daily_limit is not None:
        service_sms_count = db.query(models.SmsMessage).join(models.Session).filter(models.Session.service_id == target_service.id, cast(models.SmsMessage.received_at, Date) == today_db_date).count()
        if service_sms_count >= target_service.daily_limit: log.warning(f"ЛИМИТ СЕРВИСА (getRepeatNumber): Лимит для '{target_service.name}' ({target_service.daily_limit}) достигнут."); return Response("NO_NUMBERS", media_type="text/plain")
    if num_obj.provider and num_obj.provider.daily_limit is not None:
        provider_sms_count = db.query(models.SmsMessage).join(models.Session).join(models.PhoneNumber).filter(models.PhoneNumber.provider_id == num_obj.provider.id, cast(models.SmsMessage.received_at, Date) == today_db_date).count()
        if provider_sms_count >= num_obj.provider.daily_limit: log.warning(f"ОБЩИЙ ЛИМИТ (getRepeatNumber): Провайдер '{num_obj.provider.name}' ({num_obj.provider.daily_limit}) достиг лимита."); return Response("NO_NUMBERS", media_type="text/plain")
    return await _create_session_for_number(db, num_obj, target_service, api_key_obj)

app.include_router(api_router)