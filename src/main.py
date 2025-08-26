# src/main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import logging
import threading
import datetime
import random
import time
import asyncio
import csv
import io
from urllib.parse import quote
from contextlib import asynccontextmanager
from typing import Optional, List, Dict

from fastapi import Depends, FastAPI, Request, HTTPException, APIRouter
from fastapi.responses import StreamingResponse, Response, RedirectResponse
from sqlalchemy import func, cast, Date, text, or_, and_
from sqlalchemy.orm import Session, selectinload, aliased
from sqlalchemy.sql.expression import select
from starlette.responses import JSONResponse
from starlette_admin.contrib.sqla import Admin, ModelView
from starlette_admin.auth import AuthProvider
from starlette_admin.views import Link
from starlette_admin.fields import StringField, EnumField, TextAreaField
from starlette.middleware.sessions import SessionMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import redis

from src.database import SessionLocal, engine
from src import models, tools, api_stats
from src.utils import normalize_phone_number
from src.logging_setup import setup_logging

# ---- logging (цвет/уровень берутся из .env: LOG_STYLE/LOG_LEVEL/LOG_SQL) ----
setup_logging()
log = logging.getLogger("main")

# === NEW: минимальный анти-дубль через “карантин” номера после использования ===
NUMBER_COOLDOWN_MINUTES = int(os.getenv("NUMBER_COOLDOWN_MINUTES", "30"))

# =========================
#     Redis (не обязателен)
# =========================
try:
    redis_client = redis.Redis(decode_responses=True)
    redis_client.ping()
    log.info("✓ Main: Успешное подключение к Redis.")
except redis.exceptions.ConnectionError as e:
    log.error(f"🔥 Main: Не удалось подключиться к Redis: {e}.")
    redis_client = None

# =========================
#   Фоновые процессы
# =========================
background_threads: Dict[str, threading.Thread] = {}
stop_events: Dict[str, threading.Event] = {}

def delete_numbers_in_background(provider_id_str: str, country_id_str: str, is_in_use_str: str):
    db = SessionLocal()
    try:
        base_delete_sql = (
            "DELETE FROM phone_numbers "
            "WHERE ctid IN (SELECT ctid FROM phone_numbers {where_clause} LIMIT :batch_size)"
        )
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
            deleted_in_batch = result.rowcount or 0
            if deleted_in_batch == 0:
                break
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

                # Просроченные без кода → CANCEL (8)
                sessions_to_cancel = (
                    db.query(models.Session)
                    .filter(models.Session.status.in_([1, 3]), models.Session.created_at < expiration_time)
                    .options(selectinload(models.Session.phone_number))
                    .all()
                )
                if sessions_to_cancel:
                    log.warning(f"🧹 Найдено {len(sessions_to_cancel)} сессий без SMS. Отменяю...")
                    for sess in sessions_to_cancel:
                        sess.status = 8
                        if sess.phone_number:
                            sess.phone_number.is_in_use = False
                    db.commit()

                # С кодом, но не закрытые → COMPLETE (6)
                sessions_to_complete = (
                    db.query(models.Session)
                    .filter(models.Session.status == 2, models.Session.created_at < expiration_time)
                    .options(selectinload(models.Session.phone_number))
                    .all()
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
    stop_cleanup_event = threading.Event()
    cleanup_thread = threading.Thread(
        target=cleanup_expired_sessions,
        args=(stop_cleanup_event,),
        daemon=True,
        name="Session-Cleaner",
    )
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

# =========================
#     FastAPI + Admin
# =========================
app = FastAPI(title="SMS Activation API", version="FINAL-REFACTORED", lifespan=lifespan)
@app.middleware("http")
async def _perf_timer(request, call_next):
    import time
    t0 = time.time()
    resp = await call_next(request)
    dt_ms = (time.time() - t0) * 1000
    path = request.url.path
    # короткий лог: что именно долго
    level = "WARNING" if dt_ms > 300 else "INFO"
    logging.getLogger("perf").log(
        logging.WARNING if level=="WARNING" else logging.INFO,
        "[%s] %s %.1f ms", request.method, path, dt_ms
    )
    return resp
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "CHANGE_ME_NOW__32+chars"),  # ← вынесено в .env
)
Instrumentator().instrument(app).expose(app)

class SimpleAuthProvider(AuthProvider):
    ADMIN_USERNAME = os.getenv("ADMIN_USER", "admin")             # ← .env
    ADMIN_PASSWORD = os.getenv("ADMIN_PASS", "super-secret")      # ← .env

    async def is_authenticated(self, request: Request) -> bool:
        return "is_authenticated" in request.session

    async def get_display_name(self, request: Request) -> str:
        return request.session.get("username", "Admin")

    async def get_photo_url(self, request: Request, **kwargs) -> Optional[str]:
        return None

    async def login(self, username: str, password: str, remember_me: bool,
                    request: Request, response: Response) -> Response:
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

class ProviderView(ModelView):
    fields = [
        "id",
        "name",
        EnumField("connection_type", label="Connection Type", choices=[
            ("outbound", "Outbound (Мы к ним)"),
            ("inbound", "Inbound (Они к нам)")
        ]),
        "smpp_host",
        "smpp_port",
        "system_id",
        "password",
        "system_type",
        "is_active",
        "daily_limit",
    ]

class ServiceView(ModelView):
    # --- Я ИЗМЕНИЛ ЭТУ СТРОКУ ---
    fields = ["id", "name", "code", "icon_class", "allowed_senders", "daily_limit"]
    # -----------------------------

class PhoneNumberView(ModelView):
    # Поля, которые отображаются в общем списке (убрали лишнее)
    fields_for_list = ["number_str", "provider", "country", "is_active", "is_in_use"]
    
    # Поля, по которым можно будет искать (поиск по номеру)
    searchable_fields = ["number_str"]

    # Поля, которые появятся в колонке фильтров
    fields_for_list_filters = ["provider", "country", "is_active", "is_in_use"]

    # Остальные поля, как и раньше, будут доступны при редактировании
    fields = ["id", "number_str", "provider", "country", "operator", "is_active", "is_in_use", "sort_order"]
    preloads = ["provider", "country", "operator"]
    page_size = 50

class SessionView(ModelView):
    fields = ["id", "phone_number_str", "status", "created_at", "service", "phone_number", "api_key"]
    preloads = ["service", "phone_number", "api_key"]
    page_size = 50
    fields_default_sort = "-id"

class SmsMessageView(ModelView):
    preloads = ["session"]
    fields = ["id", "session", StringField("phone_number", label="Phone Number",
             exclude_from_create=True, exclude_from_edit=True), "source_addr", "text", "code", "received_at"]
    page_size = 50
    fields_default_sort = "-id"

class ApiKeyView(ModelView):
    fields = ["id", "key", "description", "is_active", "created_at"]
    fields_default_sort = "-created_at"

class OperatorView(ModelView):
    fields = ["id", "name", "country", "provider"]

class ServiceLimitView(ModelView):
    fields = ["id", "service", "provider", "country", "daily_limit"]
    page_size = 5

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

# =========================
#           API
# =========================
api_router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def get_valid_api_key(api_key: str, db: Session = Depends(get_db)) -> models.ApiKey:
    db_key = (
        db.query(models.ApiKey)
        .filter(models.ApiKey.key == api_key, models.ApiKey.is_active.is_(True))
        .first()
    )
    if not db_key:
        raise HTTPException(status_code=401, detail="BAD_KEY")
    return db_key

async def _create_session_for_number(db: Session, num_obj: models.PhoneNumber,
                                     service: models.Service, api_key: models.ApiKey) -> Response:
    num_obj.is_in_use = True
    norm = normalize_phone_number(num_obj.number_str)
    sess = models.Session(
        phone_number_str=norm,
        service_id=service.id,
        phone_number_id=num_obj.id,
        api_key_id=api_key.id,
        status=1,
    )
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
    service: Optional[str] = None,
    country: Optional[int] = None,
    operator: Optional[str] = None,
    id: Optional[int] = None,
    status: Optional[int] = None,
    number: Optional[str] = None,
    db: Session = Depends(get_db),
):
    try:
        if action == "getBalance":
            return Response("ACCESS_BALANCE:9999", media_type="text/plain")

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

        if action == "getNumber":
            if service is None or country is None:
                return Response("BAD_ACTION", media_type="text/plain")
            return await get_number(db, api_key_obj, service, country, operator)

        if action == "getRepeatNumber":
            if not number or not service:
                return Response("BAD_ACTION", media_type="text/plain")
            return await get_repeat_number(db, api_key_obj, service, number)

        if action == "getStatus":
            if id is None:
                return Response("BAD_ACTION", media_type="text/plain")
            sess = (
                db.query(models.Session)
                .filter(models.Session.id == id, models.Session.api_key_id == api_key_obj.id)
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
            return Response("STATUS_WAIT_CODE" if sess.status != 3 else "STATUS_WAIT_RETRY", media_type="text/plain")

        if action == "setStatus":
            if id is None or status is None:
                return Response("BAD_ACTION", media_type="text/plain")
            sess = (
                db.query(models.Session)
                .options(selectinload(models.Session.phone_number))
                .filter(models.Session.id == id, models.Session.api_key_id == api_key_obj.id)
                .first()
            )
            if not sess:
                return Response("NO_ACTIVATION", media_type="text/plain")

            if status == 3:
                sess.status = 3
                db.commit()
                return Response("ACCESS_RETRY_GET", media_type="text/plain")

            if status in (6, 8):
                sess.status = status
                if sess.phone_number:
                    # номер освобождаем
                    sess.phone_number.is_in_use = False

                    # === NEW: всегда фиксируем время последнего использования (для анти-дублей)
                    usage_record = (
                        db.query(models.PhoneNumberUsage)
                        .filter_by(phone_number_id=sess.phone_number.id, service_id=sess.service_id)
                        .with_for_update()
                        .first()
                    )
                    if usage_record:
                        # для успешной активации сохраним статистику, как и раньше
                        if status == 6:
                            usage_record.usage_count += 1
                        # Важное: обновляем "последнее использ." всегда (и для 6, и для 8)
                        usage_record.last_used_at = func.now()
                    else:
                        # не было записи — создаём (для 6 usage_count=1, для 8 — 0)
                        db.add(
                            models.PhoneNumberUsage(
                                phone_number_id=sess.phone_number.id,
                                service_id=sess.service_id,
                                usage_count=(1 if status == 6 else 0),
                                last_used_at=func.now(),
                            )
                        )
                db.commit()
                return Response("ACCESS_ACTIVATION" if status == 6 else "ACCESS_CANCEL", media_type="text/plain")

        return Response("BAD_ACTION", media_type="text/plain")

    except HTTPException as e:
        # Возвращаем 200 OK для совместимости с клиентами
        return Response(e.detail, media_type="text/plain", status_code=200)

# ---------- Бизнес-ручки ----------
async def get_number(db: Session, api_key_obj: models.ApiKey,
                     service_code: str, country_id: int, operator_name: Optional[str]):
    """
    Оптимизированная версия v4 (финальная), основанная на диагностике.
    1. Убран медленный JOIN+DISTINCT для проверки лимитов.
    2. Убрана медленная сортировка `ORDER BY` в базе данных.
    3. Выборка номера происходит в 4 быстрых этапа:
        - Получение ID провайдеров, у которых не исчерпан лимит.
        - Получение небольшой пачки (до 200) ID номеров-кандидатов без медленной сортировки.
        - Перемешивание малого списка ID в коде (мгновенно).
        - Атомарная блокировка одного номера из кандидатов через `FOR UPDATE SKIP LOCKED`.
    """
    db_service = db.query(models.Service).filter(models.Service.code == service_code).first()
    if not db_service:
        return Response("BAD_SERVICE", media_type="text/plain")

    today_db_date = db.query(func.current_date()).scalar()

    # --- ШАГ 1: Сверхбыстрая проверка лимитов ---
    SmsCount = aliased(
        select(
            models.PhoneNumber.provider_id,
            func.count().label("sms_today")
        )
        .join(models.Session, models.Session.phone_number_id == models.PhoneNumber.id)
        .join(models.SmsMessage, models.SmsMessage.session_id == models.Session.id)
        .where(
            models.Session.service_id == db_service.id,
            models.PhoneNumber.country_id == country_id,
            cast(models.SmsMessage.received_at, Date) == today_db_date
        )
        .group_by(models.PhoneNumber.provider_id)
        .subquery("sms_count")
    )

    q_providers = (
        db.query(models.Provider.id)
        .outerjoin(SmsCount, SmsCount.c.provider_id == models.Provider.id)
        .outerjoin(
            models.ServiceLimit,
            and_(
                models.ServiceLimit.provider_id == models.Provider.id,
                models.ServiceLimit.service_id == db_service.id,
                models.ServiceLimit.country_id == country_id,
            ),
        )
        .filter(
            func.coalesce(SmsCount.c.sms_today, 0) < func.coalesce(
                models.ServiceLimit.daily_limit,
                db_service.daily_limit,
                models.Provider.daily_limit,
                1_000_000_000  # "Бесконечный" лимит
            )
        )
    )
    providers_within_limit_ids = [row[0] for row in q_providers.all()]

    if not providers_within_limit_ids:
        log.warning(f"ЛИМИТЫ: Не найдено провайдеров для сервиса '{service_code}' в стране {country_id}, у которых не исчерпан суточный лимит.")
        return Response("NO_NUMBERS", media_type="text/plain")

    # --- ШАГ 2: Получение пачки кандидатов БЕЗ медленной сортировки в БД ---
    base_query_ids = (
        db.query(models.PhoneNumber.id)
        .filter(
            models.PhoneNumber.provider_id.in_(providers_within_limit_ids),
            models.PhoneNumber.country_id == country_id,
            models.PhoneNumber.is_active.is_(True),
            models.PhoneNumber.is_in_use.is_(False),
        )
    )
    if operator_name and operator_name != "any":
        base_query_ids = base_query_ids.filter(
            models.PhoneNumber.operator.has(models.Operator.name == operator_name)
        )

    cutoff_expr = func.now() - text(f"interval '{NUMBER_COOLDOWN_MINUTES} minutes'")
    query_ids_with_cooldown = (
        base_query_ids.outerjoin(
            models.PhoneNumberUsage,
            (models.PhoneNumber.id == models.PhoneNumberUsage.phone_number_id)
            & (models.PhoneNumberUsage.service_id == db_service.id)
        ).filter(
            or_(
                models.PhoneNumberUsage.phone_number_id.is_(None),
                models.PhoneNumberUsage.last_used_at < cutoff_expr,
            )
        )
    )

    # БЫСТРО получаем до 200 кандидатов. Сортируем по индексированным полям для скорости.
    candidate_rows = query_ids_with_cooldown.order_by(models.PhoneNumber.sort_order, models.PhoneNumber.id).limit(200).all()
    candidate_ids = [row[0] for row in candidate_rows]

    if not candidate_ids:
        log.warning("Не найдено номеров с учетом 'карантина', пробую найти любой свободный...")
        candidate_rows = base_query_ids.order_by(models.PhoneNumber.sort_order, models.PhoneNumber.id).limit(200).all()
        candidate_ids = [row[0] for row in candidate_rows]

    if not candidate_ids:
        log.warning(f"Нет свободных номеров для сервиса '{service_code}' и страны {country_id}.")
        return Response("NO_NUMBERS", media_type="text/plain")

    # --- ШАГ 3: Случайный выбор в коде (мгновенно) ---
    random.shuffle(candidate_ids)

    # --- ШАГ 4: Атомарная блокировка одного номера ---
    # Этот запрос очень быстрый, так как работает с небольшим списком ID
    num_obj = (
        db.query(models.PhoneNumber)
        .filter(
            models.PhoneNumber.id.in_(candidate_ids),
            models.PhoneNumber.is_in_use.is_(False) # Повторная проверка на случай гонки
        )
        .limit(1)
        .with_for_update(skip_locked=True)
        .first()
    )

    if not num_obj:
        log.warning(f"Не удалось заблокировать ни один из {len(candidate_ids)} номеров-кандидатов (все оказались заняты).")
        return Response("NO_NUMBERS", media_type="text/plain")

    return await _create_session_for_number(db, num_obj, db_service, api_key_obj)

async def get_repeat_number(db: Session, api_key_obj: models.ApiKey, service_code: str, number_str: str):
    norm = normalize_phone_number(number_str)
    if not norm:
        return Response("BAD_NUMBER", media_type="text/plain")

    num_obj = (
        db.query(models.PhoneNumber)
        .options(selectinload(models.PhoneNumber.provider))
        .filter(models.PhoneNumber.number_str == norm)
        .with_for_update()
        .first()
    )
    if not num_obj:
        return Response("NO_ACTIVATION", media_type="text/plain")
    if num_obj.is_in_use:
        return Response("NUMBER_BUSY", media_type="text/plain")

    target_service = db.query(models.Service).filter(models.Service.code == service_code).first()
    if not target_service:
        return Response("BAD_SERVICE", media_type="text/plain")

    today_db_date = db.query(func.current_date()).scalar()

    # 1) Узкий лимит: service + provider + country
    specific_limit_rule = (
        db.query(models.ServiceLimit)
        .filter(
            models.ServiceLimit.service_id == target_service.id,
            models.ServiceLimit.provider_id == num_obj.provider_id,
            models.ServiceLimit.country_id == num_obj.country_id,
        )
        .first()
    )
    if specific_limit_rule is not None:
        spc_sms_count = (
            db.query(models.SmsMessage)
            .join(models.Session)
            .join(models.PhoneNumber)
            .filter(
                models.Session.service_id == target_service.id,
                models.PhoneNumber.provider_id == num_obj.provider_id,
                models.PhoneNumber.country_id == num_obj.country_id,
                cast(models.SmsMessage.received_at, Date) == today_db_date,
            )
            .count()
        )
        if spc_sms_count >= specific_limit_rule.daily_limit:
            log.warning(
                f"ЛИМИТ (getRepeatNumber): сервис={target_service.id}, провайдер={num_obj.provider_id}, страна={num_obj.country_id} достигнут ({specific_limit_rule.daily_limit})."
            )
            return Response("NO_NUMBERS", media_type="text/plain")

    # 2) Общий лимит сервиса
    if target_service.daily_limit is not None:
        service_sms_count = (
            db.query(models.SmsMessage)
            .join(models.Session)
            .filter(
                models.Session.service_id == target_service.id,
                cast(models.SmsMessage.received_at, Date) == today_db_date,
            )
            .count()
        )
        if service_sms_count >= target_service.daily_limit:
            log.warning(
                f"ЛИМИТ СЕРВИСА (getRepeatNumber): '{target_service.name}' ({target_service.daily_limit}) достигнут."
            )
            return Response("NO_NUMBERS", media_type="text/plain")

    # 3) Общий лимит провайдера
    if num_obj.provider and num_obj.provider.daily_limit is not None:
        provider_sms_count = (
            db.query(models.SmsMessage)
            .join(models.Session)
            .join(models.PhoneNumber)
            .filter(
                models.PhoneNumber.provider_id == num_obj.provider.id,
                cast(models.SmsMessage.received_at, Date) == today_db_date,
            )
            .count()
        )
        if provider_sms_count >= num_obj.provider.daily_limit:
            log.warning(
                f"ЛИМИТ ПРОВАЙДЕРА (getRepeatNumber): '{num_obj.provider.name}' ({num_obj.provider.daily_limit}) достигнут."
            )
            return Response("NO_NUMBERS", media_type="text/plain")

    # Всё ок — создаём новую сессию на тот же номер
    return await _create_session_for_number(db, num_obj, target_service, api_key_obj)

app.include_router(api_router)

# =========================
#   CSV EXPORT: ORPHAN SMS
# =========================
def _parse_bool(s: Optional[str]) -> bool:
    return str(s).lower() in {"1", "true", "yes", "y", "on"}

@app.get("/orphan/export.csv", name="orphan_export_csv")
def orphan_export_csv(
    request: Request,
    provider_id: Optional[int] = None,
    sender: Optional[str] = None,           # source_addr
    country_id: Optional[int] = None,
    operator_id: Optional[int] = None,
    null_only: Optional[str] = None,        # "true"/"false"
    date_from: Optional[str] = None,        # ISO: "2025-08-10" или "2025-08-10T00:00:00Z"
    date_to: Optional[str] = None,          # не включая, если не указано — now()
    db: Session = Depends(get_db),
):
    # Временной диапазон
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    start_dt = None
    end_dt = None
    try:
        if date_from:
            start_dt = datetime.datetime.fromisoformat(date_from.replace("Z", "+00:00"))
        if date_to:
            end_dt = datetime.datetime.fromisoformat(date_to.replace("Z", "+00:00"))
    except Exception:
        start_dt = end_dt = None

    if end_dt is None:
        end_dt = now_utc
    if start_dt is None:
        start_dt = end_dt - datetime.timedelta(days=7)  # дефолтно 7 дней

    q = (
        db.query(
            models.OrphanSms.phone_number_str.label("phone_number"),
            func.count().label("sms_count"),
            func.min(models.OrphanSms.received_at).label("first_seen"),
            func.max(models.OrphanSms.received_at).label("last_seen"),
        )
        .filter(models.OrphanSms.received_at >= start_dt, models.OrphanSms.received_at < end_dt)
    )

    # Фильтры
    if provider_id is not None:
        q = q.filter(models.OrphanSms.provider_id == provider_id)
    if sender:
        q = q.filter(models.OrphanSms.source_addr == sender)
    if country_id is not None:
        q = q.filter(models.OrphanSms.country_id == country_id)
    if operator_id is not None:
        q = q.filter(models.OrphanSms.operator_id == operator_id)
    if _parse_bool(null_only):
        q = q.filter(models.OrphanSms.provider_id.is_(None))

    q = q.group_by(models.OrphanSms.phone_number_str).order_by(func.count().desc())

    # Готовим CSV
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["phone_number", "sms_count", "first_seen", "last_seen"])
    for row in q.all():
        writer.writerow([
            row.phone_number,
            int(row.sms_count or 0),
            (row.first_seen or "").isoformat() if row.first_seen else "",
            (row.last_seen or "").isoformat() if row.last_seen else "",
        ])
    buf.seek(0)

    # Имя файла
    parts = []
    if provider_id is not None:
        parts.append(f"p{provider_id}")
    if sender:
        parts.append(sender)
    if country_id is not None:
        parts.append(f"c{country_id}")
    if operator_id is not None:
        parts.append(f"o{operator_id}")
    if _parse_bool(null_only):
        parts.append("only-null-provider")
    period = f"{start_dt.date()}_{end_dt.date()}"
    fname = f"orphan_numbers_{'-'.join(parts) or 'all'}_{period}.csv"
    headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{quote(fname)}"}
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8", headers=headers)