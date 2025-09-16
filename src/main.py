# src/main.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import logging
import threading
import datetime
import random
import time
import anyio
import asyncio
import psycopg
import csv
import io
from urllib.parse import quote
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Tuple, Callable, Any # ДОБАВЛЕНО Callable, Any

from fastapi import Depends, FastAPI, Request, HTTPException, APIRouter, Query
from fastapi.responses import StreamingResponse, Response, RedirectResponse
from sqlalchemy import func, text
from sqlalchemy.orm import Session, selectinload
from starlette.responses import JSONResponse
from starlette_admin.contrib.sqla import Admin, ModelView
from starlette_admin.auth import AuthProvider
from starlette_admin.views import Link
from starlette_admin.fields import StringField, EnumField
from starlette.middleware.sessions import SessionMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from src.database import SessionLocal, engine
from src import models, tools, api_stats, tester
from src.utils import normalize_phone_number
from src.logging_setup import setup_logging
from fastapi.responses import FileResponse
from pathlib import Path
from psycopg.types.numeric import NumericLoader
from src.revenue import router as revenue_router
from src.deps import get_redis



# ---- logging ----
setup_logging()
log = logging.getLogger("main")

# ======================================================================
#  ГЛОБАЛЬНЫЙ АДАПТЕР ТИПОВ ДЛЯ POSTGRESQL
#  Это решает ошибку "operator does not exist: integer = character varying"
#  Он сообщает драйверу, что строки, похожие на числа, нужно отправлять
#  в базу как числа, а не как текст.
# ======================================================================
def _parse_numeric(value, cursor):
    return NumericLoader.load(value, cursor)

def register_numeric_adapter(conn):
    if isinstance(conn, psycopg.Connection):
        conn.adapters.register_loader("numeric", _parse_numeric)
        # Адаптируем также для всех целочисленных типов
        for oid in [20, 21, 23, 26, 700, 701, 1700]:
            conn.adapters.register_loader(oid, psycopg.types.numeric.load_int)

_original_connect = psycopg.Connection.connect

def _new_connect(*args, **kwargs):
    conn = _original_connect(*args, **kwargs)
    register_numeric_adapter(conn)
    return conn

psycopg.Connection.connect = _new_connect
# ======================================================================

# === Настройки ротации/лимитов/кулдауна ===
NUMBER_COOLDOWN_MINUTES = int(os.getenv("NUMBER_COOLDOWN_MINUTES", "5"))
ALLOW_COOLDOWN_FALLBACK = os.getenv("ALLOW_COOLDOWN_FALLBACK", "1") == "1"
CANDIDATE_ATTEMPTS = int(os.getenv("CANDIDATE_ATTEMPTS", "30"))
PROVIDER_SAMPLE_SIZE = int(os.getenv("PROVIDER_SAMPLE_SIZE", "8"))
BOUNDS_CACHE_TTL = int(os.getenv("BOUNDS_CACHE_TTL", "600"))
# При падении Redis продолжаем работать, а лимиты считаем по БД
STRICT_LIMITS = os.getenv("STRICT_LIMITS", "1") == "1"
# TTL для прогретых лимитов (сек)
PRIME_LIMIT_TTL = int(os.getenv("PRIME_LIMIT_TTL", "15"))
# Опционально (по умолчанию ВЫКЛ): инкрементить редис-счётчики лимитов при setStatus=6
INCR_LIMITS_ON_SUCCESS = os.getenv("INCR_LIMITS_ON_SUCCESS", "0") == "1"

# =========================
#     Redis (опционально)
# =========================
redis_client = get_redis()
if redis_client:
    log.info("✓ Main: Redis клиент инициализирован через deps.")
else:
    log.warning("Main: Redis недоступен, продолжаем работу без кэша.")

# =========================
#   Фоновые процессы
# =========================
background_threads: Dict[str, threading.Thread] = {}
stop_events: Dict[str, threading.Event] = {}

def delete_numbers_in_background(
    provider_id_str: str,
    country_id_str: str,
    is_in_use_str: str,
    operator_id_str: str = "",
    prefix: str = "",
):
    db = SessionLocal()
    try:
        base_delete_sql = (
            "DELETE FROM phone_numbers "
            "WHERE ctid IN (SELECT ctid FROM phone_numbers {where_clause} LIMIT :batch_size)"
        )

        where_conditions, params = [], {}

        if provider_id_str:
            where_conditions.append("provider_id = :provider_id")
            params["provider_id"] = int(provider_id_str)
        if country_id_str:
            where_conditions.append("country_id = :country_id")
            params["country_id"] = int(country_id_str)
        if is_in_use_str:
            where_conditions.append("is_in_use = :is_in_use")
            params["is_in_use"] = (is_in_use_str == "true")
        if operator_id_str:
            where_conditions.append("operator_id = :operator_id")
            params["operator_id"] = int(operator_id_str)
        if prefix:
            where_conditions.append("number_str LIKE :prefix_like")
            params["prefix_like"] = f"{prefix}%"

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        delete_stmt = text(base_delete_sql.format(where_clause=where_clause))

        total_deleted_count, batch_size = 0, 20_000
        log.info(f"[BG TASK] Начинаю массовое удаление номеров порциями по {batch_size}...")
        while True:
            params["batch_size"] = batch_size
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

def shuffle_numbers_in_background(
    provider_id_str: str = "",
    country_id_str: str = "",
    operator_id_str: str = "",
):
    db = SessionLocal()
    try:
        where_conditions, params = [], {}
        if provider_id_str:
            where_conditions.append("provider_id = :provider_id")
            params["provider_id"] = int(provider_id_str)
        if country_id_str:
            where_conditions.append("country_id = :country_id")
            params["country_id"] = int(country_id_str)
        if operator_id_str:
            where_conditions.append("operator_id = :operator_id")
            params["operator_id"] = int(operator_id_str)

        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        base_update_sql = text(f"""
            UPDATE phone_numbers pn
               SET sort_order = FLOOR(random() * 1000000000)::int
             WHERE ctid IN (
                 SELECT ctid FROM phone_numbers
                 {where_clause}
                 LIMIT :batch_size
             )
        """)

        batch_size = 50_000
        total_updated = 0
        log.info(f"[BG SHUFFLE] Старт перемешивания sort_order порциями по {batch_size}...")
        while True:
            params["batch_size"] = batch_size
            res = db.execute(base_update_sql, params)
            db.commit()
            updated = res.rowcount or 0
            if updated == 0:
                break
            total_updated += updated
            log.info(f"[BG SHUFFLE] Обновлена порция {updated}. Всего обновлено: {total_updated}")
            time.sleep(0.05)
        log.info(f"[BG SHUFFLE] Завершено. Всего перемешано: {total_updated}.")
    except Exception as e:
        log.error(f"[BG SHUFFLE] Ошибка: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()

def cleanup_expired_sessions(stop_event: threading.Event):
    log.info("▶︎ Запущен поток-уборщик старых сессий.")
    if not stop_event.wait(timeout=60):
        while not stop_event.wait(timeout=300):
            log.info("🧹 Выполняю очистку сессий старше 10 минут...")
            db = SessionLocal()
            try:
                expiration_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=10)

                sessions_to_cancel = (
                    db.query(models.Session)
                    .filter(models.Session.status.in_([1, 3]),
                            models.Session.created_at < expiration_time)
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

                sessions_to_complete = (
                    db.query(models.Session)
                    .filter(models.Session.status == 2,
                            models.Session.created_at < expiration_time)
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
    try:
        yield
    finally:
        log.info("=== Lifespan shutdown: останавливаю фоновые процессы ===")
        for name, ev in stop_events.items():
            log.info(" ← останавливаю процесс %s", name)
            ev.set()
        for name, thr in background_threads.items():
            thr.join(timeout=2.0)
        log.info("Все фоновые процессы остановлены.")

app = FastAPI(title="SMS Activation API", version="LIMITS_BY_SMS+REDIS_PRIME", lifespan=lifespan)

@app.get("/standalone-tester", include_in_schema=False)
def standalone_tester():
    p = Path(__file__).resolve().parent.parent / "templates" / "api_tester.html"
    if not p.exists():
        return Response("api_tester.html not found", media_type="text/plain", status_code=404)
    return FileResponse(p)

@app.middleware("http")
async def _perf_timer(request, call_next):
    t0 = time.time()
    resp = await call_next(request)
    dt_ms = (time.time() - t0) * 1000
    level = "WARNING" if dt_ms > 300 else "INFO"
    logging.getLogger("perf").log(
        logging.WARNING if level == "WARNING" else logging.INFO,
        "[%s] %s %.1f ms", request.method, request.url.path, dt_ms
    )
    return resp

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "CHANGE_ME_NOW__32+chars"),
)
Instrumentator().instrument(app).expose(app)

# --------------------------
#  Admin панели
# --------------------------
class SimpleAuthProvider(AuthProvider):
    ADMIN_USERNAME = os.getenv("ADMIN_USER", "admin")
    ADMIN_PASSWORD = os.getenv("ADMIN_PASS", "super-secret")

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

# =================================================================
#    НОВЫЙ БЛОК: Базовый класс для всех View, который очищает кэш
# =================================================================
# =================================================================
#    БАЗОВЫЙ КЛАСС ДЛЯ VIEW: очищает кэш + безопасный edit
# =================================================================
class CacheClearingView(ModelView):
    # Функция очистки кэша (передаём из конкретного View)
    cache_clear_func: Optional[Callable] = None

    async def create(self, request: Request, data: Dict[str, Any]) -> Any:
        obj = await super().create(request, data)
        if self.cache_clear_func:
            log.info(f"Очищаю кэш для {self.identity} после создания записи...")
            self.cache_clear_func()
        return obj

    async def edit(self, request: Request, pk: Any, data: Dict[str, Any]) -> Any:
        """
        Исправление редкого бага starlette-admin:
        при обновлении иногда падает session.refresh(obj) с
        InvalidRequestError: 'Instance ... is not persistent'.
        Делаем надёжный fallback: вручную подгружаем объект, обновляем, коммитим.
        """
        try:
            obj = await super().edit(request, pk, data)
        except Exception as e:
            from sqlalchemy.exc import InvalidRequestError
            # Пытаемся "мягко" исправить ситуацию только для указанной ошибки
            if isinstance(e, InvalidRequestError) or "not persistent" in str(e).lower():
                session = request.state.session  # starlette-admin кладёт сессию сюда
                # Загружаем текущий объект из БД
                inst = await anyio.to_thread.run_sync(session.get, self.model, pk)
                if inst is None:
                    raise  # нет объекта — пробрасываем исходную ошибку

                # Применяем пришедшие поля (только существующие атрибуты)
                for k, v in (data or {}).items():
                    if hasattr(inst, k):
                        setattr(inst, k, v)

                await anyio.to_thread.run_sync(session.commit)
                obj = inst
            else:
                raise

        if self.cache_clear_func:
            log.info(f"Очищаю кэш для {self.identity} после редактирования записи...")
            self.cache_clear_func()
        return obj

    async def delete(self, request: Request, pks: List[Any]) -> int:
        count = await super().delete(request, pks)
        if self.cache_clear_func:
            log.info(f"Очищаю кэш для {self.identity} после удаления {count} записей...")
            self.cache_clear_func()
        return count
# =================================================================

# =================================================================
#    КОНЕЦ НОВОГО БЛОКА
# =================================================================


class ProviderView(CacheClearingView): # ИЗМЕНЕНИЕ ЗДЕСЬ
    fields = [
        "id", "name",
        EnumField("connection_type", label="Connection Type",
                  choices=[("outbound", "Outbound (Мы к ним)"), ("inbound", "Inbound (Они к нам)")]),
        "smpp_host", "smpp_port", "system_id", "password", "system_type", "is_active", "daily_limit",
    ]
    cache_clear_func = tools.get_cached_providers.cache_clear # ДОБАВЛЕНО

class ServiceView(CacheClearingView): # ИЗМЕНЕНИЕ ЗДЕСЬ
    fields = ["id", "name", "code", "icon_class", "allowed_senders", "daily_limit"]
    cache_clear_func = tools.get_cached_services.cache_clear # ДОБАВЛЕНО

class PhoneNumberView(ModelView):
    fields_for_list = ["number_str", "provider", "country", "is_active", "is_in_use"]
    searchable_fields = ["number_str"]
    fields_for_list_filters = ["provider", "country", "is_active", "is_in_use"]
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
    fields = ["id", "session",
              StringField("phone_number", label="Phone Number", exclude_from_create=True, exclude_from_edit=True),
              "source_addr", "text", "code", "received_at"]
    page_size = 50
    fields_default_sort = "-id"

class ApiKeyView(ModelView):
    fields = ["id", "key", "description", "is_active", "created_at"]
    fields_default_sort = "-created_at"

class OperatorView(CacheClearingView):
    fields = ["id", "name", "country", "provider", "price_eur_cent", "price_eur"]
    cache_clear_func = tools.get_cached_operators.cache_clear


class CountryView(CacheClearingView): # НОВЫЙ КЛАСС
    fields = ["id", "name", "iso_code", "phone_code"]
    cache_clear_func = tools.get_cached_countries.cache_clear

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
admin.add_view(CountryView(models.Country, icon="fa fa-flag")) # ИЗМЕНЕНИЕ ЗДЕСЬ
admin.add_view(OperatorView(models.Operator, icon="fa fa-wifi"))
admin.add_view(ServiceLimitView(models.ServiceLimit, icon="fa fa-balance-scale", label="Service Limits"))
admin.add_view(Link(label="Инструменты", icon="fa fa-tools", url="/tools"))
admin.mount_to(app)
app.include_router(tools.router)
app.include_router(api_stats.router)
app.include_router(tester.router)
app.include_router(revenue_router)


# --------------------------
#  API роуты
# --------------------------
api_router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ======== утилиты для "сегодня" (UTC) и кэша счётчиков ========
def _today_window_utc() -> Tuple[datetime.datetime, datetime.datetime]:
    now = datetime.datetime.now(datetime.timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + datetime.timedelta(days=1)
    return start, end

def _today_str() -> str:
    return datetime.date.today().isoformat()

def _cache_get(key: str) -> Optional[int]:
    if not redis_client:
        return None
    try:
        val = redis_client.get(key)
        return int(val) if val is not None else None
    except Exception:
        return None

def _cache_set(key: str, value: int, ttl: int = 10) -> None:
    if not redis_client:
        return
    try:
        redis_client.setex(key, ttl, int(value))
    except Exception:
        pass

def _mget_int(keys: List[str]) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {}
    if not redis_client or not keys:
        return {k: None for k in keys}
    try:
        vals = redis_client.mget(keys)
        for k, v in zip(keys, vals):
            out[k] = int(v) if v is not None else None
        return out
    except Exception:
        return {k: None for k in keys}

# --- НОВОЕ: прогрев лимитов с нулями и «штампом» ---
def _prime_provider_counters(
    db: Session,
    service_id: int,
    country_id: int,
    provider_ids: Optional[List[int]] = None,
    ttl_sec: int = PRIME_LIMIT_TTL,
) -> None:
    """
    Прогреваем счётчики за сегодня:
      - limit:{today}:{service_id}:{country_id}:{provider_id} = кол-во SMS
        (включая НУЛИ для всех активных провайдеров, если переданы provider_ids)
      - limit_service:{today}:{service_id}:{country_id} = сумма по провайдерам
      - prime_stamp:{today}:{service_id}:{country_id} = маркер "кэш прогрет"
    Чтобы не гонять параллельные GROUP BY — ставим короткий лок.
    """
    if not redis_client:
        return

    today = _today_str()
    lock_key = f"prime_lock:{today}:{service_id}:{country_id}"

    try:
        if not redis_client.set(lock_key, "1", nx=True, ex=10):
            return

        start, end = _today_window_utc()
        rows = (
            db.query(models.PhoneNumber.provider_id, func.count(models.SmsMessage.id))
              .join(models.Session, models.Session.id == models.SmsMessage.session_id)
              .join(models.PhoneNumber, models.PhoneNumber.id == models.Session.phone_number_id)
              .filter(models.Session.service_id == service_id,
                      models.PhoneNumber.country_id == country_id,
                      models.SmsMessage.received_at >= start,
                      models.SmsMessage.received_at < end)
              .group_by(models.PhoneNumber.provider_id)
              .all()
        )

        counts: Dict[int, int] = {int(pid) if pid is not None else 0: int(cnt or 0) for pid, cnt in rows}

        # ставим нули для всех активных провайдеров (если список указан)
        if provider_ids:
            for pid in provider_ids:
                counts.setdefault(pid, 0)

        total = sum(counts.values())

        pipe = redis_client.pipeline()
        for pid, cnt in counts.items():
            pipe.set(f"limit:{today}:{service_id}:{country_id}:{pid}", cnt, ex=ttl_sec)
        pipe.set(f"limit_service:{today}:{service_id}:{country_id}", total, ex=ttl_sec)
        pipe.set(f"prime_stamp:{today}:{service_id}:{country_id}", 1, ex=ttl_sec)
        pipe.execute()

        log.debug(
            "[prime] service=%s country=%s: total=%s, prov=%s (ttl=%ss)",
            service_id, country_id, total, len(counts), ttl_sec
        )
    except Exception as e:
        log.warning(f"[prime] ошибка прогрева лимитов: {e}")

# --- НОВОЕ: чтение кэша с трактовкой пустого ключа как 0 при прогреве ---
def _read_used_from_cache(
    service_id: int,
    country_id: int,
    provider_ids: List[int],
) -> Tuple[Optional[int], Dict[int, Optional[int]]]:
    """
    Возвращает (used_global, {pid: used_pid}) из Redis.

    Если есть prime_stamp (кэш прогрет) и по какому-то провайдеру ключа нет —
    считаем 0, а НЕ "кэш отсутствует".
    """
    if not redis_client:
        return None, {pid: None for pid in provider_ids}

    today = _today_str()
    gkey = f"limit_service:{today}:{service_id}:{country_id}"
    pkeys = [f"limit:{today}:{service_id}:{country_id}:{pid}" for pid in provider_ids]
    stamp_key = f"prime_stamp:{today}:{service_id}:{country_id}"

    vals = _mget_int([gkey] + pkeys)
    used_global = vals.get(gkey)
    stamp = _cache_get(stamp_key)  # 1, если прогрето; None — если нет

    used_by_provider: Dict[int, Optional[int]] = {}
    for pid, k in zip(provider_ids, pkeys):
        v = vals.get(k)
        if v is None and stamp:
            used_by_provider[pid] = 0
        else:
            used_by_provider[pid] = v

    return used_global, used_by_provider

# ======== Аутентификация API ключа ========
async def get_valid_api_key(api_key: str = Query(...), db: Session = Depends(get_db)) -> models.ApiKey:
    db_key = (
        db.query(models.ApiKey)
        .filter(models.ApiKey.key == api_key, models.ApiKey.is_active.is_(True))
        .first()
    )
    if not db_key:
        raise HTTPException(status_code=401, detail="BAD_KEY")
    return db_key

# ======== создание сессии ========
async def _create_session_for_locked_id(db: Session, phone_id: int, number_str: str,
                                        service: models.Service, api_key: models.ApiKey) -> Response:
    norm = normalize_phone_number(number_str)
    sess = models.Session(
        phone_number_str=norm,
        service_id=service.id,
        phone_number_id=phone_id,
        api_key_id=api_key.id,
        status=1,
    )
    db.add(sess)
    db.commit()
    db.refresh(sess)
    if redis_client:
        redis_client.set(f"pending_session:{norm}", 1, ex=20)
    return Response(f"ACCESS_NUMBER:{sess.id}:{norm.replace('+', '')}", media_type="text/plain")

async def _create_session_for_number(db: Session, num_obj: models.PhoneNumber,
                                     service: models.Service, api_key: models.ApiKey) -> Response:
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
    if redis_client:
        redis_client.set(f"pending_session:{norm}", 1, ex=20)
    return Response(f"ACCESS_NUMBER:{sess.id}:{norm.replace('+', '')}", media_type="text/plain")

# ======== HTTP API ========
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
    import json
    try:
        if action == "getBalance":
            return Response("ACCESS_BALANCE:9999", media_type="text/plain")

        if action == "getNumbersStatus":
            if country is None:
                return Response("BAD_ACTION", media_type="text/plain")
            cache_key = f"gns:{int(country)}"
            if redis_client:
                try:
                    cached = redis_client.get(cache_key)
                    if cached:
                        return JSONResponse(json.loads(cached))
                except Exception as re:
                    log.warning(f"Redis getNumbersStatus cache miss/error: {re}")

            q = (
                db.query(models.Service.code, func.count(models.PhoneNumber.id))
                .select_from(models.Service)
                .outerjoin(
                    models.PhoneNumber,
                    (models.PhoneNumber.is_active.is_(True)) &
                    (models.PhoneNumber.is_in_use.is_(False)) &
                    (models.PhoneNumber.country_id == country)
                )
                .group_by(models.Service.code)
            )
            result = {f"{code}_0": cnt for code, cnt in q.all()}

            if redis_client:
                try:
                    redis_client.setex(cache_key, 5, json.dumps(result))
                except Exception as re:
                    log.warning(f"Redis setex getNumbersStatus error: {re}")

            return JSONResponse(result)

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
            return Response("STATUS_WAIT_CODE" if sess.status != 3 else "STATUS_WAIT_RETRY",
                            media_type="text/plain")

        if action == "setStatus":
            if id is None or status is None:
                return Response("BAD_ACTION", media_type="text/plain")
            sess = (
                db.query(models.Session)
                .options(selectinload(models.Session.phone_number),
                         selectinload(models.Session.service))
                .filter(models.Session.id == id, models.Session.api_key_id == api_key_obj.id)
                .first()
            )
            if not sess:
                return Response("NO_ACTIVATION", media_type="text/plain")

            if status == 3:  # повторно подождать код
                sess.status = 3
                db.commit()
                return Response("ACCESS_RETRY_GET", media_type="text/plain")

            if status in (6, 8):  # завершить/отменить
                sess.status = status
                phone_number = sess.phone_number
                if phone_number:
                    phone_number.is_in_use = False
                    usage_record = (
                        db.query(models.PhoneNumberUsage)
                        .filter_by(phone_number_id=phone_number.id, service_id=sess.service_id)
                        .with_for_update()
                        .first()
                    )
                    if usage_record:
                        if status == 6:
                            usage_record.usage_count += 1
                        usage_record.last_used_at = func.now()
                    else:
                        db.add(models.PhoneNumberUsage(
                            phone_number_id=phone_number.id,
                            service_id=sess.service_id,
                            usage_count=(1 if status == 6 else 0),
                            last_used_at=func.now()
                        ))
                db.commit()

                # Кулдаун (лимиты считаются по факту пришедших SMS; инкремент по успеху — опционально)
                try:
                    if redis_client and phone_number:
                        pipe = redis_client.pipeline()
                        pipe.setex(
                            f"cool:{sess.service_id}:{phone_number.id}",
                            NUMBER_COOLDOWN_MINUTES * 60,
                            1
                        )
                        # Опционально ускорить лимиты (по умолчанию выкл)
                        if INCR_LIMITS_ON_SUCCESS and status == 6:
                            today = _today_str()
                            # по провайдеру
                            pkey = f"limit:{today}:{sess.service_id}:{phone_number.country_id}:{phone_number.provider_id}"
                            pipe.incr(pkey)
                            pipe.expire(pkey, 86400)
                            # глобально по сервису/стране
                            gkey = f"limit_service:{today}:{sess.service_id}:{phone_number.country_id}"
                            pipe.incr(gkey)
                            pipe.expire(gkey, 86400)
                        # Инвалидируем кэш статуса
                        pipe.delete(f"gns:{int(phone_number.country_id)}")
                        pipe.execute()
                except Exception as re:
                    log.warning(f"Redis обновление пропущено: {re}")

                return Response("ACCESS_ACTIVATION" if status == 6 else "ACCESS_CANCEL",
                                media_type="text/plain")

        return Response("BAD_ACTION", media_type="text/plain")
    except HTTPException as e:
        return Response(e.detail, media_type="text/plain", status_code=200)

# =========================
#   ЛОГИКА ВЫДАЧИ НОМЕРА
# =========================
def _as_infinite_limit(v: Optional[int]) -> int:
    """None или <=0 считаем 'без лимита'."""
    return 1_000_000_000 if (v is None or v <= 0) else v

def _bounds_cache_key(country_id: int, provider_id: int, operator_id: Optional[int]) -> str:
    op = operator_id if operator_id is not None else 0
    return f"bounds:{country_id}:{provider_id}:{op}"

def _get_id_bounds_cached(db: Session, country_id: int, provider_id: int,
                          operator_id: Optional[int]) -> Optional[Tuple[int, int]]:
    key = _bounds_cache_key(country_id, provider_id, operator_id)
    if redis_client:
        try:
            packed = redis_client.get(key)
            if packed:
                a, b = packed.split("|", 1)
                return int(a), int(b)
        except Exception as re:
            log.warning(f"[bounds] Redis get error: {re}")

    filters = [
        models.PhoneNumber.country_id == country_id,
        models.PhoneNumber.provider_id == provider_id,
        models.PhoneNumber.is_active.is_(True),
        models.PhoneNumber.is_in_use.is_(False),
    ]
    if operator_id is not None:
        filters.append(models.PhoneNumber.operator_id == operator_id)

    min_row = (
        db.query(models.PhoneNumber.id)
          .filter(*filters)
          .order_by(models.PhoneNumber.id.asc())
          .limit(1)
          .first()
    )
    if not min_row:
        return None

    max_row = (
        db.query(models.PhoneNumber.id)
          .filter(*filters)
          .order_by(models.PhoneNumber.id.desc())
          .limit(1)
          .first()
    )
    if not max_row:
        return None

    min_id, max_id = int(min_row[0]), int(max_row[0])
    if redis_client:
        try:
            redis_client.setex(key, BOUNDS_CACHE_TTL, f"{min_id}|{max_id}")
        except Exception:
            pass
    return min_id, max_id

def _count_sms_service_country(db: Session, service_id: int, country_id: int,
                               start: datetime.datetime, end: datetime.datetime) -> int:
    """
    Кол-во СМС за сегодня по (Service, Country).
    """
    cache_key = f"cnt:svc:{start.date().isoformat()}:{service_id}:{country_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cnt = (
        db.query(func.count(models.SmsMessage.id))
          .join(models.Session, models.Session.id == models.SmsMessage.session_id)
          .join(models.PhoneNumber, models.PhoneNumber.id == models.Session.phone_number_id)
          .filter(models.Session.service_id == service_id,
                  models.PhoneNumber.country_id == country_id,
                  models.SmsMessage.received_at >= start,
                  models.SmsMessage.received_at < end)
          .scalar()
    ) or 0
    _cache_set(cache_key, cnt, ttl=8)
    return cnt

def _count_sms_service_country_group_by_provider(db: Session, service_id: int, country_id: int,
                                                 start: datetime.datetime, end: datetime.datetime) -> Dict[int, int]:
    """
    Возвращает {provider_id: cnt} за сегодня по (Service, Country) с группировкой по провайдеру.
    Используется как фолбэк, если нет Redis/кэша.
    """
    cache_prefix = f"cnt:spc:{start.date().isoformat()}:{service_id}:{country_id}:"
    rows = (
        db.query(models.PhoneNumber.provider_id, func.count(models.SmsMessage.id))
          .join(models.Session, models.Session.id == models.SmsMessage.session_id)
          .join(models.PhoneNumber, models.PhoneNumber.id == models.Session.phone_number_id)
          .filter(models.Session.service_id == service_id,
                  models.PhoneNumber.country_id == country_id,
                  models.SmsMessage.received_at >= start,
                  models.SmsMessage.received_at < end)
          .group_by(models.PhoneNumber.provider_id)
          .all()
    )
    result: Dict[int, int] = {}
    for pid, cnt in rows:
        pid = int(pid) if pid is not None else 0
        result[pid] = int(cnt or 0)
        _cache_set(cache_prefix + str(pid), result[pid], ttl=8)
    return result

async def get_number(db: Session, api_key_obj: models.ApiKey,
                     service_code: str, country_id: int, operator_name: Optional[str]):

    # 1) сервис
    db_service = db.query(models.Service).filter(models.Service.code == service_code).first()
    if not db_service:
        return Response("BAD_SERVICE", media_type="text/plain")

    # 2) оператор (опционально)
    operator_id: Optional[int] = None
    if operator_name and operator_name != "any":
        op = (
            db.query(models.Operator.id)
              .filter(models.Operator.country_id == country_id,
                      models.Operator.name == operator_name)
              .first()
        )
        if not op:
            log.warning("[getNumber] operator='%s' не найден в country_id=%s", operator_name, country_id)
            return Response("NO_NUMBERS", media_type="text/plain")
        operator_id = op[0]

    # 3) окно "сегодня" (UTC)
    start, end = _today_window_utc()

    # 4) Глобальный лимит сервиса по стране (Service.daily_limit > 0) — сначала быстрый Redis
    if db_service.daily_limit and db_service.daily_limit > 0:
        used_global: Optional[int] = None
        if redis_client:
            used_global, _dummy = _read_used_from_cache(db_service.id, country_id, [])
            if used_global is None:
                # прогреем и попробуем ещё раз
                _prime_provider_counters(db, db_service.id, country_id)
                used_global, _dummy = _read_used_from_cache(db_service.id, country_id, [])
        # фолбэк на БД
        if used_global is None:
            used_global = _count_sms_service_country(db, db_service.id, country_id, start, end)

        if used_global >= db_service.daily_limit:
            log.warning("[getNumber] Глобальный лимит сервиса ДОСТИГНУТ: service=%s country=%s used=%s limit=%s",
                        db_service.id, country_id, used_global, db_service.daily_limit)
            return Response("NO_NUMBERS", media_type="text/plain")

    # 5) Список активных провайдеров
    active_provider_ids = [pid for (pid,) in db.query(models.Provider.id).filter(models.Provider.is_active.is_(True)).all()]
    if not active_provider_ids:
        log.warning("[getNumber] Нет активных провайдеров → NO_NUMBERS")
        return Response("NO_NUMBERS", media_type="text/plain")

    # 6) Лимиты SPC (Service-Provider-Country)
    svc_limits_rows = (
        db.query(models.ServiceLimit.provider_id, models.ServiceLimit.daily_limit)
          .filter(models.ServiceLimit.service_id == db_service.id,
                  models.ServiceLimit.country_id == country_id)
          .all()
    )
    spc_limits: Dict[int, int] = {pid: _as_infinite_limit(lim) for pid, lim in svc_limits_rows}
    for pid in active_provider_ids:
        if pid not in spc_limits:
            spc_limits[pid] = _as_infinite_limit(None)

    # 7) Текущее потребление SPC за сегодня (по факту SMS) — сначала из Redis
    used_by_provider: Dict[int, int] = {}

    # читаем из кэша
    used_global_cache, cached_map = _read_used_from_cache(db_service.id, country_id, active_provider_ids) if redis_client else (None, {pid: None for pid in active_provider_ids})

    # если кэш не полный — прогреем его С НУЛЯМИ для всех активных провайдеров
    if redis_client and (used_global_cache is None or not all(v is not None for v in cached_map.values())):
        _prime_provider_counters(db, db_service.id, country_id, provider_ids=active_provider_ids)
        used_global_cache, cached_map = _read_used_from_cache(db_service.id, country_id, active_provider_ids)

    if redis_client and all(v is not None for v in cached_map.values()):
        used_by_provider = {pid: int(cached_map[pid] or 0) for pid in active_provider_ids}

    # если кэша всё ещё нет/Redis упал — считаем через БД (как раньше)
    if not used_by_provider:
        used_by_provider = _count_sms_service_country_group_by_provider(
            db, db_service.id, country_id, start, end
        )

    providers_within_limit = [
        pid for pid in active_provider_ids
        if (used_by_provider.get(pid, 0) < spc_limits[pid])
    ]
    if not providers_within_limit:
        log.warning("[getNumber] Все провайдеры упёрлись в SPC-лимиты (по SMS) → NO_NUMBERS")
        return Response("NO_NUMBERS", media_type="text/plain")

    # 8) Общие фильтры по номерам
    base_filters_common = [
        models.PhoneNumber.country_id == country_id,
        models.PhoneNumber.is_active.is_(True),
        models.PhoneNumber.is_in_use.is_(False),
    ]
    if operator_id is not None:
        base_filters_common.append(models.PhoneNumber.operator_id == operator_id)

    # 9) Перемешиваем провайдеров и пробуем по случайной точке входа в диапазоне id
    random.shuffle(providers_within_limit)
    provider_cycle = providers_within_limit[:max(1, min(PROVIDER_SAMPLE_SIZE, len(providers_within_limit)))]

    attempts_left = CANDIDATE_ATTEMPTS
    hit_only_cooldown = False

    for provider_id in provider_cycle:
        if attempts_left <= 0:
            break

        bounds = _get_id_bounds_cached(db, country_id, provider_id, operator_id)
        if not bounds:
            continue
        min_id, max_id = bounds

        local_attempts = min(10, attempts_left)
        for _ in range(local_attempts):
            attempts_left -= 1
            rand_id = random.randint(min_id, max_id)

            cand = (
                db.query(models.PhoneNumber.id, models.PhoneNumber.number_str)
                  .filter(*base_filters_common, models.PhoneNumber.provider_id == provider_id,
                          models.PhoneNumber.id >= rand_id)
                  .order_by(models.PhoneNumber.id.asc())
                  .limit(1)
                  .first()
            )
            if not cand:
                cand = (
                    db.query(models.PhoneNumber.id, models.PhoneNumber.number_str)
                      .filter(*base_filters_common, models.PhoneNumber.provider_id == provider_id)
                      .order_by(models.PhoneNumber.id.asc())
                      .limit(1)
                      .first()
                )
                if not cand:
                    break

            in_cooldown = False
            if redis_client:
                try:
                    in_cooldown = bool(redis_client.get(f"cool:{db_service.id}:{cand.id}"))
                except Exception as re:
                    log.warning(f"[getNumber] Ошибка Redis при проверке cooldown: {re}")

            if in_cooldown:
                hit_only_cooldown = True
                continue

            locked = (
                db.query(models.PhoneNumber.id, models.PhoneNumber.number_str)
                  .filter(models.PhoneNumber.id == cand.id,
                          models.PhoneNumber.is_in_use.is_(False))
                  .with_for_update(skip_locked=True)
                  .first()
            )
            if not locked:
                continue

            db.execute(text("UPDATE phone_numbers SET is_in_use = TRUE WHERE id = :id")
                       .bindparams(id=locked.id))
            log.info(f"[getNumber] Выдан номер ID={locked.id} {locked.number_str} "
                     f"(svc={db_service.code}, c={country_id}, op={operator_name or 'any'}, p={provider_id})")
            return await _create_session_for_locked_id(
                db, locked.id, locked.number_str, db_service, api_key_obj
            )

    # 10) Один фоллбэк — игнорируем cooldown, если упёрлись только в него
    if hit_only_cooldown and ALLOW_COOLDOWN_FALLBACK:
        log.warning("[getNumber] FALLBACK_COOLDOWN: все кандидаты были в cooldown — игнорирую один раз.")
        random.shuffle(providers_within_limit)
        for provider_id in providers_within_limit:
            fb = (
                db.query(models.PhoneNumber.id, models.PhoneNumber.number_str)
                  .filter(*base_filters_common, models.PhoneNumber.provider_id == provider_id)
                  .order_by(models.PhoneNumber.id.asc())
                  .with_for_update(skip_locked=True)
                  .limit(1)
                  .first()
            )
            if fb:
                db.execute(text("UPDATE phone_numbers SET is_in_use = TRUE WHERE id = :id")
                           .bindparams(id=fb.id))
                return await _create_session_for_locked_id(
                    db, fb.id, fb.number_str, db_service, api_key_obj
                )

    log.error(f"[getNumber] NO_NUMBERS: svc={db_service.code}, c={country_id}, op={operator_name or 'any'} "
              f"(providers_within_limit={len(providers_within_limit)}, attempts_used={CANDIDATE_ATTEMPTS - attempts_left})")
    return Response("NO_NUMBERS", media_type="text/plain")

# =========================
#  Повторная активация
# =========================
async def get_repeat_number(db: Session, api_key_obj: models.ApiKey,
                            service_code: str, number_str: str):
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

    start, end = _today_window_utc()

    # Быстрый редис-гейт (если включён Redis): общий лимит и SPC
    if redis_client:
        today = _today_str()
        # общий лимит сервиса по стране
        if target_service.daily_limit and target_service.daily_limit > 0:
            gkey = f"limit_service:{today}:{target_service.id}:{num_obj.country_id}"
            gval = _cache_get(gkey)
            if gval is None:
                active_pids = [pid for (pid,) in db.query(models.Provider.id).filter(models.Provider.is_active.is_(True)).all()]
                _prime_provider_counters(db, target_service.id, num_obj.country_id, provider_ids=active_pids)
                gval = _cache_get(gkey)
            if gval is not None and gval >= target_service.daily_limit:
                return Response("NO_NUMBERS", media_type="text/plain")

        # SPC лимит (если задан)
        specific_limit_rule = (
            db.query(models.ServiceLimit)
            .filter(models.ServiceLimit.service_id == target_service.id,
                    models.ServiceLimit.provider_id == num_obj.provider_id,
                    models.ServiceLimit.country_id == num_obj.country_id)
            .first()
        )
        if specific_limit_rule and specific_limit_rule.daily_limit and specific_limit_rule.daily_limit > 0:
            pkey = f"limit:{today}:{target_service.id}:{num_obj.country_id}:{num_obj.provider_id}"
            pval = _cache_get(pkey)
            if pval is None:
                active_pids = [pid for (pid,) in db.query(models.Provider.id).filter(models.Provider.is_active.is_(True)).all()]
                _prime_provider_counters(db, target_service.id, num_obj.country_id, provider_ids=active_pids)
                pval = _cache_get(pkey)
            if pval is not None and pval >= specific_limit_rule.daily_limit:
                return Response("NO_NUMBERS", media_type="text/plain")
    else:
        # без Redis — прямые подсчёты
        specific_limit_rule = (
            db.query(models.ServiceLimit)
            .filter(models.ServiceLimit.service_id == target_service.id,
                    models.ServiceLimit.provider_id == num_obj.provider_id,
                    models.ServiceLimit.country_id == num_obj.country_id)
            .first()
        )
        if specific_limit_rule and specific_limit_rule.daily_limit and specific_limit_rule.daily_limit > 0:
            spc_sms_count = (
                db.query(func.count(models.SmsMessage.id))
                  .join(models.Session, models.Session.id == models.SmsMessage.session_id)
                  .join(models.PhoneNumber, models.PhoneNumber.id == models.Session.phone_number_id)
                  .filter(models.Session.service_id == target_service.id,
                          models.PhoneNumber.provider_id == num_obj.provider_id,
                          models.PhoneNumber.country_id == num_obj.country_id,
                          models.SmsMessage.received_at >= start,
                          models.SmsMessage.received_at < end)
                  .scalar()
            ) or 0
            if spc_sms_count >= specific_limit_rule.daily_limit:
                log.warning("ЛИМИТ (getRepeatNumber): service=%s, provider=%s, country=%s достигнут (%s).",
                            target_service.id, num_obj.provider_id, num_obj.country_id, specific_limit_rule.daily_limit)
                return Response("NO_NUMBERS", media_type="text/plain")

        if target_service.daily_limit and target_service.daily_limit > 0:
            service_sms_count = _count_sms_service_country(db, target_service.id, num_obj.country_id, start, end)
            if service_sms_count >= target_service.daily_limit:
                log.warning("ЛИМИТ СЕРВИСА (getRepeatNumber): '%s' (%s) достигнут по стране %s.",
                            target_service.name, target_service.daily_limit, num_obj.country_id)
                return Response("NO_NUMBERS", media_type="text/plain")

    num_obj.is_in_use = True
    db.commit()
    return await _create_session_for_number(db, num_obj, target_service, api_key_obj)

app.include_router(api_router)

def _parse_bool(s: Optional[str]) -> bool:
    return str(s).lower() in {"1", "true", "yes", "y", "on"}

@app.get("/orphan/export.csv", name="orphan_export_csv")
def orphan_export_csv(
    request: Request,
    provider_id: Optional[int] = None,
    sender: Optional[str] = None,
    country_id: Optional[int] = None,
    operator_id: Optional[int] = None,
    null_only: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Экспорт «осиротевших» номеров (для аналитики) с фильтрами и периодом.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    start_dt, end_dt = None, None
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
        start_dt = end_dt - datetime.timedelta(days=7)

    q = (
        db.query(
            models.OrphanSms.phone_number_str.label("phone_number"),
            func.count(models.OrphanSms.id).label("sms_count"),
            func.min(models.OrphanSms.received_at).label("first_seen"),
            func.max(models.OrphanSms.received_at).label("last_seen"),
        )
        .filter(models.OrphanSms.received_at >= start_dt,
                models.OrphanSms.received_at < end_dt)
    )

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

    q = q.group_by(models.OrphanSms.phone_number_str).order_by(func.count(models.OrphanSms.id).desc())

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

    fname_parts = []
    if provider_id is not None:
        fname_parts.append(f"p{provider_id}")
    if sender:
        fname_parts.append(sender)
    if country_id is not None:
        fname_parts.append(f"c{country_id}")
    if operator_id is not None:
        fname_parts.append(f"o{operator_id}")
    if _parse_bool(null_only):
        fname_parts.append("only-null-provider")
    period = f"{start_dt.date()}_{end_dt.date()}"
    fname = f"orphan_numbers_{'-'.join(fname_parts) or 'all'}_{period}.csv"
    headers = {"Content-Disposition": f"attachment; filename*=UTF-8''{quote(fname)}"}
    return StreamingResponse(iter([buf.getvalue()]),
                             media_type="text/csv; charset=utf-8", headers=headers)