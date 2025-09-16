# src/tools.py
# -*- coding: utf-8 -*-
import datetime
import random
import string
import io
import csv
import logging
import time
from datetime import timedelta
from typing import Optional, List, Dict, Any
from functools import lru_cache

from fastapi import Request, UploadFile, Depends, Form, APIRouter, BackgroundTasks
from starlette.responses import RedirectResponse, StreamingResponse
from starlette.responses import JSONResponse
from starlette.templating import Jinja2Templates

from sqlalchemy import func, text
from sqlalchemy.orm import Session, selectinload

from .database import SessionLocal
from . import models
from .utils import normalize_phone_number
from . import main as main_app
from .deps import get_redis

# --- Redis JSON cache helpers (используем общий redis_client из deps) ---
import json

redis_client = get_redis()

def _cache_get_json(key: str):
    if not redis_client:
        return None
    try:
        v = redis_client.get(key)
        return json.loads(v) if v else None
    except Exception:
        return None

def _cache_set_json(key: str, data, ttl: int):
    if not redis_client:
        return
    try:
        redis_client.setex(key, ttl, json.dumps(data, default=str))
    except Exception:
        pass


log = logging.getLogger(__name__)

templates = Jinja2Templates(directory="templates")
router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# вспомогательный генератор случайного порядка (для NOT NULL sort_order)
def _make_sort_order() -> int:
    return random.randint(1, 2_147_483_646)

# --- LRU Cache справочников ---
@lru_cache(maxsize=1)
def get_cached_providers() -> List[models.Provider]:
    db = SessionLocal()
    try:
        return db.query(models.Provider).order_by(models.Provider.name).all()
    finally:
        db.close()

@lru_cache(maxsize=1)
def get_cached_countries() -> List[models.Country]:
    db = SessionLocal()
    try:
        return db.query(models.Country).order_by(models.Country.name).all()
    finally:
        db.close()

@lru_cache(maxsize=1)
def get_cached_operators() -> List[models.Operator]:
    # нужно, чтобы в шаблоне безопасно дергать op.country.name/op.provider.name
    db = SessionLocal()
    try:
        return (
            db.query(models.Operator)
              .options(
                  selectinload(models.Operator.country),
                  selectinload(models.Operator.provider),
              )
              .order_by(models.Operator.name)
              .all()
        )
    finally:
        db.close()

@lru_cache(maxsize=1)
def get_cached_services() -> List[models.Service]:
    db = SessionLocal()
    try:
        return db.query(models.Service).order_by(models.Service.name).all()
    finally:
        db.close()

# --------------------------
# Главная страница инструментов (оптимизировано)
# --------------------------
# Поместите этот код вместо существующей функции get_tools_page в src/tools.py
@router.get("/tools", tags=["Tools"], summary="Страница со всеми инструментами")
def get_tools_page(
    request: Request,
    db: Session = Depends(get_db),
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    api_key_str: Optional[str] = None
):
    t_start = time.time()

    # --- Период (UTC) ---
    try:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc) if start_date_str else \
            (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=datetime.timezone.utc) if end_date_str else \
            datetime.datetime.now(datetime.timezone.utc)
    except (ValueError, TypeError):
        start_date = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime.datetime.now(datetime.timezone.utc)

    # --- Справочники (из LRU-кэша) ---
    providers = get_cached_providers()
    countries = get_cached_countries()
    operators = get_cached_operators()
    all_services = get_cached_services()

    # Мапы id->name для быстрого отображения без JOIN-ов
    prov_map = {p.id: p.name for p in providers}
    country_map = {c.id: c.name for c in countries}
    oper_map = {o.id: o.name for o in operators}

    # --- Query-параметры ---
    q = request.query_params
    stat_group_by = (q.get("stat_group_by") or "service").strip()
    stat_provider_id = int(q.get("stat_provider_id")) if (q.get("stat_provider_id") or "").isdigit() else None
    stat_country_id  = int(q.get("stat_country_id"))  if (q.get("stat_country_id")  or "").isdigit() else None
    stat_service_id  = int(q.get("stat_service_id"))  if (q.get("stat_service_id")  or "").isdigit() else None

    ot_search_sender = (q.get("ot_search_sender") or "").strip()
    ot_provider_id = int(q.get("ot_provider_id")) if (q.get("ot_provider_id") or "").isdigit() else None
    ot_country_id  = int(q.get("ot_country_id"))  if (q.get("ot_country_id")  or "").isdigit() else None
    ot_operator_id = int(q.get("ot_operator_id")) if (q.get("ot_operator_id") or "").isdigit() else None
    ot_page = max(1, int(q.get("ot_page") or 1))
    per_page = 20

    # --- Фильтры для статистики ---
    stat_params = {"start_date": start_date, "end_date": end_date}
    stat_filters_sql = []
    need_join_pn = False

    if stat_service_id:
        stat_filters_sql.append("s.service_id = :stat_service_id")
        stat_params["stat_service_id"] = stat_service_id
    if stat_provider_id:
        stat_filters_sql.append("pn.provider_id = :stat_provider_id")
        stat_params["stat_provider_id"] = stat_provider_id
        need_join_pn = True
    if stat_country_id:
        stat_filters_sql.append("pn.country_id = :stat_country_id")
        stat_params["stat_country_id"] = stat_country_id
        need_join_pn = True

    where_clause = (" AND " + " AND ".join(stat_filters_sql)) if stat_filters_sql else ""
    join_pn_sql = "JOIN phone_numbers pn ON s.phone_number_id = pn.id" if need_join_pn or stat_group_by in ("provider", "country") else ""

    # --- ИТОГИ (кэш 300с) ---
    sum_key = f"tools:sum:{start_date.isoformat()}:{end_date.isoformat()}:{stat_service_id}:{stat_provider_id}:{stat_country_id}"
    totals = _cache_get_json(sum_key)
    if totals is None:
        summary_sql = text(f"""
            SELECT
                COUNT(m.id) AS total_sms,
                COUNT(DISTINCT s.phone_number_id) AS unique_numbers,
                COUNT(DISTINCT s.service_id) AS unique_services
            FROM sms_messages m
            JOIN sessions s ON m.session_id = s.id
            {join_pn_sql}
            WHERE m.received_at BETWEEN :start_date AND :end_date {where_clause}
        """)
        row = db.execute(summary_sql, stat_params).fetchone()
        totals = {
            "total_sms": int(row.total_sms or 0),
            "unique_numbers": int(row.unique_numbers or 0),
            "unique_services": int(row.unique_services or 0),
        }
        _cache_set_json(sum_key, totals, ttl=300)

    total_sms = totals["total_sms"]
    unique_numbers = totals["unique_numbers"]
    unique_services = totals["unique_services"]
    avg_sms_per_number = round((total_sms / unique_numbers), 2) if unique_numbers else 0.0

    # --- Счетчики заняты/свободны (кэш 300с) ---
    pn_key = "tools:pn_counts"
    pn_counts = _cache_get_json(pn_key)
    if pn_counts is None:
        row = db.execute(text("""
            SELECT
               (SELECT COUNT(id) FROM phone_numbers WHERE is_in_use IS TRUE) AS busy_cnt,
               (SELECT COUNT(id) FROM phone_numbers WHERE is_in_use IS FALSE AND is_active IS TRUE) AS free_active_cnt
        """)).first()
        pn_counts = {"busy": int(row.busy_cnt), "free_active": int(row.free_active_cnt)}
        _cache_set_json(pn_key, pn_counts, ttl=300)
    numbers_in_use_now = pn_counts["busy"]
    numbers_free_now = pn_counts["free_active"]

    # --- Таймсерия по дням (кэш 300с) ---
    byday_key = f"tools:byday:{start_date.date()}:{end_date.date()}:{stat_service_id}:{stat_provider_id}:{stat_country_id}"
    byday = _cache_get_json(byday_key)
    if byday is None:
        chart_sql = text(f"""
            SELECT date_trunc('day', m.received_at)::date AS day, COUNT(m.id) AS sms_count
            FROM sms_messages m
            JOIN sessions s ON m.session_id = s.id
            {join_pn_sql}
            WHERE m.received_at BETWEEN :start_date AND :end_date {where_clause}
            GROUP BY 1
            ORDER BY 1
        """)
        rows = db.execute(chart_sql, stat_params).fetchall()
        byday = {r.day.isoformat(): int(r.sms_count) for r in rows}
        _cache_set_json(byday_key, byday, ttl=300)

    day_labels, day_values = [], []
    cur = start_date.date()
    while cur <= end_date.date():
        k = cur.isoformat()
        day_labels.append(k)
        day_values.append(byday.get(k, 0))
        cur += timedelta(days=1)
    chart_data = {"labels": day_labels, "data": day_values}

    # --- Детализация (group by) (кэш 300с) ---
    gb = (stat_group_by or "service").lower()
    group_by_map = {
        "provider": ("p.name", "pn.provider_id", "providers p", "p.id = pn.provider_id"),
        "country":  ("c.name", "pn.country_id",  "countries c", "c.id = pn.country_id"),
        "date":     ("(m.received_at::date)::text", "m.received_at::date", None, None),
        "service":  ("svc.name", "s.service_id", "services svc", "svc.id = s.service_id"),
    }
    gb_name, gb_col, gb_join_table, gb_join_on = group_by_map.get(gb, group_by_map["service"])
    join_sql = f"JOIN {gb_join_table} ON {gb_join_on}" if gb_join_table else ""

    details_key = f"tools:details:{gb}:{start_date.isoformat()}:{end_date.isoformat()}:{stat_service_id}:{stat_provider_id}:{stat_country_id}"
    details_table = _cache_get_json(details_key)
    if details_table is None:
        details_sql = text(f"""
            SELECT {gb_name} AS name,
                   COUNT(m.id) AS sms_count,
                   COUNT(DISTINCT s.phone_number_id) AS unique_numbers
            FROM sms_messages m
            JOIN sessions s ON m.session_id = s.id
            {'JOIN phone_numbers pn ON s.phone_number_id = pn.id' if (need_join_pn or gb in ("provider","country")) else ""}
            {join_sql}
            WHERE m.received_at BETWEEN :start_date AND :end_date {where_clause}
            GROUP BY {gb_name}
            ORDER BY sms_count DESC
            LIMIT 100
        """)
        rows = db.execute(details_sql, stat_params).fetchall()
        details_table = [{"name": r.name, "sms_count": int(r.sms_count), "unique_numbers": int(r.unique_numbers)} for r in rows]
        _cache_set_json(details_key, details_table, ttl=300)

    # --------------------------
    # «Осиротевший» трафик — ДВУХФАЗНЫЙ top-N
    # --------------------------
    # Фильтры
    ot_params: Dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    where_parts = ["o.received_at BETWEEN :start_date AND :end_date"]
    if ot_search_sender:
        where_parts.append("o.source_addr ILIKE :sender")
        ot_params["sender"] = f"%{ot_search_sender}%"
    if ot_provider_id is not None:
        where_parts.append("o.provider_id = :ot_provider_id")
        ot_params["ot_provider_id"] = ot_provider_id
    if ot_country_id is not None:
        where_parts.append("o.country_id = :ot_country_id")
        ot_params["ot_country_id"] = ot_country_id
    if ot_operator_id is not None:
        where_parts.append("o.operator_id = :ot_operator_id")
        ot_params["ot_operator_id"] = ot_operator_id
    ot_where_clause = " AND ".join(where_parts)

    # COUNT(*) групп (делаем только на 1-й странице; кэш 300с)
    total_rows: int
    total_pages: int
    count_key = f"tools:orph:cnt:{start_date.date()}:{end_date.date()}:{ot_provider_id}:{ot_country_id}:{ot_operator_id}:{ot_search_sender}"
    if ot_page == 1:
        total_rows = _cache_get_json(count_key)
        if total_rows is None:
            count_sql = text(f"""
                SELECT COUNT(*) FROM (
                  SELECT 1
                  FROM orphan_sms o
                  WHERE {ot_where_clause}
                  GROUP BY o.provider_id, o.country_id, o.operator_id, o.source_addr
                ) t
            """)
            total_rows = int(db.execute(count_sql, ot_params).scalar() or 0)
            _cache_set_json(count_key, total_rows, ttl=300)
        total_pages = max(1, (total_rows + per_page - 1) // per_page)
    else:
        total_rows = -1
        total_pages = ot_page

    # Фаза 1: только топ-группы по COUNT(*)
    phase1_limit = per_page + (1 if ot_page > 1 else 0)
    phase1_offset = (ot_page - 1) * per_page

    orph_phase1_key = f"tools:orph:phase1:{start_date.date()}:{end_date.date()}:{ot_provider_id}:{ot_country_id}:{ot_operator_id}:{ot_search_sender}:{ot_page}:{per_page}"
    phase1_rows = _cache_get_json(orph_phase1_key)
    has_next = None
    if phase1_rows is None:
        try:
            db.execute(text("SET LOCAL work_mem = '256MB'"))
        except Exception:
            pass

        phase1_sql = text(f"""
            SELECT
              o.provider_id  AS provider_id,
              o.source_addr  AS source_addr,
              o.country_id   AS country_id,
              o.operator_id  AS operator_id,
              COUNT(*)       AS sms_count
            FROM orphan_sms o
            WHERE {ot_where_clause}
            GROUP BY 1,2,3,4
            ORDER BY sms_count DESC
            LIMIT :limit OFFSET :offset
        """)
        phase1_rows_raw = db.execute(phase1_sql, {**ot_params, "limit": phase1_limit, "offset": phase1_offset}).fetchall()
        has_next = (ot_page > 1 and len(phase1_rows_raw) == phase1_limit)
        phase1_rows = [{
            "provider_id": r.provider_id,
            "country_id": r.country_id,
            "operator_id": r.operator_id,
            "source_addr": r.source_addr,
            "sms_count": int(r.sms_count),
        } for r in (phase1_rows_raw[:per_page] if ot_page > 1 else phase1_rows_raw)]
        _cache_set_json(orph_phase1_key, phase1_rows, ttl=300)

    else:
        has_next = None

    # Фаза 2: для выбранных групп — через CTE VALUES + JOIN (быстрее, чем длинный OR)
    if phase1_rows:
        # Конструируем VALUES (:p0,:c0,:o0,:s0), ...
        vals_parts = []
        params2: Dict[str, Any] = {"start_date": start_date, "end_date": end_date}
        base_parts = ["o.received_at BETWEEN :start_date AND :end_date"]
        if ot_search_sender:
            base_parts.append("o.source_addr ILIKE :sender")
            params2["sender"] = f"%{ot_search_sender}%"
        if ot_provider_id is not None:
            base_parts.append("o.provider_id = :ot_provider_id")
            params2["ot_provider_id"] = ot_provider_id
        if ot_country_id is not None:
            base_parts.append("o.country_id = :ot_country_id")
            params2["ot_country_id"] = ot_country_id
        if ot_operator_id is not None:
            base_parts.append("o.operator_id = :ot_operator_id")
            params2["ot_operator_id"] = ot_operator_id

        for i, g in enumerate(phase1_rows):
            params2[f"p{i}"] = g["provider_id"]
            params2[f"c{i}"] = g["country_id"]
            params2[f"o{i}"] = g["operator_id"]
            params2[f"s{i}"] = g["source_addr"]
            vals_parts.append(f"(:p{i}, :c{i}, :o{i}, :s{i})")

        keys_values_sql = ", ".join(vals_parts)
        base_where_sql = " AND ".join(base_parts)

        # DISTINCT считаем по join-результату (индексы отрабатывают лучше)
        phase2_sql = text(f"""
            WITH keys(provider_id, country_id, operator_id, source_addr) AS (
                VALUES {keys_values_sql}
            )
            SELECT
              k.provider_id,
              k.source_addr,
              k.country_id,
              k.operator_id,
              COUNT(DISTINCT o.phone_number_str) AS unique_numbers_count,
              MIN(o.text) AS sample_text
            FROM keys k
            JOIN orphan_sms o
              ON o.provider_id  IS NOT DISTINCT FROM k.provider_id
             AND o.country_id   IS NOT DISTINCT FROM k.country_id
             AND o.operator_id  IS NOT DISTINCT FROM k.operator_id::integer -- <<< ИСПРАВЛЕНИЕ ЗДЕСЬ
             AND o.source_addr  =  k.source_addr
            WHERE {base_where_sql}
            GROUP BY 1,2,3,4
        """)
        try:
            db.execute(text("SET LOCAL work_mem = '256MB'"))
        except Exception:
            pass

        phase2_rows = db.execute(phase2_sql, params2).fetchall()

        det_map: Dict[tuple, Dict[str, Any]] = {}
        for r in phase2_rows:
            det_map[(r.provider_id, r.country_id, r.operator_id, r.source_addr)] = {
                "unique_numbers_count": int(r.unique_numbers_count),
                "sample_text": r.sample_text,
            }

        orphan_rows = []
        for g in phase1_rows:
            key = (g["provider_id"], g["country_id"], g["operator_id"], g["source_addr"])
            extra = det_map.get(key, {"unique_numbers_count": 0, "sample_text": None})
            orphan_rows.append({
                "provider_id": g["provider_id"],
                "provider_name": prov_map.get(g["provider_id"], "—"),
                "source_addr": g["source_addr"],
                "country_id": g["country_id"],
                "country_name": country_map.get(g["country_id"], "—"),
                "operator_id": g["operator_id"],
                "operator_name": oper_map.get(g["operator_id"], "—"),
                "sms_count": g["sms_count"],
                "unique_numbers_count": extra["unique_numbers_count"],
                "sample_text": extra["sample_text"],
            })
    else:
        orphan_rows = []

    # Пагинация для страниц >1 без COUNT
    if ot_page == 1:
        ot_pagination = {"current_page": ot_page, "total_pages": total_pages, "total_rows": total_rows}
    else:
        if has_next is None:
            has_next = len(phase1_rows) == per_page
        inferred_total_pages = ot_page + 1 if has_next else ot_page
        ot_pagination = {"current_page": ot_page, "total_pages": inferred_total_pages, "total_rows": -1}

    # --- API статистика по ключу (кэш 300с) ---
    api_stats_obj, error_api_stats, selected_key = None, None, None
    if api_key_str:
        selected_key = db.query(models.ApiKey).filter(models.ApiKey.key == api_key_str.strip()).first()
        if not selected_key:
            error_api_stats = "API ключ не найден."
        else:
            api_cache_key = f"tools:api:{selected_key.id}:{start_date.date()}:{end_date.date()}"
            api_cached = _cache_get_json(api_cache_key)
            if api_cached is None:
                rows = db.execute(text("""
                    SELECT svc.name, COUNT(m.id) AS sms_count
                    FROM sms_messages m
                    JOIN sessions s ON m.session_id = s.id
                    JOIN services svc ON s.service_id = svc.id
                    WHERE s.api_key_id = :api_key_id AND m.received_at BETWEEN :start_date AND :end_date
                    GROUP BY svc.name
                    ORDER BY sms_count DESC
                """), {"api_key_id": selected_key.id, "start_date": start_date, "end_date": end_date}).fetchall()
                total_api_sms = int(sum(r.sms_count for r in rows))
                api_cached = {"total_sms": total_api_sms,
                              "service_breakdown": [{"name": r.name, "sms_count": int(r.sms_count)} for r in rows]}
                _cache_set_json(api_cache_key, api_cached, ttl=300)
            api_stats_obj = api_cached

    # --- Контекст ---
    context = {
        "request": request,
        "dashboard_stats": {
            "total_sms": total_sms, "unique_numbers": unique_numbers, "unique_services": unique_services,
            "avg_sms_per_number": avg_sms_per_number, "numbers_in_use": numbers_in_use_now, "numbers_free": numbers_free_now,
        },
        "stat_filters": {
            "group_by": stat_group_by, "provider_id": stat_provider_id, "country_id": stat_country_id, "service_id": stat_service_id,
        },
        "ot_filters": {
            "search_sender": ot_search_sender, "provider_id": ot_provider_id, "country_id": ot_country_id, "operator_id": ot_operator_id,
        },
        "ot_pagination": ot_pagination,
        "providers": providers, "countries": countries, "operators": operators, "all_services": all_services,
        "chart_data": chart_data,
        "details_table": details_table,
        "orphan_traffic_data": orphan_rows,
        "start_date_str": start_date.strftime("%Y-%m-%d"),
        "end_date_str": end_date.strftime("%Y-%m-%d"),
        "api_stats": api_stats_obj,
        "error_api_stats": error_api_stats,
        "selected_key": selected_key,
        "selected_key_str": api_key_str,
    }
    log.info(f"Страница /tools сгенерирована за {time.time() - t_start:.2f} сек.")
    return templates.TemplateResponse("tools.html", context)

# --------------------------
# Детализация «осиротевших» по списку номеров (с датами/временем) — FIX
# --------------------------
@router.get("/tools/orphan-numbers-detail", tags=["Tools"])
def get_orphan_numbers_detail(
    request: Request,
    source_addr: str,
    provider_id: Optional[str] = None,
    country_id: Optional[str] = None,
    operator_id: Optional[str] = None,
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    db: Session = Depends(get_db),
    format: Optional[str] = None,
):
    # --- разбор периода (UTC) ---
    def _parse_dt(s: Optional[str], is_start: bool) -> Optional[datetime.datetime]:
        if not s:
            return None
        try:
            dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(datetime.timezone.utc)
        except ValueError:
            try:
                dt = datetime.datetime.strptime(s, "%Y-%m-%d")
                return dt.replace(
                    hour=0 if is_start else 23,
                    minute=0 if is_start else 59,
                    second=0 if is_start else 59,
                    microsecond=0,
                    tzinfo=datetime.timezone.utc,
                )
            except ValueError:
                return None

    now = datetime.datetime.now(datetime.timezone.utc)
    start_dt = _parse_dt(start_date_str, True) or now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = _parse_dt(end_date_str, False) or now

    pid = int(provider_id) if provider_id and provider_id.isdigit() else None
    cid = int(country_id)  if country_id  and country_id.isdigit()  else None
    oid = int(operator_id) if operator_id and operator_id.isdigit() else None

    # --- ДИНАМИЧЕСКОЕ построение WHERE (не передаём NULL в "=") ---
    where_parts = [
        "o.source_addr = :source_addr",
        "o.received_at BETWEEN :start_dt AND :end_dt",
    ]
    params = {
        "source_addr": source_addr,
        "start_dt": start_dt,
        "end_dt": end_dt,
    }
    if pid is not None:
        where_parts.append("o.provider_id = :provider_id")
        params["provider_id"] = pid
    if cid is not None:
        where_parts.append("o.country_id = :country_id")
        params["country_id"] = cid
    if oid is not None:
        where_parts.append("o.operator_id = :operator_id")
        params["operator_id"] = oid

    sql_numbers = text(f"""
        SELECT DISTINCT o.phone_number_str
        FROM orphan_sms o
        WHERE {' AND '.join(where_parts)}
        ORDER BY o.phone_number_str
    """)

    numbers = [row[0] for row in db.execute(sql_numbers, params).fetchall()]

    res_names = db.execute(text(
        "SELECT (SELECT name FROM providers WHERE id = :p), "
        "       (SELECT name FROM countries WHERE id = :c), "
        "       (SELECT name FROM operators WHERE id = :o)"
    ), {"p": pid, "c": cid, "o": oid}).fetchone()

    if format == "csv":
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow(["phone_number"])
        for number in numbers:
            writer.writerow([number])
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        fn = f'numbers_{source_addr}_{start_dt.strftime("%Y%m%d")}_{end_dt.strftime("%Y%m%d")}.csv'
        response.headers["Content-Disposition"] = f'attachment; filename="{fn}"'
        return response

    context = {
        "request": request, "numbers": numbers,
        "provider_name": (res_names[0] if res_names else None) or "—",
        "country_name": (res_names[1] if res_names else None) or "—",
        "operator_name": (res_names[2] if res_names else None) or "—",
        "source_addr": source_addr, "start_date_str": start_dt.isoformat(), "end_date_str": end_dt.isoformat(),
        "provider_id": pid, "country_id": cid, "operator_id": oid,
    }
    return templates.TemplateResponse("orphan_numbers_detail_tool.html", context)

# --------------------------
# Экспорт текущего среза «осиротевших» групп (CSV)
# --------------------------
@router.get("/tools/orphan/export", tags=["Tools"])
def export_orphan_groups_csv(
    request: Request,
    db: Session = Depends(get_db),
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    ot_search_sender: Optional[str] = None,
    ot_provider_id: Optional[str] = None,
    ot_country_id: Optional[str] = None,
    ot_operator_id: Optional[str] = None,
):
    try:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc) if start_date_str else \
            (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=datetime.timezone.utc) if end_date_str else \
            datetime.datetime.now(datetime.timezone.utc)
    except (ValueError, TypeError):
        start_date = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime.datetime.now(datetime.timezone.utc)

    ot_params: Dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    where_parts = ["o.received_at BETWEEN :start_date AND :end_date"]
    if ot_search_sender:
        where_parts.append("o.source_addr ILIKE :sender")
        ot_params["sender"] = f"%{ot_search_sender}%"
    if ot_provider_id and ot_provider_id.isdigit():
        where_parts.append("o.provider_id = :ot_provider_id")
        ot_params["ot_provider_id"] = int(ot_provider_id)
    if ot_country_id and ot_country_id.isdigit():
        where_parts.append("o.country_id = :ot_country_id")
        ot_params["ot_country_id"] = int(ot_country_id)
    if ot_operator_id and ot_operator_id.isdigit():
        where_parts.append("o.operator_id = :ot_operator_id")
        ot_params["ot_operator_id"] = int(ot_operator_id)
    where_sql = " AND ".join(where_parts)

    # Экспорт без JOIN: имена восстановим через кэш
    base_sql = text(f"""
        SELECT
            o.provider_id  AS provider_id,
            o.source_addr  AS source_addr,
            o.country_id   AS country_id,
            o.operator_id  AS operator_id,
            MIN(o.text)                        AS sample_text,
            COUNT(o.id)                        AS sms_count,
            COUNT(DISTINCT o.phone_number_str) AS unique_numbers_count
        FROM orphan_sms o
        WHERE {where_sql}
        GROUP BY 1,2,3,4
        ORDER BY sms_count DESC
    """)

    def _gen():
        prov_map = {p.id: p.name for p in get_cached_providers()}
        country_map = {c.id: c.name for c in get_cached_countries()}
        oper_map = {o.id: o.name for o in get_cached_operators()}
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["provider", "sender", "country", "operator", "sms_count", "unique_numbers", "sample_text"])
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)
        # убрали .yield_per(1000) — это Core-результат
        for row in db.execute(base_sql, ot_params):
            writer.writerow([
                prov_map.get(row.provider_id, "—"),
                row.source_addr,
                country_map.get(row.country_id, "—"),
                oper_map.get(row.operator_id, "—"),
                int(row.sms_count), int(row.unique_numbers_count),
                (row.sample_text or "")[:200].replace("\n", " "),
            ])
            yield buf.getvalue()
            buf.seek(0); buf.truncate(0)

    filename = f'orph_groups_{start_date.strftime("%Y%m%d")}_{end_date.strftime("%Y%m%d")}.csv'
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(_gen(), media_type="text/csv", headers=headers)

# --------------------------
# Массовая установка лимитов
# --------------------------
@router.post("/tools/bulk-set-limits", tags=["Tools"])
async def handle_bulk_limits(
    service_ids: List[int] = Form(...),
    provider_id: int = Form(...),
    country_id: int = Form(...),
    daily_limit: int = Form(...),
    db: Session = Depends(get_db),
):
    try:
        updated_count, created_count = 0, 0
        for service_id in service_ids:
            limit = db.query(models.ServiceLimit).filter_by(
                service_id=service_id, provider_id=provider_id, country_id=country_id
            ).first()
            if limit:
                limit.daily_limit = daily_limit
                updated_count += 1
            else:
                db.add(models.ServiceLimit(
                    service_id=service_id, provider_id=provider_id, country_id=country_id, daily_limit=daily_limit
                ))
                created_count += 1
        db.commit()
        return RedirectResponse(
            url=f"/tools?success=Успешно! Обновлено: {updated_count}, создано: {created_count}.&tab=bulk-limits-pane",
            status_code=303
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=bulk-limits-pane",
            status_code=303
        )

# --------------------------
# Импорт номеров из файла
# --------------------------
@router.post("/importer", tags=["Tools"])
async def handle_file_upload(
    file: UploadFile = Form(...),
    provider_id: int = Form(...),
    country_id: int = Form(...),
    operator_id: Optional[str] = Form(""),
    db: Session = Depends(get_db),
):
    op_id = int(operator_id) if operator_id and operator_id.isdigit() else None
    content = await file.read()
    lines = content.decode("utf-8", errors="ignore").splitlines()
    candidate_numbers = {n for n in (normalize_phone_number(line.strip()) for line in lines) if n}
    invalid_count = len(lines) - len(candidate_numbers)
    added_count, skipped_count = 0, 0
    candidate_list = list(candidate_numbers)
    batch_size = 5000
    try:
        for i in range(0, len(candidate_list), batch_size):
            batch = candidate_list[i : i + batch_size]
            existing_in_batch = {
                n[0] for n in db.query(models.PhoneNumber.number_str)
                                .filter(models.PhoneNumber.number_str.in_(batch))
            }
            skipped_count += len(existing_in_batch)
            new_numbers_to_add = [{
                "number_str": num, "provider_id": provider_id, "country_id": country_id,
                "operator_id": op_id, "is_active": True, "is_in_use": False,
                "sort_order": _make_sort_order(),  # добавили заполнение NOT NULL поля
            } for num in batch if num not in existing_in_batch]
            if new_numbers_to_add:
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                added_count += len(new_numbers_to_add)
        db.commit()
        # мягко обновим только быстрый счётчик, чтобы /tools показывал верно
        if redis_client:
            try:
                redis_client.delete("tools:pn_counts")
            except Exception:
                pass
        return RedirectResponse(
            url=f"/tools?success=Добавлено {added_count}. Дублей: {skipped_count}, невалидных: {invalid_count}.&tab=importer-pane",
            status_code=303
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=importer-pane",
            status_code=303
        )

# --------------------------
# Генератор диапазонов
# --------------------------
@router.post("/generator", tags=["Tools"])
def handle_range_generation(
    masks: str = Form(..., alias="masks"),
    quantity: int = Form(..., alias="quantity"),
    provider_id: int = Form(...),
    country_id: int = Form(...),
    operator_id: Optional[str] = Form(""),
    db: Session = Depends(get_db),
):
    op_id = int(operator_id) if operator_id and operator_id.isdigit() else None
    masks_list = [m.strip() for m in masks.strip().splitlines() if m.strip()]
    if not masks_list:
        return RedirectResponse(url="/tools?error=Не введено ни одной маски.&tab=generator-pane", status_code=303)

    total_generated_count, total_skipped_count = 0, 0
    try:
        for mask in masks_list:
            lower = mask.lower()
            if "x" not in lower:
                continue

            prefix = lower.split("x")[0]
            num_x = lower.count("x")
            if num_x <= 0:
                continue

            # --- генерируем ровно quantity уникальных хвостов (или все возможные, если quantity >= 10**num_x)
            space = 10 ** num_x
            if quantity >= space:
                # все возможные комбинации, перемешанные
                tails = [f"{i:0{num_x}d}" for i in range(space)]
                random.shuffle(tails)
                candidate_list = [f"{prefix}{t}" for t in tails]
            else:
                # выбор без повторов из пространства
                tails_int = random.sample(range(space), k=quantity)
                candidate_list = [f"{prefix}{i:0{num_x}d}" for i in tails_int]

            # нормализуем кандидатов в единый формат (как в импорте) — корректная дедупликация
            candidate_list = [n for n in (normalize_phone_number(x) for x in candidate_list) if n]

            if not candidate_list:
                continue

            existing_numbers = {
                n[0] for n in db.query(models.PhoneNumber.number_str)
                                 .filter(models.PhoneNumber.number_str.in_(candidate_list))
            }

            new_numbers_to_add = [{
                "number_str": num,
                "provider_id": provider_id,
                "country_id": country_id,
                "operator_id": op_id,
                "is_active": True,
                "is_in_use": False,
                "sort_order": _make_sort_order(),  # заполняем NOT NULL
            } for num in candidate_list if num not in existing_numbers]

            if new_numbers_to_add:
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                total_generated_count += len(new_numbers_to_add)

            total_skipped_count += (len(candidate_list) - len(new_numbers_to_add))

        db.commit()

        # мягко обновим быстрый кэш счётчиков
        if redis_client:
            try:
                redis_client.delete("tools:pn_counts")
            except Exception:
                pass

        return RedirectResponse(
            url=f"/tools?success=Сгенерировано {total_generated_count} номеров. Дублей: {total_skipped_count}.&tab=generator-pane",
            status_code=303
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=generator-pane",
            status_code=303
        )


# --------------------------
# Управление: предпросмотр массового удаления
# --------------------------
@router.get("/manager/delete/preview", tags=["Tools"])
def preview_mass_delete(
    provider_id: str = "", country_id: str = "", operator_id: str = "",
    is_in_use: str = "", prefix: str = "", db: Session = Depends(get_db),
):
    q = db.query(func.count(models.PhoneNumber.id))
    if provider_id.isdigit(): q = q.filter(models.PhoneNumber.provider_id == int(provider_id))
    if country_id.isdigit(): q = q.filter(models.PhoneNumber.country_id == int(country_id))
    if operator_id.isdigit(): q = q.filter(models.PhoneNumber.operator_id == int(operator_id))
    if is_in_use in ("true", "false"): q = q.filter(models.PhoneNumber.is_in_use == (is_in_use == "true"))
    if prefix: q = q.filter(models.PhoneNumber.number_str.like(f"{prefix}%"))
    return JSONResponse({"count": q.scalar()})

# --------------------------
# Управление: массовое удаление (фон)
# --------------------------
@router.post("/manager/delete", tags=["Tools"])
async def handle_mass_delete(
    background_tasks: BackgroundTasks,
    provider_id: str = Form(""), country_id: str = Form(""), is_in_use: str = Form(""),
    operator_id: str = Form(""), prefix: str = Form(""),
):
    try:
        background_tasks.add_task(main_app.delete_numbers_in_background, provider_id, country_id, is_in_use, operator_id, prefix)
        return RedirectResponse(url="/tools?success=Процесс массового удаления запущен (см. логи).&tab=manager-pane", status_code=303)
    except Exception as e:
        log.error(f"Ошибка при запуске фоновой задачи удаления: {e}", exc_info=True)
        return RedirectResponse(url=f"/tools?error=Не удалось запустить задачу: {str(e)[:100]}&tab=manager-pane", status_code=303)

# --------------------------
# Пакетное «перемешивание» sort_order (фон)
# --------------------------
@router.post("/manager/shuffle", tags=["Tools"])
async def handle_shuffle(
    background_tasks: BackgroundTasks,
    provider_id: str = Form(""),
    country_id: str = Form(""),
    operator_id: str = Form(""),
    db: Session = Depends(get_db),
):
    try:
        if not (provider_id or country_id or operator_id):
            return RedirectResponse(
                url="/tools?error=Укажите хотя бы один фильтр (provider/country/operator).&tab=manager-pane",
                status_code=303
            )
        background_tasks.add_task(
            main_app.shuffle_numbers_in_background,
            provider_id, country_id, operator_id
        )
        return RedirectResponse(
            url="/tools?success=Перемешивание запущено в фоне (смотрите логи).&tab=manager-pane",
            status_code=303
        )
    except Exception as e:
        log.error(f"Ошибка перемешивания: {e}", exc_info=True)
        return RedirectResponse(
            url=f"/tools?error=Ошибка: {str(e)[:180]}&tab=manager-pane",
            status_code=303
        )

# --------------------------
# Обогащение «осиротевших» (фон)
# --------------------------
def enrich_orphans_in_background():
    db = SessionLocal()
    try:
        log.info("[ENRICH] Запуск фонового обогащения...")
        sql = text("""
            UPDATE orphan_sms o SET
                provider_id = COALESCE(o.provider_id, pn.provider_id),
                country_id  = COALESCE(o.country_id,  pn.country_id),
                operator_id = COALESCE(o.operator_id, pn.operator_id)
            FROM phone_numbers pn
            WHERE o.phone_number_str = pn.number_str
              AND (o.provider_id IS NULL OR o.country_id IS NULL OR o.operator_id IS NULL)
        """)
        result = db.execute(sql)
        db.commit()
        log.info(f"[ENRICH] Готово. Обновлено строк: {result.rowcount}")
    except Exception as e:
        log.exception("[ENRICH] Ошибка: %s", e)
        db.rollback()
    finally:
        db.close()

@router.post("/tools/orphans/enrich", tags=["Tools"])
async def trigger_enrich_orphans(background_tasks: BackgroundTasks):
    background_tasks.add_task(enrich_orphans_in_background)
    return RedirectResponse(url="/tools?success=Обогащение осиротевших запущено.&tab=orphan-traffic-pane", status_code=303)

@router.get("/tools/orphans/enrich", tags=["Tools"])
async def trigger_enrich_orphans_get(background_tasks: BackgroundTasks):
    background_tasks.add_task(enrich_orphans_in_background)
    return RedirectResponse(url="/tools?success=Обогащение осиротевших запущено.&tab=orphan-traffic-pane", status_code=303)
