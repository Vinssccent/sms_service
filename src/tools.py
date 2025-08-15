# src/tools.py
# -*- coding: utf-8 -*-
import datetime
import random
import string
import io
import csv
import logging
from datetime import timedelta
from typing import Optional, List

from fastapi import Request, UploadFile, Depends, Form, APIRouter, BackgroundTasks
from starlette.responses import RedirectResponse, StreamingResponse
from starlette.templating import Jinja2Templates

from sqlalchemy import func, text, bindparam, Integer, String
from sqlalchemy.orm import Session

from .database import SessionLocal
from . import models
from .utils import normalize_phone_number
from . import main as main_app  # для вызова фоновой очистки

log = logging.getLogger(__name__)

templates = Jinja2Templates(directory="templates")
router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# --------------------------
# Главная страница инструментов
# --------------------------
@router.get("/tools", tags=["Tools"], summary="Страница со всеми инструментами")
async def get_tools_page(
    request: Request,
    db: Session = Depends(get_db),
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    api_key_str: Optional[str] = None
):
    # --- Даты периода (UTC) ---
    try:
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc
            )
        else:
            start_date = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        if end_date_str:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, microsecond=0, tzinfo=datetime.timezone.utc
            )
        else:
            end_date = datetime.datetime.now(datetime.timezone.utc)
    except (ValueError, TypeError):
        start_date = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_date = datetime.datetime.now(datetime.timezone.utc)

    # --- Справочники ---
    providers = db.query(models.Provider).order_by(models.Provider.name).all()
    countries = db.query(models.Country).order_by(models.Country.name).all()
    operators = db.query(models.Operator).order_by(models.Operator.name).all()
    all_services = db.query(models.Service).order_by(models.Service.name).all()

    # --- Query-параметры ---
    q = request.query_params
    stat_group_by = (q.get("stat_group_by") or "service").strip()

    # ФИЛЬТРЫ ДЛЯ ДАШБОРДА
    stat_provider_id = int(q.get("stat_provider_id")) if (q.get("stat_provider_id") or "").isdigit() else None
    stat_country_id  = int(q.get("stat_country_id"))  if (q.get("stat_country_id")  or "").isdigit() else None
    stat_service_id  = int(q.get("stat_service_id"))  if (q.get("stat_service_id")  or "").isdigit() else None

    # ФИЛЬТРЫ «осиротевших»
    ot_search_sender = (q.get("ot_search_sender") or "").strip()
    ot_provider_id = int(q.get("ot_provider_id")) if (q.get("ot_provider_id") or "").isdigit() else None
    ot_country_id  = int(q.get("ot_country_id"))  if (q.get("ot_country_id")  or "").isdigit() else None
    ot_page = int(q.get("ot_page") or 1)
    if ot_page < 1: ot_page = 1
    per_page = 20  # как в шаблоне

    # Помощник: применяет фильтры дашборда
    def apply_stat_filters(qry, join_session=True, join_phone=False):
        if join_session:
            qry = qry.join(models.Session, models.SmsMessage.session_id == models.Session.id)
        if join_phone or stat_provider_id or stat_country_id:
            qry = qry.join(models.PhoneNumber, models.PhoneNumber.id == models.Session.phone_number_id)
        if stat_service_id:
            qry = qry.filter(models.Session.service_id == stat_service_id)
        if stat_provider_id:
            qry = qry.filter(models.PhoneNumber.provider_id == stat_provider_id)
        if stat_country_id:
            qry = qry.filter(models.PhoneNumber.country_id == stat_country_id)
        return qry

    # --- Сводные метрики (с учётом фильтров) ---
    total_sms = apply_stat_filters(
        db.query(models.SmsMessage).select_from(models.SmsMessage),   # ВАЖНО: select_from СНАЧАЛА
        join_session=True, join_phone=True
    ).filter(
        models.SmsMessage.received_at.between(start_date, end_date)
    ).count()

    unique_numbers = apply_stat_filters(
        db.query(func.count(func.distinct(models.Session.phone_number_id))).select_from(models.SmsMessage),
        join_session=True, join_phone=True
    ).filter(
        models.SmsMessage.received_at.between(start_date, end_date)
    ).scalar() or 0

    unique_services = apply_stat_filters(
        db.query(func.count(func.distinct(models.Session.service_id))).select_from(models.SmsMessage),
        join_session=True, join_phone=True
    ).filter(
        models.SmsMessage.received_at.between(start_date, end_date)
    ).scalar() or 0

    avg_sms_per_number = round((total_sms / unique_numbers), 2) if unique_numbers else 0.0

    numbers_in_use_now = db.query(models.PhoneNumber).filter(models.PhoneNumber.is_in_use.is_(True)).count()
    numbers_free_now = db.query(models.PhoneNumber).filter(
        models.PhoneNumber.is_in_use.is_(False), models.PhoneNumber.is_active.is_(True)
    ).count()

    # --- Таймсерия для графика (с учётом фильтров) ---
    counts_by_day = dict(
        apply_stat_filters(
            db.query(func.date(models.SmsMessage.received_at), func.count(models.SmsMessage.id))
              .select_from(models.SmsMessage),
            join_session=True, join_phone=True
        )
        .filter(models.SmsMessage.received_at.between(start_date, end_date))
        .group_by(func.date(models.SmsMessage.received_at))
        .all()
    )
    day_labels, day_values = [], []
    cur = start_date
    while cur.date() <= end_date.date():
        dstr = cur.strftime("%Y-%m-%d")
        day_labels.append(dstr)
        day_values.append(int(counts_by_day.get(dstr, 0) or counts_by_day.get(cur.date(), 0) or 0))
        cur += timedelta(days=1)
    chart_data = {"labels": day_labels, "data": day_values}

    # --- Правая детализация (по вкладке) + фильтры ---
    if stat_group_by == "service":
        details_table = apply_stat_filters(
            db.query(
                models.Service.name.label("name"),
                func.count(models.SmsMessage.id).label("sms_count"),
                func.count(func.distinct(models.Session.phone_number_id)).label("unique_numbers"),
            )
            .select_from(models.SmsMessage)
            .join(models.Session)
            .join(models.Service),
            join_session=False, join_phone=True
        ).filter(
            models.SmsMessage.received_at.between(start_date, end_date)
        ).group_by(
            models.Service.name
        ).order_by(
            func.count(models.SmsMessage.id).desc()
        ).limit(200).all()

    elif stat_group_by == "provider":
        details_table = apply_stat_filters(
            db.query(
                models.Provider.name.label("name"),
                func.count(models.SmsMessage.id).label("sms_count"),
                func.count(func.distinct(models.Session.phone_number_id)).label("unique_numbers"),
            )
            .select_from(models.SmsMessage)
            .join(models.Session)
            .join(models.PhoneNumber)
            .join(models.Provider),
            join_session=False, join_phone=False
        ).filter(
            models.SmsMessage.received_at.between(start_date, end_date)
        ).group_by(
            models.Provider.name
        ).order_by(
            func.count(models.SmsMessage.id).desc()
        ).limit(200).all()

    elif stat_group_by == "country":
        details_table = apply_stat_filters(
            db.query(
                models.Country.name.label("name"),
                func.count(models.SmsMessage.id).label("sms_count"),
                func.count(func.distinct(models.Session.phone_number_id)).label("unique_numbers"),
            )
            .select_from(models.SmsMessage)
            .join(models.Session)
            .join(models.PhoneNumber)
            .join(models.Country),
            join_session=False, join_phone=False
        ).filter(
            models.SmsMessage.received_at.between(start_date, end_date)
        ).group_by(
            models.Country.name
        ).order_by(
            func.count(models.SmsMessage.id).desc()
        ).limit(200).all()

    else:  # date
        details_table = apply_stat_filters(
            db.query(
                func.date(models.SmsMessage.received_at).label("name"),
                func.count(models.SmsMessage.id).label("sms_count"),
                func.count(func.distinct(models.Session.phone_number_id)).label("unique_numbers"),
            )
            .select_from(models.SmsMessage),
            join_session=True, join_phone=True
        ).filter(
            models.SmsMessage.received_at.between(start_date, end_date)
        ).group_by(
            func.date(models.SmsMessage.received_at)
        ).order_by(
            func.date(models.SmsMessage.received_at)
        ).all()

    # --- «Осиротевший» трафик (как раньше) + пагинация ---
    prov_id_expr = func.coalesce(models.OrphanSms.provider_id, models.PhoneNumber.provider_id)
    country_id_expr = func.coalesce(models.OrphanSms.country_id, models.PhoneNumber.country_id)
    oper_id_expr = func.coalesce(models.OrphanSms.operator_id, models.PhoneNumber.operator_id)

    prov_name_expr = func.coalesce(models.Provider.name, "—").label("provider_name")
    country_name_expr = func.coalesce(models.Country.name, "—").label("country_name")
    oper_name_expr = func.coalesce(models.Operator.name, "—").label("operator_name")

    base_q = (
        db.query(
            prov_name_expr,
            models.OrphanSms.source_addr.label("source_addr"),
            country_name_expr,
            oper_name_expr,
            func.min(models.OrphanSms.text).label("sample_text"),
            func.count(models.OrphanSms.id).label("sms_count"),
            func.count(func.distinct(models.OrphanSms.phone_number_str)).label("unique_numbers_count"),
            prov_id_expr.label("provider_id"),
            country_id_expr.label("country_id"),
            oper_id_expr.label("operator_id"),
        )
        .select_from(models.OrphanSms)
        .outerjoin(models.PhoneNumber, models.PhoneNumber.number_str == models.OrphanSms.phone_number_str)
        .outerjoin(models.Provider, models.Provider.id == prov_id_expr)
        .outerjoin(models.Country, models.Country.id == country_id_expr)
        .outerjoin(models.Operator, models.Operator.id == oper_id_expr)
        .filter(models.OrphanSms.received_at.between(start_date, end_date))
    )
    if ot_search_sender:
        base_q = base_q.filter(models.OrphanSms.source_addr.ilike(f"%{ot_search_sender}%"))
    if ot_provider_id:
        base_q = base_q.filter(prov_id_expr == ot_provider_id)
    if ot_country_id:
        base_q = base_q.filter(country_id_expr == ot_country_id)

    base_q = base_q.group_by(
        prov_name_expr, models.OrphanSms.source_addr, country_name_expr, oper_name_expr,
        prov_id_expr, country_id_expr, oper_id_expr
    ).order_by(func.count(models.OrphanSms.id).desc())

    all_groups = base_q.all()
    total_rows = len(all_groups)
    total_pages = max(1, (total_rows + per_page - 1) // per_page)
    if ot_page > total_pages: ot_page = total_pages
    start_idx = (ot_page - 1) * per_page
    end_idx = start_idx + per_page
    orphan_traffic_data = all_groups[start_idx:end_idx]

    ot_pagination = {"current_page": ot_page, "total_pages": total_pages, "total_rows": total_rows}

    # --- Объекты для шаблона ---
    dashboard_stats = {
        "total_sms": total_sms,
        "unique_numbers": unique_numbers,
        "unique_services": unique_services,
        "avg_sms_per_number": avg_sms_per_number,
        "numbers_in_use": numbers_in_use_now,
        "numbers_free": numbers_free_now,
        "period_start": start_date.strftime("%Y-%m-%d"),
        "period_end": end_date.strftime("%Y-%m-%d"),
    }
    stat_filters = {
        "group_by": stat_group_by,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "provider_id": stat_provider_id,
        "country_id": stat_country_id,
        "service_id": stat_service_id,
        "api_key": api_key_str or None,
    }
    ot_filters = {
        "search_sender": ot_search_sender,
        "provider_id": ot_provider_id,
        "country_id": ot_country_id,
        "operator_id": None,
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
    }

    context = {
        "request": request,
        "dashboard_stats": dashboard_stats,
        "stat_filters": stat_filters,
        "ot_filters": ot_filters,
        "ot_pagination": ot_pagination,

        "providers": providers,
        "countries": countries,
        "operators": operators,
        "all_services": all_services,

        "chart_data": chart_data,
        "details_table": details_table,
        "chart_provider_data": {"labels": [], "data": []},

        "orphan_traffic_data": orphan_traffic_data,
        "start_date_str": start_date.strftime("%Y-%m-%d"),
        "end_date_str": end_date.strftime("%Y-%m-%d"),
        "selected_key_str": api_key_str,
    }
    return templates.TemplateResponse("tools.html", context)


# --------------------------
# Детализация «осиротевших» по списку номеров
# --------------------------
@router.get("/tools/orphan-numbers-detail", tags=["Tools"])
def get_orphan_numbers_detail(
    request: Request,
    source_addr: str,
    provider_id: Optional[str] = None,   # ← принимаем как str (может прийти "")
    country_id: Optional[str] = None,    # ←
    operator_id: Optional[str] = None,   # ←
    db: Session = Depends(get_db),
    format: Optional[str] = None,
):
    # безопасно конвертируем в int либо None
    pid = int(provider_id) if provider_id and provider_id.isdigit() else None
    cid = int(country_id)  if country_id  and country_id.isdigit()  else None
    oid = int(operator_id) if operator_id and operator_id.isdigit() else None

    # Фильтрация по COALESCE(o.*, pn.*) — учитываем обе стороны сопоставления
    sql_numbers = text(
        """
        SELECT DISTINCT o.phone_number_str
        FROM orphan_sms o
        LEFT JOIN phone_numbers pn ON o.phone_number_str = pn.number_str
        WHERE
            o.source_addr = :source_addr
            AND (:provider_id IS NULL OR COALESCE(o.provider_id, pn.provider_id) = :provider_id)
            AND (:country_id  IS NULL OR COALESCE(o.country_id,  pn.country_id)  = :country_id)
            AND (:operator_id IS NULL OR COALESCE(o.operator_id, pn.operator_id) = :operator_id)
        ORDER BY o.phone_number_str
        """
    ).bindparams(
        bindparam("provider_id", type_=Integer),
        bindparam("country_id", type_=Integer),
        bindparam("operator_id", type_=Integer),
        bindparam("source_addr", type_=String),
    )

    sql_names = text(
        """
        SELECT 
            (SELECT name FROM providers WHERE id = :provider_id) AS provider_name,
            (SELECT name FROM countries WHERE id = :country_id) AS country_name,
            (SELECT name FROM operators WHERE id = :operator_id) AS operator_name
        """
    ).bindparams(
        bindparam("provider_id", type_=Integer),
        bindparam("country_id", type_=Integer),
        bindparam("operator_id", type_=Integer),
    )

    params = {
        "provider_id": pid,
        "country_id": cid,
        "operator_id": oid,
        "source_addr": source_addr,
    }

    res_numbers = db.execute(sql_numbers, params)
    res_names = db.execute(sql_names, params)

    numbers = [row[0] for row in res_numbers.fetchall()]
    names = res_names.fetchone()

    if format == "csv":
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow(["phone_number"])
        for number in numbers:
            writer.writerow([number])
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename=numbers_{source_addr}.csv"
        return response

    context = {
        "request": request,
        "numbers": numbers,
        "provider_name": (names.provider_name if names and names.provider_name else "N/A"),
        "country_name": (names.country_name if names and names.country_name else "N/A"),
        "operator_name": (names.operator_name if names and names.operator_name else "N/A"),
        "source_addr": source_addr,
    }
    return templates.TemplateResponse("orphan_numbers_detail_tool.html", context)


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
            limit = (
                db.query(models.ServiceLimit)
                .filter_by(service_id=service_id, provider_id=provider_id, country_id=country_id)
                .first()
            )
            if limit:
                limit.daily_limit = daily_limit
                updated_count += 1
            else:
                db.add(
                    models.ServiceLimit(
                        service_id=service_id, provider_id=provider_id, country_id=country_id, daily_limit=daily_limit
                    )
                )
                created_count += 1
        db.commit()
        return RedirectResponse(
            url=f"/tools?success=Успешно! Обновлено: {updated_count}, создано: {created_count}.&tab=bulk-limits-pane",
            status_code=303,
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=bulk-limits-pane", status_code=303
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

    # Нормализация и фильтрация пустых результатов
    raw_candidates = {normalize_phone_number(line.strip()) for line in lines}
    candidate_numbers = {n for n in raw_candidates if n}  # убираем "" (пустые)
    invalid_count = len(lines) - len(candidate_numbers)

    added_count, skipped_count = 0, 0
    candidate_list = list(candidate_numbers)
    batch_size = 5000

    try:
        for i in range(0, len(candidate_list), batch_size):
            batch = candidate_list[i : i + batch_size]
            existing_in_batch = {
                n[0] for n in db.query(models.PhoneNumber.number_str).filter(models.PhoneNumber.number_str.in_(batch))
            }
            skipped_count += len(existing_in_batch)
            new_numbers_to_add = [
                {
                    "number_str": num,
                    "provider_id": provider_id,
                    "country_id": country_id,
                    "operator_id": op_id,
                    "is_active": True,
                    "is_in_use": False,
                }
                for num in batch
                if num not in existing_in_batch
            ]
            if new_numbers_to_add:
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                added_count += len(new_numbers_to_add)
        db.commit()
        return RedirectResponse(
            url=f"/tools?success=Добавлено {added_count}. Дублей: {skipped_count}, невалидных: {invalid_count}.&tab=importer-pane",
            status_code=303,
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=importer-pane", status_code=303
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

            # Генерация уникальных вариантов
            candidates_to_generate = set()
            max_attempts = int(quantity * 1.5) + 1000
            while len(candidates_to_generate) < quantity and len(candidates_to_generate) < max_attempts:
                candidates_to_generate.add(f"{prefix}{''.join(random.choices(string.digits, k=num_x))}")

            candidate_list = list(candidates_to_generate)
            existing_numbers = {
                n[0]
                for n in db.query(models.PhoneNumber.number_str).filter(models.PhoneNumber.number_str.in_(candidate_list))
            }

            new_numbers_to_add = [
                {
                    "number_str": num,
                    "provider_id": provider_id,
                    "country_id": country_id,
                    "operator_id": op_id,
                    "is_active": True,
                    "is_in_use": False,
                }
                for num in candidate_list
                if num not in existing_numbers
            ]
            if new_numbers_to_add:
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                total_generated_count += len(new_numbers_to_add)

            total_skipped_count += (len(candidate_list) - len(new_numbers_to_add))

        db.commit()
        return RedirectResponse(
            url=f"/tools?success=Сгенерировано {total_generated_count} номеров по {len(masks_list)} маскам. Дублей: {total_skipped_count}.&tab=generator-pane",
            status_code=303,
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(
            url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=generator-pane", status_code=303
        )


# --------------------------
# Управление: массовое удаление и перемешивание sort_order
# --------------------------
@router.post("/manager/delete", tags=["Tools"])
async def handle_mass_delete(
    background_tasks: BackgroundTasks,
    provider_id: str = Form(""),
    country_id: str = Form(""),
    is_in_use: str = Form(""),
):
    try:
        background_tasks.add_task(main_app.delete_numbers_in_background, provider_id, country_id, is_in_use)
        return RedirectResponse(
            url="/tools?success=Процесс массового удаления запущен (см. логи).&tab=manager-pane",
            status_code=303,
        )
    except Exception as e:
        log.error(f"Ошибка при запуске фоновой задачи удаления: {e}", exc_info=True)
        return RedirectResponse(
            url=f"/tools?error=Не удалось запустить задачу: {str(e)[:100]}&tab=manager-pane",
            status_code=303,
        )


@router.post("/manager/shuffle", tags=["Tools"])
async def handle_shuffle(db: Session = Depends(get_db)):
    try:
        stmt = text(
            """
            WITH random_orders AS (
              SELECT id, row_number() OVER (ORDER BY random()) AS new_order
              FROM phone_numbers
            )
            UPDATE phone_numbers p
            SET sort_order = r.new_order
            FROM random_orders r
            WHERE p.id = r.id;
            """
        )
        result = db.execute(stmt)
        db.commit()
        return RedirectResponse(
            url=f"/tools?success=Перемешано {result.rowcount} номеров.&tab=manager-pane", status_code=303
        )
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/tools?error=Ошибка БД: {str(e)}&tab=manager-pane", status_code=303)

def enrich_orphans_in_background():
    # Отдельно импортируем, чтобы не было циклов
    from .database import SessionLocal
    from .utils import normalize_phone_number
    from . import models
    import logging

    log = logging.getLogger("enrich_orphans")
    db = SessionLocal()
    try:
        q = db.query(models.OrphanSms).filter(
            (models.OrphanSms.provider_id.is_(None)) |
            (models.OrphanSms.country_id.is_(None)) |
            (models.OrphanSms.operator_id.is_(None))
        )

        total, filled = 0, 0
        for o in q.yield_per(1000):
            total += 1
            # Нормализуем номер (на случай “сырых” исторических данных)
            norm = normalize_phone_number(o.phone_number_str)
            if norm and norm != o.phone_number_str:
                o.phone_number_str = norm

            pn = db.query(models.PhoneNumber).filter(
                models.PhoneNumber.number_str == o.phone_number_str
            ).first()

            if pn:
                if o.provider_id is None: o.provider_id = pn.provider_id
                if o.country_id  is None: o.country_id  = pn.country_id
                if o.operator_id is None: o.operator_id = pn.operator_id
                filled += 1

            if total % 500 == 0:
                db.flush()

        db.commit()
        log.info(f"[ENRICH] processed={total}, enriched={filled}")
    except Exception as e:
        log.exception("[ENRICH] failed: %s", e)
        db.rollback()
    finally:
        db.close()

@router.post("/tools/orphans/enrich", tags=["Tools"])
async def trigger_enrich_orphans(background_tasks: BackgroundTasks):
    background_tasks.add_task(enrich_orphans_in_background)
    return RedirectResponse(
        url="/tools?success=Обогащение осиротевших запущено.&tab=orphan-traffic-pane",
        status_code=303
    )
    
@router.get("/tools/orphans/enrich", tags=["Tools"])
async def trigger_enrich_orphans_get(background_tasks: BackgroundTasks):
    background_tasks.add_task(enrich_orphans_in_background)
    return RedirectResponse(
        url="/tools?success=Обогащение осиротевших запущено.&tab=orphan-traffic-pane",
        status_code=303
    )

# ============================================================================