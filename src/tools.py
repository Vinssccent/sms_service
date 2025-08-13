# src/tools.py
# -*- coding: utf-8 -*-
import datetime
import random
import string
import io
import csv
import math
import logging
import time
from datetime import timedelta
from typing import Optional, List
from fastapi import Request, UploadFile, Depends, Form, APIRouter, BackgroundTasks
from starlette.responses import RedirectResponse, StreamingResponse
from sqlalchemy import func, cast, Date, text, distinct
from sqlalchemy.orm import Session, selectinload
from starlette.templating import Jinja2Templates

from .database import SessionLocal, engine
from . import models
from .utils import normalize_phone_number
from . import main as main_app # Импортируем main, чтобы получить доступ к фоновой функции

# Добавляем логгер для отладки
log = logging.getLogger(__name__)

templates = Jinja2Templates(directory="templates")
router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/tools", tags=["Tools"], summary="Страница со всеми инструментами")
async def get_tools_page(
    request: Request,
    db: Session = Depends(get_db),
    start_date_str: Optional[str] = None,
    end_date_str: Optional[str] = None,
    api_key_str: Optional[str] = None
):
    query_params = request.query_params
    
    # --- Общие параметры даты ---
    try:
        if start_date_str:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)
        else:
            start_date = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)

        if end_date_str:
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=datetime.timezone.utc)
        else:
            end_date = datetime.datetime.now(datetime.timezone.utc)
            
    except (ValueError, TypeError):
        start_date = (datetime.datetime.now(datetime.timezone.utc) - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = datetime.datetime.now(datetime.timezone.utc)

    # --- Старая, простая статистика для обратной совместимости ---
    numbers_in_use_now = db.query(models.PhoneNumber).filter(models.PhoneNumber.is_in_use == True).count()
    numbers_free_now = db.query(models.PhoneNumber).filter(models.PhoneNumber.is_in_use == False, models.PhoneNumber.is_active == True).count()
    total_sms_in_period_simple = db.query(models.SmsMessage).filter(models.SmsMessage.received_at.between(start_date, end_date)).count()
    top_services_q = (
        db.query(models.Service.name, models.Service.icon_class, func.count(models.SmsMessage.id).label("sms_count"))
        .select_from(models.SmsMessage).join(models.Session).join(models.Service)
        .filter(models.SmsMessage.received_at.between(start_date, end_date))
        .group_by(models.Service.name, models.Service.icon_class).order_by(func.count(models.SmsMessage.id).desc()).limit(10)
    )
    provider_load_q = (
        db.query(models.Provider.name, func.count(models.SmsMessage.id).label("sms_count"))
        .select_from(models.SmsMessage).join(models.Session).join(models.PhoneNumber).join(models.Provider)
        .filter(models.SmsMessage.received_at.between(start_date, end_date))
        .group_by(models.Provider.name).order_by(func.count(models.SmsMessage.id).desc()).limit(10)
    )
    general_stats = {
        "total_sms_in_period": total_sms_in_period_simple,
        "numbers_in_use": numbers_in_use_now, "numbers_free": numbers_free_now,
        "top_services": top_services_q.all(),
        "provider_load": provider_load_q.all(),
    }

    # --- Логика для нового Аналитического дашборда ---
    stat_provider_id_str = query_params.get('stat_provider_id')
    stat_provider_id = int(stat_provider_id_str) if stat_provider_id_str and stat_provider_id_str.isdigit() else None
    stat_country_id_str = query_params.get('stat_country_id')
    stat_country_id = int(stat_country_id_str) if stat_country_id_str and stat_country_id_str.isdigit() else None
    stat_service_id_str = query_params.get('stat_service_id')
    stat_service_id = int(stat_service_id_str) if stat_service_id_str and stat_service_id_str.isdigit() else None
    stat_group_by = query_params.get('stat_group_by', 'service')

    base_stats_query = (
        db.query(
            models.SmsMessage.id.label("sms_id"),
            models.SmsMessage.received_at,
            models.SmsMessage.session_id,
            models.Session.service_id,
            models.PhoneNumber.id.label('phone_number_id'),
            models.PhoneNumber.provider_id,
            models.PhoneNumber.country_id
        )
        .join(models.Session, models.SmsMessage.session_id == models.Session.id)
        .join(models.PhoneNumber, models.Session.phone_number_id == models.PhoneNumber.id)
        .filter(models.SmsMessage.received_at.between(start_date, end_date))
    )

    if stat_provider_id:
        base_stats_query = base_stats_query.filter(models.PhoneNumber.provider_id == stat_provider_id)
    if stat_country_id:
        base_stats_query = base_stats_query.filter(models.PhoneNumber.country_id == stat_country_id)
    if stat_service_id:
        base_stats_query = base_stats_query.filter(models.Session.service_id == stat_service_id)

    stats_subquery = base_stats_query.subquery('stats_subquery')
    
    total_sms = db.query(func.count(stats_subquery.c.sms_id)).scalar()
    unique_numbers_count = db.query(func.count(distinct(stats_subquery.c.phone_number_id))).scalar()
    unique_services_count = db.query(func.count(distinct(stats_subquery.c.service_id))).scalar()
    avg_sms_per_number = total_sms / unique_numbers_count if unique_numbers_count > 0 else 0

    dashboard_stats = {
        "total_sms": total_sms,
        "unique_numbers": unique_numbers_count,
        "unique_services": unique_services_count,
        "avg_sms_per_number": round(avg_sms_per_number, 2)
    }

    sms_by_day_q = (
        db.query(cast(stats_subquery.c.received_at, Date).label("date"), func.count(stats_subquery.c.sms_id).label("count"))
        .group_by("date").order_by("date")
    )
    chart_data = {"labels": [item.date.strftime("%Y-%m-%d") for item in sms_by_day_q], "data": [item.count for item in sms_by_day_q]}

    details_table = []
    if stat_group_by == 'provider':
        details_table_query = db.query(models.Provider.name.label('name'), func.count(stats_subquery.c.sms_id).label('sms_count'), func.count(distinct(stats_subquery.c.phone_number_id)).label('unique_numbers')).select_from(stats_subquery).join(models.Provider, models.Provider.id == stats_subquery.c.provider_id).group_by(models.Provider.name).order_by(func.count(stats_subquery.c.sms_id).desc())
        details_table = details_table_query.all()
    elif stat_group_by == 'country':
        details_table_query = db.query(models.Country.name.label('name'), func.count(stats_subquery.c.sms_id).label('sms_count'), func.count(distinct(stats_subquery.c.phone_number_id)).label('unique_numbers')).select_from(stats_subquery).join(models.Country, models.Country.id == stats_subquery.c.country_id).group_by(models.Country.name).order_by(func.count(stats_subquery.c.sms_id).desc())
        details_table = details_table_query.all()
    elif stat_group_by == 'date':
        details_table_query = db.query(cast(stats_subquery.c.received_at, Date).label('name'), func.count(stats_subquery.c.sms_id).label('sms_count'), func.count(distinct(stats_subquery.c.phone_number_id)).label('unique_numbers')).select_from(stats_subquery).group_by(cast(stats_subquery.c.received_at, Date)).order_by(cast(stats_subquery.c.received_at, Date).desc())
        details_table_raw = details_table_query.all()
        details_table = [{'name': row.name.strftime('%d.%m.%Y'), 'sms_count': row.sms_count, 'unique_numbers': row.unique_numbers} for row in details_table_raw]
    else: 
        details_table_query = db.query(models.Service.name.label('name'), func.count(stats_subquery.c.sms_id).label('sms_count'), func.count(distinct(stats_subquery.c.phone_number_id)).label('unique_numbers')).select_from(stats_subquery).join(models.Session, models.Session.id == stats_subquery.c.session_id).join(models.Service, models.Service.id == models.Session.service_id).group_by(models.Service.name).order_by(func.count(stats_subquery.c.sms_id).desc())
        details_table = details_table_query.all()

    ot_page = int(query_params.get('ot_page', 1))
    ot_search_sender = query_params.get('ot_search_sender')
    ot_provider_id_str = query_params.get('ot_provider_id')
    ot_provider_id = int(ot_provider_id_str) if ot_provider_id_str and ot_provider_id_str.isdigit() else None
    ot_country_id_str = query_params.get('ot_country_id')
    ot_country_id = int(ot_country_id_str) if ot_country_id_str and ot_country_id_str.isdigit() else None
    count_query_str = "SELECT COUNT(*) FROM (SELECT 1 FROM orphan_sms o JOIN phone_numbers pn ON o.phone_number_str = pn.number_str {where_clause} GROUP BY pn.provider_id, pn.country_id, pn.operator_id, o.source_addr) as subquery"
    data_query_str = "SELECT p.name AS provider_name, c.name AS country_name, op.name AS operator_name, o.source_addr, MIN(o.text) AS sample_text, COUNT(o.id) AS sms_count, p.id AS provider_id, c.id AS country_id, pn.operator_id AS operator_id FROM orphan_sms o JOIN phone_numbers pn ON o.phone_number_str = pn.number_str JOIN providers p ON pn.provider_id = p.id JOIN countries c ON pn.country_id = c.id LEFT JOIN operators op ON pn.operator_id = op.id {where_clause} GROUP BY p.name, c.name, op.name, o.source_addr, p.id, c.id, pn.operator_id ORDER BY sms_count DESC LIMIT :limit OFFSET :offset;"
    where_conditions, params = [], {}
    if ot_search_sender: where_conditions.append("o.source_addr ILIKE :sender"); params['sender'] = f"%{ot_search_sender}%"
    if ot_provider_id: where_conditions.append("pn.provider_id = :provider_id"); params['provider_id'] = ot_provider_id
    if ot_country_id: where_conditions.append("pn.country_id = :country_id"); params['country_id'] = ot_country_id
    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    total_count_query = text(count_query_str.format(where_clause=where_clause))
    total_rows = db.execute(total_count_query, params).scalar_one() or 0
    page_size, offset = 20, (ot_page - 1) * 20
    total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 0
    params['limit'], params['offset'] = page_size, offset
    data_query = text(data_query_str.format(where_clause=where_clause))
    orphan_traffic_data = db.execute(data_query, params).mappings().all()
    
    selected_key, api_stats_data, error_api_stats = None, None, None
    if api_key_str:
        selected_key = db.query(models.ApiKey).filter(models.ApiKey.key == api_key_str).first()
        if not selected_key: error_api_stats = "API ключ не найден."
        else:
            service_breakdown_q = db.query(models.Service.name, func.count(models.SmsMessage.id).label("sms_count")).select_from(models.SmsMessage).join(models.Session).join(models.Service).filter(models.Session.api_key_id == selected_key.id, models.SmsMessage.received_at.between(start_date, end_date)).group_by(models.Service.name).order_by(func.count(models.SmsMessage.id).desc())
            total_sms_api = sum(item.sms_count for item in service_breakdown_q.all())
            api_stats_data = {"total_sms": total_sms_api, "service_breakdown": service_breakdown_q.all()}

    providers = db.query(models.Provider).order_by(models.Provider.name).all()
    countries = db.query(models.Country).order_by(models.Country.name).all()
    operators = db.query(models.Operator).options(selectinload(models.Operator.country), selectinload(models.Operator.provider)).order_by(models.Operator.name).all()
    all_services = db.query(models.Service).order_by(models.Service.name).all()

    context = {
        "request": request, "providers": providers, "countries": countries,
        "operators": operators, "all_services": all_services,
        "general_stats": general_stats,
        "dashboard_stats": dashboard_stats, "chart_data": chart_data,
        "details_table": details_table,
        "stat_filters": { "provider_id": stat_provider_id, "country_id": stat_country_id, "service_id": stat_service_id, "group_by": stat_group_by },
        "orphan_traffic_data": orphan_traffic_data,
        "ot_pagination": { "current_page": ot_page, "total_pages": total_pages, "total_rows": total_rows },
        "ot_filters": { "search_sender": ot_search_sender, "provider_id": ot_provider_id, "country_id": ot_country_id },
        "api_stats": api_stats_data, "error_api_stats": error_api_stats, 
        "selected_key_str": api_key_str, "selected_key": selected_key,
        "start_date_str": start_date.strftime("%Y-%m-%d"), "end_date_str": end_date.strftime("%Y-%m-%d"),
    }
    return templates.TemplateResponse("tools.html", context)

@router.get("/tools/orphan-numbers-detail", tags=["Tools"])
def get_orphan_numbers_detail(request: Request, provider_id: int, source_addr: str, country_id: int, db: Session = Depends(get_db), operator_id: Optional[int] = None, format: Optional[str] = None):
    sql_numbers = text("SELECT DISTINCT o.phone_number_str FROM orphan_sms o JOIN phone_numbers pn ON o.phone_number_str = pn.number_str WHERE pn.provider_id = :provider_id AND o.source_addr = :source_addr AND pn.country_id = :country_id AND (pn.operator_id = :operator_id OR (:operator_id IS NULL AND pn.operator_id IS NULL)) ORDER BY o.phone_number_str;")
    sql_names = text("SELECT p.name as provider_name, c.name as country_name, op.name as operator_name FROM providers p, countries c LEFT JOIN operators op ON op.id = :operator_id WHERE p.id = :provider_id AND c.id = :country_id")
    params = {"provider_id": provider_id, "source_addr": source_addr, "country_id": country_id, "operator_id": operator_id}
    res_numbers, res_names = db.execute(sql_numbers, params), db.execute(sql_names, params)
    numbers, names = [row[0] for row in res_numbers.fetchall()], res_names.fetchone()
    if format == "csv":
        stream, writer = io.StringIO(), csv.writer(stream)
        writer.writerow(["phone_number"])
        for number in numbers: writer.writerow([number])
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename=numbers_{provider_id}_{source_addr}.csv"
        return response
    context = {"request": request, "numbers": numbers, "provider_name": names.provider_name if names else 'N/A', "source_addr": source_addr, "country_name": names.country_name if names else 'N/A', "operator_name": names.operator_name if names and names.operator_name else 'N/A'}
    return templates.TemplateResponse("orphan_numbers_detail_tool.html", context)

@router.post("/tools/bulk-set-limits", tags=["Tools"])
async def handle_bulk_limits(service_ids: List[int] = Form(...), provider_id: int = Form(...), country_id: int = Form(...), daily_limit: int = Form(...), db: Session = Depends(get_db)):
    try:
        updated_count, created_count = 0, 0
        for service_id in service_ids:
            limit = db.query(models.ServiceLimit).filter_by(service_id=service_id, provider_id=provider_id, country_id=country_id).first()
            if limit: limit.daily_limit = daily_limit; updated_count += 1
            else: db.add(models.ServiceLimit(service_id=service_id, provider_id=provider_id, country_id=country_id, daily_limit=daily_limit)); created_count += 1
        db.commit()
        return RedirectResponse(url=f"/tools?success=Успешно! Обновлено: {updated_count}, Создано: {created_count} лимитов.&tab=bulk-limits-pane", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=bulk-limits-pane", status_code=303)

@router.post("/importer", tags=["Tools"])
async def handle_file_upload(file: UploadFile = Form(...), provider_id: int = Form(...), country_id: int = Form(...), operator_id: Optional[str] = Form(""), db: Session = Depends(get_db)):
    op_id = int(operator_id) if operator_id else None
    content = await file.read()
    lines = content.decode('utf-8', errors='ignore').splitlines()
    candidate_numbers = {normalize_phone_number(line.strip()) for line in lines}
    candidate_numbers.discard(None)
    invalid_count, added_count, skipped_count = len(lines) - len(candidate_numbers), 0, 0
    candidate_list = list(candidate_numbers)
    batch_size = 5000
    try:
        for i in range(0, len(candidate_list), batch_size):
            batch = candidate_list[i:i + batch_size]
            existing_in_batch = {n[0] for n in db.query(models.PhoneNumber.number_str).filter(models.PhoneNumber.number_str.in_(batch))}
            skipped_count += len(existing_in_batch)
            new_numbers_to_add = [{"number_str": num, "provider_id": provider_id, "country_id": country_id, "operator_id": op_id, "is_active": True, "is_in_use": False} for num in batch if num not in existing_in_batch]
            if new_numbers_to_add:
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                added_count += len(new_numbers_to_add)
        db.commit()
        return RedirectResponse(url=f"/tools?success=Добавлено {added_count} номеров. Пропущено дублей: {skipped_count}, невалидных: {invalid_count}.&tab=importer-pane", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=importer-pane", status_code=303)

@router.post("/generator", tags=["Tools"])
def handle_range_generation(
    masks_text: str = Form(..., alias="masks"),
    quantity_per_mask: int = Form(..., alias="quantity"),
    provider_id: int = Form(...),
    country_id: int = Form(...),
    operator_id: Optional[str] = Form(""),
    db: Session = Depends(get_db)
):
    op_id = int(operator_id) if operator_id and operator_id.isdigit() else None
    
    masks = [m.strip() for m in masks_text.strip().splitlines() if m.strip()]
    if not masks:
        return RedirectResponse(url="/tools?error=Не введено ни одной маски.&tab=generator-pane", status_code=303)

    total_generated_count = 0
    total_skipped_count = 0
    
    try:
        for mask in masks:
            if 'x' not in mask.lower():
                log.warning(f"Пропущена неверная маска (нет 'x'): {mask}")
                continue

            prefix = mask.lower().split('x')[0]
            num_x = mask.lower().count('x')
            
            generated_for_this_mask = 0
            max_attempts = int(quantity_per_mask * 1.5) + 1000 
            
            candidates_to_generate = set()
            while len(candidates_to_generate) < quantity_per_mask and len(candidates_to_generate) < max_attempts:
                candidates_to_generate.add(f"{prefix}{''.join(random.choices(string.digits, k=num_x))}")
            
            candidate_list = list(candidates_to_generate)
            existing_numbers = {n[0] for n in db.query(models.PhoneNumber.number_str).filter(models.PhoneNumber.number_str.in_(candidate_list))}
            
            new_numbers_to_add = [
                {
                    "number_str": num, 
                    "provider_id": provider_id, 
                    "country_id": country_id, 
                    "operator_id": op_id, 
                    "is_active": True, 
                    "is_in_use": False
                } 
                for num in candidate_list if num not in existing_numbers
            ]
            
            if new_numbers_to_add:
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                total_generated_count += len(new_numbers_to_add)
            
            total_skipped_count += (len(candidate_list) - len(new_numbers_to_add))
        
        db.commit()
        return RedirectResponse(url=f"/tools?success=Всего сгенерировано {total_generated_count} номеров по {len(masks)} маскам. Пропущено дублей: {total_skipped_count}.&tab=generator-pane", status_code=303)

    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/tools?error=Ошибка БД: {str(e)[:100]}&tab=generator-pane", status_code=303)

@router.post("/manager/delete", tags=["Tools"])
async def handle_mass_delete(
    background_tasks: BackgroundTasks,
    provider_id: str = Form(""), 
    country_id: str = Form(""), 
    is_in_use: str = Form("")
):
    try:
        background_tasks.add_task(
            main_app.delete_numbers_in_background, 
            provider_id, 
            country_id, 
            is_in_use
        )
        return RedirectResponse(
            url="/tools?success=Процесс массового удаления запущен в фоновом режиме. Следите за логами.&tab=manager-pane", 
            status_code=303
        )
    except Exception as e:
        log.error(f"Ошибка при запуске фоновой задачи удаления: {e}", exc_info=True)
        return RedirectResponse(url=f"/tools?error=Не удалось запустить задачу: {str(e)[:100]}&tab=manager-pane", status_code=303)

@router.post("/manager/shuffle", tags=["Tools"])
async def handle_shuffle(db: Session = Depends(get_db)):
    try:
        stmt = text("WITH random_orders AS (SELECT id, row_number() OVER (ORDER BY random()) as new_order FROM phone_numbers) UPDATE phone_numbers p SET sort_order = r.new_order FROM random_orders r WHERE p.id = r.id;")
        result = db.execute(stmt)
        db.commit()
        return RedirectResponse(url=f"/tools?success=Перемешано {result.rowcount} номеров.&tab=manager-pane", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/tools?error=Ошибка БД: {str(e)}&tab=manager-pane", status_code=303)