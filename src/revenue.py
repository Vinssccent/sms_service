# -*- coding: utf-8 -*-
from __future__ import annotations
import datetime, io, csv
from typing import List, Optional
from decimal import Decimal, ROUND_HALF_UP

from fastapi import APIRouter, Request, Depends, Query
from starlette.templating import Jinja2Templates
from starlette.responses import StreamingResponse

from sqlalchemy.orm import Session
from sqlalchemy import func, cast, Numeric

from . import models
from src.deps import get_db

router = APIRouter()
templates = Jinja2Templates(directory="templates")
def _parse_date(s: Optional[str], default_today: bool = False) -> Optional[datetime.datetime]:
    if not s:
        return (datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                if default_today else None)
    try:
        dt = datetime.datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.datetime.strptime(s, "%Y-%m-%d")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt

def _parse_int(opt: Optional[str]) -> Optional[int]:
    if opt is None:
        return None
    s = str(opt).strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None

def _decimal(val, q: str) -> Decimal:
    d = Decimal("0") if val is None else (val if isinstance(val, Decimal) else Decimal(str(val)))
    return d.quantize(Decimal(q), rounding=ROUND_HALF_UP)

@router.get("/revenue", tags=["Revenue"], summary="Отчёт по выручке")
def revenue_page(
    request: Request,
    start_date_str: Optional[str] = Query(None),
    end_date_str: Optional[str] = Query(None),
    # ВАЖНО: провайдер как строка, чтобы спокойно принимать "" без ошибок валидации
    provider_id: Optional[str] = Query(None),
    operator_ids: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db),
):
    start_dt = _parse_date(start_date_str, default_today=True)
    end_dt   = _parse_date(end_date_str)
    provider_id_int = _parse_int(provider_id)

    providers = (
        db.query(models.Provider)
        .filter(models.Provider.is_active == True)
        .order_by(models.Provider.name)
        .all()
    )

    ops_q = db.query(models.Operator)
    if provider_id_int:
        ops_q = ops_q.filter(models.Operator.provider_id == provider_id_int)
    operators = ops_q.order_by(models.Operator.name).all()

    # точная цена в евро: берем price_eur, иначе price_eur_cent/100 как NUMERIC(10,5)
    price_eur_expr = func.coalesce(
        models.Operator.price_eur,
        (cast(models.Operator.price_eur_cent, Numeric(10, 5)) / 100)
    )

    q = (
        db.query(
            models.Operator.id.label("operator_id"),
            models.Operator.name.label("operator_name"),
            price_eur_expr.label("price_eur"),
            func.count(models.SmsMessage.id).label("sms_count"),
        )
        .join(models.PhoneNumber, models.PhoneNumber.operator_id == models.Operator.id)
        .join(models.Session, models.Session.phone_number_id == models.PhoneNumber.id)
        .join(models.SmsMessage, models.SmsMessage.session_id == models.Session.id)
    )
    if start_dt:
        q = q.filter(models.SmsMessage.received_at >= start_dt)
    if end_dt:
        q = q.filter(models.SmsMessage.received_at < end_dt)
    if provider_id_int:
        q = q.filter(models.Operator.provider_id == provider_id_int)
    if operator_ids:
        q = q.filter(models.Operator.id.in_(operator_ids))

    q = q.group_by(models.Operator.id, models.Operator.name, price_eur_expr)\
         .order_by(func.count(models.SmsMessage.id).desc())
    rows = q.all()

    table = []
    total_sms = 0
    total_eur = Decimal("0.00")
    for r in rows:
        sms_cnt = int(r.sms_count or 0)
        price_eur = _decimal(r.price_eur, "0.00001")     # 5 знаков для цены
        amount_eur = _decimal(price_eur * Decimal(sms_cnt), "0.01")  # 2 знака для суммы
        table.append({
            "operator_id": r.operator_id,
            "operator_name": r.operator_name,
            "price_eur": price_eur,
            "sms_count": sms_cnt,
            "amount_eur": amount_eur,
        })
        total_sms += sms_cnt
        total_eur += amount_eur

    context = {
        "request": request,
        "providers": providers,
        "operators": operators,
        "selected_provider_id": provider_id_int,
        "selected_operator_ids": operator_ids or [],
        "start_date_str": (start_dt.date().isoformat() if start_dt else ""),
        "end_date_str": (end_dt.date().isoformat() if end_dt else ""),
        "rows": table,
        "total_sms": total_sms,
        "total_eur": _decimal(total_eur, "0.01"),
    }
    return templates.TemplateResponse("revenue.html", context)

@router.get("/revenue/export", tags=["Revenue"], summary="Экспорт выручки (CSV)")
def revenue_export(
    start_date_str: Optional[str] = Query(None),
    end_date_str: Optional[str] = Query(None),
    provider_id: Optional[str] = Query(None),
    operator_ids: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db),
):
    start_dt = _parse_date(start_date_str, default_today=True)
    end_dt   = _parse_date(end_date_str)
    provider_id_int = _parse_int(provider_id)

    price_eur_expr = func.coalesce(
        models.Operator.price_eur,
        (cast(models.Operator.price_eur_cent, Numeric(10, 5)) / 100)
    )

    q = (
        db.query(
            models.Operator.id.label("operator_id"),
            models.Operator.name.label("operator_name"),
            price_eur_expr.label("price_eur"),
            func.count(models.SmsMessage.id).label("sms_count"),
        )
        .join(models.PhoneNumber, models.PhoneNumber.operator_id == models.Operator.id)
        .join(models.Session, models.Session.phone_number_id == models.PhoneNumber.id)
        .join(models.SmsMessage, models.SmsMessage.session_id == models.Session.id)
    )
    if start_dt:
        q = q.filter(models.SmsMessage.received_at >= start_dt)
    if end_dt:
        q = q.filter(models.SmsMessage.received_at < end_dt)
    if provider_id_int:
        q = q.filter(models.Operator.provider_id == provider_id_int)
    if operator_ids:
        q = q.filter(models.Operator.id.in_(operator_ids))
    q = q.group_by(models.Operator.id, models.Operator.name, price_eur_expr)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["operator_id", "operator_name", "price_eur", "sms_count", "amount_eur"])
    for r in q.all():
        sms_cnt = int(r.sms_count or 0)
        price_eur = _decimal(r.price_eur, "0.00001")
        amount_eur = _decimal(price_eur * Decimal(sms_cnt), "0.01")
        w.writerow([r.operator_id, r.operator_name, f"{price_eur:.5f}", sms_cnt, f"{amount_eur:.2f}"])
    buf.seek(0)

    fname = f"revenue_{(start_dt.date().isoformat() if start_dt else 'all')}_to_{(end_dt.date().isoformat() if end_dt else 'now')}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{fname}"'}
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8", headers=headers)
