# src/api_stats.py
# -*- coding: utf-8 -*-
import datetime
from typing import Optional

from fastapi import Request, Depends, APIRouter
from sqlalchemy import func
from sqlalchemy.orm import Session
from starlette.templating import Jinja2Templates

from pydantic import BaseModel, field_validator

from .database import SessionLocal
from . import models

router = APIRouter()
templates = Jinja2Templates(directory="templates")


class ApiStatsParams(BaseModel):
    api_key_id: Optional[int] = None
    period: str = "today"

    @field_validator("api_key_id", mode="before")
    @classmethod
    def _parse_api_key_id(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @field_validator("period", mode="before")
    @classmethod
    def _normalize_period(cls, value):
        if value is None:
            return "today"
        return str(value).strip() or "today"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/api-stats", tags=["Tools"], summary="Страница статистики по API")
def get_api_stats_page(
    request: Request,
    params: ApiStatsParams = Depends(),
    db: Session = Depends(get_db)
):
    api_key_id = params.api_key_id
    period = params.period

    api_keys = db.query(models.ApiKey).filter(models.ApiKey.is_active == True).all()
    context = {
        "request": request,
        "api_keys": api_keys,
        "selected_key_id": api_key_id,
        "selected_period": period,
        "stats": None,
        "selected_key": None
    }

    if api_key_id:
        now = datetime.datetime.now(datetime.timezone.utc)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if period == "today":
            start_date, end_date = today_start, None
        elif period == "yesterday":
            start_date, end_date = today_start - datetime.timedelta(days=1), today_start
        elif period == "7days":
            start_date, end_date = today_start - datetime.timedelta(days=7), None
        elif period == "month":
            start_date, end_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0), None
        else:
            start_date, end_date = today_start, None

        query = (
            db.query(models.Service.name, func.count(models.SmsMessage.id))
            .join(models.Session, models.Service.id == models.Session.service_id)
            .join(models.SmsMessage, models.Session.id == models.SmsMessage.session_id)
            .filter(models.Session.api_key_id == api_key_id)
            .filter(models.SmsMessage.received_at >= start_date)
        )
        if end_date:
            query = query.filter(models.SmsMessage.received_at < end_date)

        service_breakdown = query.group_by(models.Service.name).order_by(func.count(models.SmsMessage.id).desc()).all()
        total_sms = sum(count for _, count in service_breakdown)

        context["stats"] = {
            "total_sms": total_sms,
            "service_breakdown": [{"name": name, "count": count} for name, count in service_breakdown],
        }
        context["selected_key"] = db.get(models.ApiKey, api_key_id)

    return templates.TemplateResponse("api_stats.html", context)
