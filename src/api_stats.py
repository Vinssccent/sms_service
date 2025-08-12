# src/api_stats.py
# -*- coding: utf-8 -*-
import datetime
from fastapi import Request, Depends, APIRouter, Query
from sqlalchemy import func
from sqlalchemy.orm import Session
from .database import SessionLocal
from . import models
from .main import templates

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/api-stats", tags=["Tools"], summary="Страница статистики по API")
async def get_api_stats_page(
    request: Request,
    api_key_id: int = Query(None),
    period: str = Query("today"),
    db: Session = Depends(get_db)
):
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
            start_date = today_start
        elif period == "yesterday":
            start_date = today_start - datetime.timedelta(days=1)
            end_date = today_start
        elif period == "7days":
            start_date = today_start - datetime.timedelta(days=7)
        elif period == "month":
            start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            start_date = today_start

        query = (
            db.query(models.Service.name, func.count(models.SmsMessage.id))
            .join(models.Session, models.Service.id == models.Session.service_id)
            .join(models.SmsMessage, models.Session.id == models.SmsMessage.session_id)
            .filter(models.Session.api_key_id == api_key_id)
            .filter(models.SmsMessage.received_at >= start_date)
        )
        if period == "yesterday":
            query = query.filter(models.SmsMessage.received_at < end_date)
        
        service_breakdown = query.group_by(models.Service.name).order_by(func.count(models.SmsMessage.id).desc()).all()
        
        total_sms = sum(count for name, count in service_breakdown)

        context["stats"] = {
            "total_sms": total_sms,
            "service_breakdown": [{"name": name, "count": count} for name, count in service_breakdown]
        }
        context["selected_key"] = db.query(models.ApiKey).get(api_key_id)

    return templates.TemplateResponse("api_stats.html", context)
