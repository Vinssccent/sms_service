# src/tester.py
# -*- coding: utf-8 -*-
import logging
import random
import string
from typing import Optional

from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse, Response
from starlette.templating import Jinja2Templates

from . import models
from src.deps import get_db
from .utils import normalize_phone_number

log = logging.getLogger(__name__)

templates = Jinja2Templates(directory="templates")
router = APIRouter()
@router.get("/tester", tags=["Tools"], include_in_schema=False)
async def get_standalone_tester_page(request: Request, db: Session = Depends(get_db)):
    countries = db.query(models.Country).order_by(models.Country.name).all()
    services = db.query(models.Service).order_by(models.Service.name).all()
    providers = db.query(models.Provider).filter(models.Provider.is_active == True).all()
    api_keys = db.query(models.ApiKey).filter(models.ApiKey.is_active == True).order_by(
        models.ApiKey.description).all()
    context = {
        "request": request, "countries": countries, "services": services,
        "providers": providers, "api_keys": api_keys,
    }
    return templates.TemplateResponse("api_tester_standalone.html", context)


@router.get("/tester/get_full_sms", tags=["Tools"], include_in_schema=False)
async def get_full_sms_details(session_id: int, db: Session = Depends(get_db)):
    if not session_id:
        return JSONResponse({"error": "No session ID provided"}, status_code=400)
    sms = (db.query(models.SmsMessage).filter(models.SmsMessage.session_id == session_id)
           .order_by(models.SmsMessage.received_at.desc()).first())
    if not sms:
        return JSONResponse({"status": "wait"})
    return JSONResponse({
        "status": "ok", "text": sms.text, "sender": sms.source_addr,
        "code": sms.code, "received_at": sms.received_at.isoformat()
    })


@router.post("/tester/reuse_number", tags=["Tools"])
async def handle_reuse_number(request: Request, db: Session = Depends(get_db)):
    from .main import get_repeat_number, get_valid_api_key

    form = await request.json()
    api_key_str = form.get("api_key")
    if not api_key_str: raise HTTPException(status_code=400, detail="API_KEY_REQUIRED")
        
    api_key_obj = await get_valid_api_key(api_key=api_key_str, db=db)
    
    number_str, service_code = form.get("number"), form.get("service_code")
    if not number_str or not service_code: raise HTTPException(status_code=400, detail="BAD_PARAMS")
    response = await get_repeat_number(db, api_key_obj, service_code, number_str)
    return response


@router.post("/tester/get_number_by_mask", tags=["Tools"])
async def handle_get_number_by_mask(request: Request, db: Session = Depends(get_db)):
    from .main import get_valid_api_key
    
    form = await request.json()
    api_key_str = form.get("api_key")
    if not api_key_str: raise HTTPException(status_code=400, detail="API_KEY_REQUIRED")
        
    api_key_obj = await get_valid_api_key(api_key=api_key_str, db=db)
    
    mask = form.get("mask", "").strip().lower()
    service_code = form.get("service_code")
    
    try:
        country_id = int(form.get("country_id"))
    except (ValueError, TypeError):
        return Response("Неверный ID страны. Должно быть число.", status_code=400)

    if not all([mask, service_code, country_id]):
        return Response("Маска, сервис и страна обязательны", status_code=400)
    if 'x' not in mask:
        return Response("Маска должна содержать символ 'x'", status_code=400)
        
    # ИСПРАВЛЕНИЕ ЗДЕСЬ: Используем .filter() вместо .filter_by() для явного сравнения.
    # Это самый надежный способ указать SQLAlchemy и драйверу правильный тип данных.
    providers_in_country_q = db.query(models.PhoneNumber.provider_id).filter(models.PhoneNumber.country_id == country_id).distinct()
    
    providers_in_country_ids = {pid for (pid,) in providers_in_country_q.all()}
    if not providers_in_country_ids:
        return Response(f"В базе нет номеров для страны ID={country_id}, невозможно выбрать провайдера.", status_code=404)
        
    active_providers_q = db.query(models.Provider.id).filter(models.Provider.is_active == True, models.Provider.id.in_(providers_in_country_ids))
    valid_provider_ids = [pid for (pid,) in active_providers_q.all()]
    if not valid_provider_ids:
        return Response(f"Не найдено АКТИВНЫХ провайдеров для страны ID={country_id}.", status_code=404)
        
    chosen_provider_id = random.choice(valid_provider_ids)
    
    prefix, num_x = mask.split('x')[0], mask.count('x')
    for _ in range(20):
        generated_number = prefix + ''.join(random.choices(string.digits, k=num_x))
        if not db.query(models.PhoneNumber).filter(models.PhoneNumber.number_str == generated_number).first():
            break
    else:
        return Response(f"Не удалось сгенерировать уникальный номер для маски {mask} за 20 попыток.", status_code=409)
        
    new_phone = models.PhoneNumber(
        number_str=generated_number, provider_id=chosen_provider_id, country_id=country_id,
        is_active=True, is_in_use=True
    )
    db.add(new_phone); db.commit(); db.refresh(new_phone)
    
    service_obj = db.query(models.Service).filter(models.Service.code == service_code).first()
    if not service_obj: return Response("BAD_SERVICE", status_code=400)
    
    session = models.Session(
        phone_number_str=new_phone.number_str, service_id=service_obj.id, phone_number_id=new_phone.id,
        api_key_id=api_key_obj.id, status=1,
    )
    db.add(session); db.commit(); db.refresh(session)
    
    return Response(f"ACCESS_NUMBER:{session.id}:{new_phone.number_str.replace('+', '')}", media_type="text/plain")


@router.post("/tester/custom_number", tags=["Tools"])
async def handle_custom_number(request: Request, db: Session = Depends(get_db)):
    from .main import get_repeat_number, get_valid_api_key

    form = await request.json()
    api_key_str = form.get("api_key")
    if not api_key_str: raise HTTPException(status_code=400, detail="API_KEY_REQUIRED")
        
    api_key_obj = await get_valid_api_key(api_key=api_key_str, db=db)
    
    number_str = form.get("number")
    service_code = form.get("service_code")

    if not number_str or not service_code:
        return Response("Номер и сервис обязательны", status_code=400)
    
    normalized_number = normalize_phone_number(number_str)
    if not normalized_number:
        return Response("Неверный формат номера", status_code=400)
    
    return await get_repeat_number(db, api_key_obj, service_code, normalized_number)