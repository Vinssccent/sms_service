# src/importer.py
# -*- coding: utf-8 -*-
import random
import string
from fastapi import Request, UploadFile, Depends, Form, APIRouter
from fastapi.templating import Jinja2Templates
from starlette.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import text, insert
from .database import SessionLocal
from . import models
from .utils import normalize_phone_number

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =============================
# Импорт номеров из файла (ОПТИМИЗИРОВАНО)
# =============================
@router.get("/importer", tags=["Tools"], summary="Импорт номеров из файла")
async def get_importer_page(request: Request, db: Session = Depends(get_db)):
    providers = db.query(models.Provider).all()
    countries = db.query(models.Country).all()
    return templates.TemplateResponse("importer.html", {
        "request": request,
        "providers": providers,
        "countries": countries,
        "error": request.query_params.get("error"),
        "success": request.query_params.get("success"),
    })

@router.post("/importer", tags=["Tools"])
async def handle_file_upload(request: Request, file: UploadFile = Form(...),
                             provider_id: int = Form(...),
                             country_id: int = Form(...),
                             db: Session = Depends(get_db)):
    content = await file.read()
    lines = content.decode('utf-8', errors='ignore').splitlines()
    added_count, skipped_count, invalid_count = 0, 0, 0

    # Собираем уникальный сет кандидатов из файла
    candidate_numbers = set()
    for line in lines:
        number_str = line.strip()
        if not number_str:
            continue
        normalized = normalize_phone_number(number_str)
        if not normalized:
            invalid_count += 1
        else:
            candidate_numbers.add(normalized)

    # Преобразуем в список для обработки батчами
    candidate_list = list(candidate_numbers)
    batch_size = 5000  # Размер пачки для обработки

    try:
        for i in range(0, len(candidate_list), batch_size):
            batch = candidate_list[i:i + batch_size]

            # За один запрос к БД узнаем, какие номера из пачки уже существуют
            existing_in_batch = {n[0] for n in db.query(models.PhoneNumber.number_str).filter(models.PhoneNumber.number_str.in_(batch))}
            skipped_count += len(existing_in_batch)

            # Готовим к добавлению только те, которых нет
            new_numbers_to_add = [
                {
                    "number_str": num,
                    "provider_id": provider_id,
                    "country_id": country_id,
                    "is_active": True,
                    "is_in_use": False
                }
                for num in batch if num not in existing_in_batch
            ]

            if new_numbers_to_add:
                # Используем bulk_insert_mappings для эффективности
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                added_count += len(new_numbers_to_add)
        
        db.commit()
        message = f"Добавлено {added_count} номеров. Пропущено дублей: {skipped_count}, невалидных: {invalid_count}."
        return RedirectResponse(url=f"/importer?success={message}", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/importer?error=Ошибка БД: {str(e)[:100]}", status_code=303)


# =============================
# Генератор номеров по маске (ОПТИМИЗИРОВАНО)
# =============================
@router.post("/generator", tags=["Tools"])
async def handle_range_generation(request: Request, mask: str = Form(...),
                                  quantity: int = Form(...),
                                  provider_id: int = Form(...),
                                  country_id: int = Form(...),
                                  db: Session = Depends(get_db)):
    mask = mask.strip()
    if not mask or 'X' not in mask:
        return RedirectResponse(url=f"/generator?error=Неверная маска (используйте X).", status_code=303)

    prefix = mask.split('X')[0]
    num_x = mask.count('X')
    generated_count = 0
    
    # Максимальное количество попыток, чтобы избежать бесконечного цикла, если все номера заняты
    max_attempts = quantity * 20  

    try:
        for _ in range(max_attempts):
            if generated_count >= quantity:
                break

            # Генерируем пачку кандидатов
            candidates_to_check = set()
            for _ in range(min(quantity - generated_count, 5000)): # Генерируем до 5000 за раз
                random_part = ''.join(random.choices(string.digits, k=num_x))
                candidates_to_check.add(f"{prefix}{random_part}")

            # Проверяем, какие из сгенерированных уже есть в БД
            existing_in_batch = {n[0] for n in db.query(models.PhoneNumber.number_str).filter(models.PhoneNumber.number_str.in_(candidates_to_check))}

            new_numbers_to_add = [
                {
                    "number_str": num,
                    "provider_id": provider_id,
                    "country_id": country_id,
                    "is_active": True,
                    "is_in_use": False
                }
                for num in candidates_to_check if num not in existing_in_batch
            ]

            if new_numbers_to_add:
                db.bulk_insert_mappings(models.PhoneNumber, new_numbers_to_add)
                generated_count += len(new_numbers_to_add)
        
        db.commit()
        skipped_count = max_attempts - generated_count
        message = f"Сгенерировано {generated_count}. Пропущено попыток (дубли или невалидные): {skipped_count}."
        return RedirectResponse(url=f"/generator?success={message}", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/generator?error=Ошибка БД: {str(e)[:100]}", status_code=303)


# =============================
# Массовое удаление номеров
# =============================
@router.post("/manager/delete", tags=["Tools"])
async def handle_mass_delete(provider_id: str = Form(""), country_id: str = Form(""),
                             is_in_use: str = Form(""), db: Session = Depends(get_db)):
    try:
        query = db.query(models.PhoneNumber)
        if provider_id:
            query = query.filter(models.PhoneNumber.provider_id == int(provider_id))
        if country_id:
            query = query.filter(models.PhoneNumber.country_id == int(country_id))
        if is_in_use:
            query = query.filter(models.PhoneNumber.is_in_use == (is_in_use == 'true'))
        
        # synchronize_session=False - это быстрая стратегия удаления
        deleted_count = query.delete(synchronize_session=False)
        db.commit()
        return RedirectResponse(url=f"/manager?success=Удалено {deleted_count} номеров.", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/manager?error=Ошибка: {str(e)[:100]}", status_code=303)

# =============================
# Перемешивание номеров (ОПТИМИЗИРОВАНО)
# =============================
@router.post("/manager/shuffle", tags=["Tools"])
async def handle_shuffle(db: Session = Depends(get_db)):
    try:
        # Этот SQL-запрос выполняется полностью на стороне PostgreSQL,
        # он эффективен и не потребляет память на сервере приложения.
        stmt = text("""
            WITH random_orders AS (
                SELECT id, row_number() OVER (ORDER BY random()) as new_order
                FROM phone_numbers
            )
            UPDATE phone_numbers p
            SET sort_order = r.new_order
            FROM random_orders r
            WHERE p.id = r.id;
        """)
        result = db.execute(stmt)
        db.commit()
        return RedirectResponse(url=f"/manager?success=Перемешано {result.rowcount} номеров.", status_code=303)
    except Exception as e:
        db.rollback()
        return RedirectResponse(url=f"/manager?error=Ошибка БД: {str(e)}", status_code=303)