# src/utils.py
# -*- coding: utf-8 -*-
import phonenumbers
import re
from phonenumbers import NumberParseException
from typing import List, Set

def normalize_phone_number(phone: str, region_hint: str = "RU") -> str:
    """
    Нормализация к E.164 (+79991234567).
    Сначала "умная" нормализация через phonenumbers, затем fallback: +digits.
    """
    if not phone:
        return ""

    # --- Попытка 1: Умная нормализация ---
    try:
        parsed_number = phonenumbers.parse(phone, region_hint)
        if phonenumbers.is_valid_number(parsed_number):
            return phonenumbers.format_number(
                parsed_number,
                phonenumbers.PhoneNumberFormat.E164
            )
    except NumberParseException:
        pass

    # --- Попытка 2: Простая нормализация ---
    digits = re.sub(r'\D+', '', phone)
    if digits:
        return f"+{digits}"

    return ""

def build_phone_candidates(phone_raw: str) -> List[str]:
    """
    Строит набор безопасных вариантов представления одного номера,
    чтобы повысить шанс совпадения с тем, что лежит в БД:
      • как есть (обрезанные пробелы)
      • только цифры
      • +только_цифры (E.164 без ведущих нулей)
    Порядок важен: сначала самый "правильный" вариант.
    """
    s = (phone_raw or "").strip()
    digits = re.sub(r'\D+', '', s)
    cands: List[str] = []

    # 1) Попытка получить корректный E.164
    e164 = normalize_phone_number(s, region_hint="RU")
    if e164 and e164 not in cands:
        cands.append(e164)

    # 2) +digits
    if digits:
        plus_digits = f"+{digits}"
        if plus_digits not in cands:
            cands.append(plus_digits)

    # 3) digits (без плюса) — часто так лежит в БД/или приходит в PDU
    if digits and digits not in cands:
        cands.append(digits)

    # 4) исходная строка (если отличается от уже добавленных)
    if s and s not in cands:
        cands.append(s)

    return cands
