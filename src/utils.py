# src/utils.py
# -*- coding: utf-8 -*-
import phonenumbers
import re
from phonenumbers import NumberParseException

def normalize_phone_number(phone: str, region_hint: str = "RU") -> str:
    """
    Гибридная функция нормализации номера телефона к формату E.164 (+79991234567).

    Стратегия:
    1. Сначала пытается выполнить "умную" нормализацию с помощью библиотеки
       phonenumbers. Это хорошо работает для форматов с префиксами (8..., +7...).
    2. Если "умная" нормализация не удалась (например, для номеров без плюса,
       которые phonenumbers считает невалидными), функция переходит
       к "простой" нормализации: очищает строку от нецифровых символов
       и добавляет '+' в начало.

    :param phone: Строка с номером телефона для нормализации.
    :param region_hint: Код страны (ISO 3166-1) для "умной" нормализации.
    :return: Нормализованный номер в формате E.164 или пустую строку.
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
        # Если не удалось, это не страшно, переходим к плану Б.
        pass

    # --- Попытка 2: Простая "доверительная" нормализация ---
    digits = re.sub(r'\D+', '', phone)
    if digits:
        return f"+{digits}"

    # Если ничего не помогло
    return ""
