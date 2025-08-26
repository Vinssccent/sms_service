# src/settings.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import List
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Централизованные настройки приложения.
    Источник: переменные окружения и/или .env (см. model_config ниже).
    """

    # --- База данных ---
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    DB_HOST: str
    DB_PORT: int = 5432

    # --- Пул соединений к БД (ДОБАВЛЕНО) ---
    DB_POOL_SIZE: int = 50
    DB_MAX_OVERFLOW: int = 200
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 1800

    # --- Веб-сервер (если используется HTTP/API) ---
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    # --- Логи/отладка ---
    LOG_SQL: bool = False  # Читает src/database.py при создании engine (echo=settings.LOG_SQL)

    # --- SMPP: входящий сервер (они подключаются к нам) ---
    SMPP_BIND_HOST: str = "0.0.0.0"
    SMPP_BIND_PORT: int = 40000  # порт по умолчанию
    # Можно указать несколько портов одной строкой или JSON-массивом.
    # Примеры:
    #   SMPP_BIND_PORTS_RAW=40000,40001
    #   SMPP_BIND_PORTS_RAW=40000;40001
    #   SMPP_BIND_PORTS_RAW=["40000","40001"]
    SMPP_BIND_PORTS_RAW: str = ""

    # --- SMPP: whitelist IP ---
    # CSV/JSON со списком IP/CIDR:
    #   ALLOWED_SMPP_IPS_RAW=78.40.196.153/32,194.28.164.22,18.133.186.14/32
    #   ALLOWED_SMPP_IPS_RAW=["78.40.196.153/32","194.28.164.22"]
    ALLOWED_SMPP_IPS_RAW: str = ""
    SMPP_WHITELIST_ALLOW_LOCALHOST: bool = True
    SMPP_WHITELIST_FROM_DB: bool = True          # добирать CIDR из provider_ips и smpp_host из providers
    SMPP_WHITELIST_REFRESH_SECONDS: int = 60     # TTL кэша whitelist’а в секундах

    # --- Производные свойства/утилиты ---
    @property
    def DATABASE_URL(self) -> str:
        """
        Собираем URL для SQLAlchemy с драйвером psycopg (psycopg3).
        Все части аккуратно экранируем.
        """
        user = quote_plus((self.DB_USER or "").strip())
        pwd = quote_plus((self.DB_PASSWORD or "").strip())
        host = (self.DB_HOST or "").strip()
        db = (self.DB_NAME or "").strip()
        port = int(self.DB_PORT) if str(self.DB_PORT).strip() else 5432
        return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"

    def get_allowed_smpp_ips(self) -> List[str]:
        """
        Разобрать ALLOWED_SMPP_IPS_RAW в список строк (IP/CIDR).
        Поддерживает JSON-массив и CSV/«;».
        """
        raw = (self.ALLOWED_SMPP_IPS_RAW or "").strip()
        if not raw:
            return []
        # JSON-массив
        if raw.startswith("["):
            try:
                arr = json.loads(raw)
                return [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                pass
        # CSV/semicolon
        parts = raw.replace(";", ",").split(",")
        return [p.strip() for p in parts if p.strip()]

    def get_smpp_bind_ports(self) -> List[int]:
        """
        Вернуть список портов для прослушивания SMPP.
        Приоритет: SMPP_BIND_PORTS_RAW -> одиночный SMPP_BIND_PORT.
        """
        raw = (self.SMPP_BIND_PORTS_RAW or "").strip()
        if not raw:
            return [int(self.SMPP_BIND_PORT)]
        ports: List[int] = []
        # JSON-массив
        if raw.startswith("["):
            try:
                arr = json.loads(raw)
                for x in arr:
                    try:
                        ports.append(int(str(x).strip()))
                    except Exception:
                        pass
                return sorted(set(ports)) or [int(self.SMPP_BIND_PORT)]
            except Exception:
                pass
        # CSV/semicolon
        for item in raw.replace(";", ",").split(","):
            item = item.strip()
            if not item:
                continue
            try:
                ports.append(int(item))
            except Exception:
                pass
        return sorted(set(ports)) or [int(self.SMPP_BIND_PORT)]

    # Pydantic Settings конфигурация: читаем .env и игнорируем лишние переменные
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


# Единый инстанс настроек
settings = Settings()
