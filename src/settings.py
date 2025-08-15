# src/settings.py
# -*- coding: utf-8 -*-
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
import json

class Settings(BaseSettings):
    """
    Настройки приложения (.env либо переменные окружения).
    """

    # --- БД ---
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    DB_HOST: str
    DB_PORT: int = 5432

    # --- Веб-сервер ---
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    # --- Отладка ---
    LOG_SQL: bool = False

    # --- SMPP-сервер ---
    # Хост для прослушивания
    SMPP_BIND_HOST: str = "0.0.0.0"
    # Старый одиночный порт (для совместимости). Если задам ниже список, он проигнорирует одиночный.
    SMPP_BIND_PORT: int = 40000
    # НОВОЕ: можно указать несколько портов через запятую/точку с запятой или JSON-массив.
    # Примеры:
    #   SMPP_BIND_PORTS_RAW=40000,10002
    #   SMPP_BIND_PORTS_RAW=["40000","10002"]
    SMPP_BIND_PORTS_RAW: str = ""

    # --- WHITELIST ---
    # Разрешённые IP/CIDR (CSV или JSON). Примеры:
    #   ALLOWED_SMPP_IPS_RAW=78.40.196.153/32,194.28.164.22,18.133.186.14/32
    #   ALLOWED_SMPP_IPS_RAW=["78.40.196.153/32","194.28.164.22"]
    ALLOWED_SMPP_IPS_RAW: str = ""
    # Разрешать loopback для локальных тестов (127.0.0.1, ::1)
    SMPP_WHITELIST_ALLOW_LOCALHOST: bool = True
    # Подгружать whitelist из БД
    SMPP_WHITELIST_FROM_DB: bool = True
    # Период обновления кеша whitelist (сек)
    SMPP_WHITELIST_REFRESH_SECONDS: int = 60

    # --- Сборные свойства ---
    @property
    def DATABASE_URL(self) -> str:
        return (
            f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    def get_allowed_smpp_ips(self) -> List[str]:
        """Вернёт список IP/CIDR из RAW (умеет JSON и CSV/;)."""
        raw = (self.ALLOWED_SMPP_IPS_RAW or "").strip()
        if not raw:
            return []
        if raw.startswith("["):
            try:
                arr = json.loads(raw)
                return [str(x).strip() for x in arr if str(x).strip()]
            except Exception:
                pass
        parts = raw.replace(";", ",").split(",")
        return [p.strip() for p in parts if p.strip()]

    def get_smpp_bind_ports(self) -> List[int]:
        """Вернёт список портов для прослушивания (из RAW или одиночный SMPP_BIND_PORT)."""
        raw = (self.SMPP_BIND_PORTS_RAW or "").strip()
        if not raw:
            return [int(self.SMPP_BIND_PORT)]
        ports: List[int] = []
        if raw.startswith("["):
            try:
                arr = json.loads(raw)
                for x in arr:
                    try:
                        ports.append(int(str(x).strip()))
                    except Exception:
                        pass
                return sorted(set(ports))
            except Exception:
                pass
        for part in raw.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                ports.append(int(part))
            except Exception:
                pass
        return sorted(set(ports)) or [int(self.SMPP_BIND_PORT)]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

settings = Settings()
