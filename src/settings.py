# src/settings.py
# -*- coding: utf-8 -*-
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Класс для хранения настроек приложения.
    Все настройки читаются из файла .env
    """
    # --- Настройки базы данных ---
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str
    DB_HOST: str
    # DB_PORT не является обязательным, будет использовано значение по умолчанию 5432
    DB_PORT: int = 5432

    # --- Настройки для отладки ---
    # Установите LOG_SQL=True в .env для вывода всех SQL-запросов в консоль.
    # ВНИМАНИЕ: Не используйте в production, это замедляет работу.
    LOG_SQL: bool = False

    # --- Настройки веб-сервера (можно переопределить в .env) ---
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    
    # --- Свойства, которые собираются из других переменных ---
    @property
    def DATABASE_URL(self) -> str:
        """
        Динамически формирует полную строку подключения к базе данных
        на основе отдельных переменных.
        """
        return f"postgresql+psycopg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

# Создаем единый экземпляр настроек, который будем импортировать в другие файлы
settings = Settings()