# src/database.py
# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Импортируем единый экземпляр настроек
from src.settings import settings

# Создаем движок SQLAlchemy, используя уже готовую строку подключения
# и флаг логирования из настроек.
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,  # Проверяет "живо" ли соединение перед использованием
    pool_recycle=1800,   # Пересоздает соединения старше 30 минут, чтобы избежать проблем с обрывами
    echo=settings.LOG_SQL # Управляет логированием SQL-запросов через .env файл
)

# Создаем фабрику сессий.
# autocommit=False и autoflush=False - стандарт для работы с FastAPI,
# позволяет вручную управлять транзакциями.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)