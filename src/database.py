# src/database.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import time

from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

from src.settings import settings  # читаем значения из .env

# ---------- Сборка URL для SQLAlchemy (psycopg3) ----------
sqlalchemy_url = URL.create(
    drivername="postgresql+psycopg",
    username=str(settings.DB_USER).strip(),
    password=str(settings.DB_PASSWORD).strip(),
    host=str(settings.DB_HOST).strip(),
    port=int(settings.DB_PORT),
    database=str(settings.DB_NAME).strip(),
)

# Для локального хоста отключаем SSL-рукопожатие
connect_args: dict = {}
if str(settings.DB_HOST).strip() in {"127.0.0.1", "localhost"}:
    connect_args["sslmode"] = "disable"

# ---------- Создание engine с параметрами пула ----------
engine = create_engine(
    sqlalchemy_url,
    # Пул соединений (на процесс/воркер):
    pool_size=settings.DB_POOL_SIZE,        # базовый пул
    max_overflow=settings.DB_MAX_OVERFLOW,  # «сверху» к пулу
    pool_timeout=settings.DB_POOL_TIMEOUT,  # сек ждать свободный коннект
    pool_recycle=settings.DB_POOL_RECYCLE,  # сек до принудительного recycle
    pool_pre_ping=True,                     # чинит «мертвые» коннекты
    echo=settings.LOG_SQL,                  # подробный SQL (для отладки)
    connect_args=connect_args,
    future=True,
)

# ---------- Диагностика: лог «медленных» SQL ----------
SLOW_MS = 300  # порог, всё медленнее пишем в лог "sql.slow"

@event.listens_for(engine, "before_cursor_execute")
def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    context._query_start_time = time.time()

@event.listens_for(engine, "after_cursor_execute")
def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    start = getattr(context, "_query_start_time", None)
    if start is None:
        return
    total_ms = (time.time() - start) * 1000.0
    if total_ms >= SLOW_MS:
        # Упрощаем запрос в одну строку, чтобы лог был читаемым
        stmt_flat = " ".join(str(statement).split())
        logging.getLogger("sql.slow").warning(
            "SQL %.1f ms | %s | params=%s", total_ms, stmt_flat, parameters
        )

# ---------- Фабрика сессий ----------
# expire_on_commit=False — меньше лишних перезагрузок объектов из БД
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    bind=engine,
)
