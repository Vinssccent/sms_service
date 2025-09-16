# src/add_perf_indexes.py
# -*- coding: utf-8 -*-
"""
Одноразовый скрипт для добавления индексов производительности под самые «тяжёлые» места:
- быстрый отбор свободных номеров без COUNT/OFFSET (pivot по sort_order)
- ускорение счётчиков и агрегаций на /tools (orphan_sms, sms_messages)
- типичные join-ы для sessions

Запуск:
    source venv/bin/activate
    python -m src.add_perf_indexes

Скрипт безопасен: использует CREATE INDEX IF NOT EXISTS.
Если у тебя очень большая БД и высокая нагрузка – желательно запускать ночью.
"""
from __future__ import annotations

import sys
from sqlalchemy import text
from src.database import engine
from src.logging_setup import configure_main_logging

log = configure_main_logging(logger_name="perf.indexes")

SQLS = [
    # ---- phone_numbers: быстрый подбор без COUNT/OFFSET ----
    # Частичный индекс по активным и свободным
    """
    CREATE INDEX IF NOT EXISTS ix_phone_numbers_fast_select
    ON phone_numbers (provider_id, country_id, sort_order)
    WHERE is_active IS TRUE AND is_in_use IS FALSE
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_phone_numbers_fast_select_op
    ON phone_numbers (provider_id, country_id, operator_id, sort_order)
    WHERE is_active IS TRUE AND is_in_use IS FALSE
    """,
    # Дешёвые счётчики/флаги
    """
    CREATE INDEX IF NOT EXISTS ix_phone_numbers_in_use_true
    ON phone_numbers (id) WHERE is_in_use IS TRUE
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_phone_numbers_free_active
    ON phone_numbers (id) WHERE is_in_use IS FALSE AND is_active IS TRUE
    """,

    # ---- orphan_sms: аналитика «осиротевшего» трафика ----
    """
    CREATE INDEX IF NOT EXISTS ix_orphan_sms_recv_at_source
    ON orphan_sms (received_at, source_addr)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_orphan_sms_recv_at_provider_country_operator
    ON orphan_sms (received_at, provider_id, country_id, operator_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_orphan_sms_recv_at_phone
    ON orphan_sms (received_at, phone_number_str)
    """,

    # ---- sms_messages: частые выборки/агрегации за период ----
    """
    CREATE INDEX IF NOT EXISTS ix_sms_messages_session_received_at
    ON sms_messages (session_id, received_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS ix_sms_messages_source_received_at
    ON sms_messages (source_addr, received_at)
    """,

    # ---- sessions: типичный join service -> session -> phone_number ----
    """
    CREATE INDEX IF NOT EXISTS ix_sessions_service_phone
    ON sessions (service_id, phone_number_id)
    """,
]

def main() -> int:
    # Быстрый sanity-check на Postgres
    if engine.url.get_backend_name() not in ("postgresql", "postgresql+psycopg", "postgresql+psycopg2"):
        log.error("Движок БД не PostgreSQL. Найдено: %s", engine.url.get_backend_name())
        return 2

    log.info("Подключаюсь к БД: %s", engine.url)
    ok = 0
    with engine.begin() as conn:
        for sql in SQLS:
            sql_clean = " ".join(line.strip() for line in sql.strip().splitlines())
            try:
                log.info("Создаю индекс: %s", sql_clean.split("IF NOT EXISTS", 1)[-1].strip()[:120] + " ...")
                conn.execute(text(sql))
                ok += 1
            except Exception as e:
                log.error("Ошибка при создании индекса: %s", e)

    log.info("Готово. Создано/проверено индексов: %d", ok)
    return 0

if __name__ == "__main__":
    sys.exit(main())
