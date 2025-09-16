# src/add_operator_price_column.py
# -*- coding: utf-8 -*-
"""
Одноразовый безопасный скрипт: добавляет колонку operators.price_eur_cent и индекс на provider_id.
Повторный запуск не навредит (IF NOT EXISTS).
Запуск:
    source venv/bin/activate
    python -m src.add_operator_price_column
"""
from __future__ import annotations
import logging
from sqlalchemy import text
from src.database import engine

log = logging.getLogger("add_operator_price")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

SQLS = [
    "ALTER TABLE operators ADD COLUMN IF NOT EXISTS price_eur_cent INTEGER",
    "COMMENT ON COLUMN operators.price_eur_cent IS 'Цена за 1 SMS в евроцентах'",
    "CREATE INDEX IF NOT EXISTS ix_operators_provider_id ON operators(provider_id)",
]

def main():
    with engine.begin() as conn:
        for sql in SQLS:
            log.info("EXEC: %s", sql.replace("\n", " "))
            conn.execute(text(sql))
    log.info("Готово: колонка price_eur_cent и индекс созданы (если их не было).")

if __name__ == "__main__":
    main()
