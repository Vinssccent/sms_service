# -*- coding: utf-8 -*-
# Запуск:
#   source venv/bin/activate && python -m src.add_operator_price_eur_column
from sqlalchemy import text
from src.database import engine

SQLS = [
    "ALTER TABLE operators ADD COLUMN IF NOT EXISTS price_eur NUMERIC(10,5)",
    "COMMENT ON COLUMN operators.price_eur IS 'Цена за 1 SMS в евро (десятичная, до 5 знаков)'",
]

def main():
    with engine.begin() as conn:
        for sql in SQLS:
            conn.execute(text(sql))
    print("OK: добавлена колонка operators.price_eur (NUMERIC(10,5)).")

if __name__ == "__main__":
    main()
