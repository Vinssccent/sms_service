# seed_scripts/db_maintenance.py
# -*- coding: utf-8 -*-
"""
Безопасное обслуживание БД:
- настраивает fillfactor и autovacuum;
- создаёт функцию prune_old_data(...) для батч-удалений;
- по команде очищает старые данные;
- по команде делает REINDEX CONCURRENTLY.
Запуск:
  python -m seed_scripts.db_maintenance --setup
  PRUNE_SMS_DAYS=30 PRUNE_ORPHAN_DAYS=14 PRUNE_SESSION_DAYS=30 python -m seed_scripts.db_maintenance --prune
  python -m seed_scripts.db_maintenance --reindex
"""

import os
import argparse
from typing import List, Tuple

from sqlalchemy import text
from src.database import engine, SessionLocal

# -------- helpers (вне транзакции) --------
def exec_no_tx(sql: str) -> None:
    """Выполнить одиночный SQL вне транзакции (для CONCURRENTLY/VACUUM/REINDEX)."""
    eng = engine.execution_options(isolation_level="AUTOCOMMIT")
    with eng.connect() as conn:
        conn.exec_driver_sql(sql)

# -------- параметры хранения --------
TABLE_PARAMS = {
    "phone_numbers": {
        "fillfactor": 80,
        "autovacuum_vacuum_scale_factor": 0.02,
        "autovacuum_analyze_scale_factor": 0.02,
    },
    "sessions": {
        "fillfactor": 90,
        "autovacuum_vacuum_scale_factor": 0.02,
        "autovacuum_analyze_scale_factor": 0.02,
    },
    "sms_messages": {
        "fillfactor": 90,
        "autovacuum_vacuum_scale_factor": 0.02,
        "autovacuum_analyze_scale_factor": 0.02,
    },
    "orphan_sms": {
        "fillfactor": 90,
        "autovacuum_vacuum_scale_factor": 0.02,
        "autovacuum_analyze_scale_factor": 0.02,
    },
}

def set_table_params():
    for table, params in TABLE_PARAMS.items():
        opts = ", ".join(f'{k} = {v}' for k, v in params.items())
        sql = f'ALTER TABLE {table} SET ({opts})'
        print(f"[params] {table}: {opts}")
        exec_no_tx(sql)
    # лёгкая статистика
    for table in TABLE_PARAMS.keys():
        print(f"[analyze] {table}")
        exec_no_tx(f"VACUUM (ANALYZE) {table};")
    print("[params] готово")

# -------- функция чистки --------
PRUNE_FUNC_SQL = r"""
CREATE OR REPLACE FUNCTION prune_old_data(
  p_sms_days int DEFAULT 30,
  p_orphan_days int DEFAULT 14,
  p_session_days int DEFAULT 30
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  _cut_sms timestamptz := (now() at time zone 'utc') - make_interval(days => p_sms_days);
  _cut_orphan timestamptz := (now() at time zone 'utc') - make_interval(days => p_orphan_days);
  _cut_sess timestamptz := (now() at time zone 'utc') - make_interval(days => p_session_days);
  _r int;
BEGIN
  -- sms_messages
  LOOP
    DELETE FROM sms_messages
    WHERE ctid IN (
      SELECT ctid FROM sms_messages
      WHERE received_at < _cut_sms
      LIMIT 5000
    );
    GET DIAGNOSTICS _r = ROW_COUNT;
    EXIT WHEN _r = 0;
    PERFORM pg_sleep(0.05);
  END LOOP;

  -- orphan_sms
  LOOP
    DELETE FROM orphan_sms
    WHERE ctid IN (
      SELECT ctid FROM orphan_sms
      WHERE received_at < _cut_orphan
      LIMIT 5000
    );
    GET DIAGNOSTICS _r = ROW_COUNT;
    EXIT WHEN _r = 0;
    PERFORM pg_sleep(0.05);
  END LOOP;

  -- sessions (только закрытые)
  LOOP
    DELETE FROM sessions s
    WHERE ctid IN (
      SELECT s.ctid
      FROM sessions s
      WHERE s.status IN (6,8)
        AND s.created_at < _cut_sess
      LIMIT 5000
    );
    GET DIAGNOSTICS _r = ROW_COUNT;
    EXIT WHEN _r = 0;
    PERFORM pg_sleep(0.05);
  END LOOP;

  ANALYZE sms_messages;
  ANALYZE orphan_sms;
  ANALYZE sessions;
END $$;
"""

def ensure_prune_function():
    print("[prune-func] создаю/обновляю функцию prune_old_data(...)")
    exec_no_tx(PRUNE_FUNC_SQL)
    print("[prune-func] готово")

def prune_now():
    sms_days = int(os.getenv("PRUNE_SMS_DAYS", "30"))
    orphan_days = int(os.getenv("PRUNE_ORPHAN_DAYS", "14"))
    sess_days = int(os.getenv("PRUNE_SESSION_DAYS", "30"))
    print(f"[prune] sms>{sms_days}d, orphan>{orphan_days}d, sessions>{sess_days}d ...")
    with SessionLocal() as db:
        db.execute(text("SELECT prune_old_data(:s,:o,:p)"),
                   {"s": sms_days, "o": orphan_days, "p": sess_days})
        db.commit()
    print("[prune] готово")

# -------- reindex --------
REINDEX_TABLES = ["phone_numbers", "sessions", "sms_messages", "orphan_sms"]

def reindex_concurrently():
    for t in REINDEX_TABLES:
        print(f"[reindex] {t} ...")
        exec_no_tx(f"REINDEX TABLE CONCURRENTLY {t};")
    print("[reindex] готово")

# -------- cli --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", action="store_true", help="Настроить параметры таблиц и создать функцию prune")
    ap.add_argument("--prune", action="store_true", help="сейчас выполнить очистку батчами")
    ap.add_argument("--reindex", action="store_true", help="REINDEX TABLE CONCURRENTLY для ключевых таблиц")
    args = ap.parse_args()

    if args.setup:
        # Убрали create_missing_indexes(), так как этим теперь управляет Alembic
        set_table_params()
        ensure_prune_function()

    if args.prune:
        ensure_prune_function()
        prune_now()

    if args.reindex:
        reindex_concurrently()

    if not any([args.setup, args.prune, args.reindex]):
        ap.print_help()

if __name__ == "__main__":
    main()
