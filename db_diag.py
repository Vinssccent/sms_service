cat > db_diag.py << 'PY'
import os, psycopg
from textwrap import shorten

def env(k, d=None): return os.environ.get(k, d)

conn = psycopg.connect(
    dbname=env("DB_NAME"),
    user=env("DB_USER"),
    password=env("DB_PASSWORD"),
    host=env("DB_HOST","127.0.0.1"),
    port=env("DB_PORT","5432"),
    autocommit=True,
)

cur = conn.cursor()

print("=== VERSION / SETTINGS ===")
for row in cur.execute("select version(), current_setting('max_connections')"):
    print("version:", row[0].splitlines()[0])
    print("max_connections:", row[1])

print("\n=== ACTIVE QUERIES (> 1s) ===")
q = """
select pid, state, wait_event_type, wait_event,
       now()-query_start as age, shorten(query,200) as q
from (
  select *, left(regexp_replace(query, '\\s+', ' ', 'g'), 200) as shorten
  from pg_stat_activity
) t
where datname = current_database()
  and state <> 'idle'
  and now()-query_start > interval '1 second'
order by age desc
limit 20;
"""
for row in cur.execute(q):
    print(row)

print("\n=== TOP TABLE SIZES ===")
q = """
select c.relname,
       pg_total_relation_size(c.oid) as size_bytes,
       coalesce(s.n_live_tup,0) as rows_live
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
left join pg_stat_user_tables s on s.relname = c.relname
where n.nspname='public' and c.relkind='r'
order by size_bytes desc
limit 10;
"""
for r in cur.execute(q):
    print(r)

print("\n=== TABLE SCANS (low idx_scan may be bad) ===")
q = """
select relname, seq_scan, idx_scan, n_live_tup
from pg_stat_user_tables
order by n_live_tup desc
limit 10;
"""
for r in cur.execute(q):
    print(r)

print("\n=== LOCKS (if any) ===")
q = """
select locktype, relation::regclass, mode, granted, pid
from pg_locks l
join pg_stat_activity a using (pid)
where a.datname = current_database()
order by granted asc;
"""
for r in cur.execute(q):
    print(r)

print("\n=== EXPLAIN for COUNTs (no ANALYZE) ===")
for stmt in [
    "EXPLAIN SELECT COUNT(*) FROM sms_messages",
    "EXPLAIN SELECT COUNT(*) FROM sessions",
    "EXPLAIN SELECT COUNT(*) FROM phone_numbers",
]:
    print("--", stmt)
    for r in cur.execute(stmt):
        print(r[0])

cur.close(); conn.close()
PY

source venv/bin/activate
python db_diag.py
