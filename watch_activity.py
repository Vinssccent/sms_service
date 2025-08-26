import time, datetime
from src.settings import settings
import psycopg

conn = psycopg.connect(
    dbname=settings.DB_NAME,
    user=settings.DB_USER,
    password=settings.DB_PASSWORD,
    host=settings.DB_HOST,
    port=str(settings.DB_PORT),
    autocommit=True,
)
cur = conn.cursor()

print("Watching pg_stat_activity... (Ctrl+C to stop)")
while True:
    cur.execute("""
        SELECT pid, state, wait_event_type, wait_event,
               EXTRACT(EPOCH FROM (now()-query_start)) AS age_s,
               LEFT(regexp_replace(query, '\\s+', ' ', 'g'), 200) AS q
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND state <> 'idle'
        ORDER BY age_s DESC
        LIMIT 10;
    """)
    rows = cur.fetchall()
    print("---", datetime.datetime.now().isoformat(), f"active={len(rows)} ---")
    for pid, state, wet, we, age, q in rows:
        print(f"pid={pid} state={state} wait={wet}/{we} age={age:.3f}s q={q}")
    time.sleep(1.0)
