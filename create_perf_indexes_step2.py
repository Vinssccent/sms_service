# create_perf_indexes_step2.py
import psycopg
from src.settings import settings

DDL = [
    # Ускоряем JOIN по (phone_number_id, service_id) и даём покрытие для полей выборки/фильтра
    """
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pnu_phone_service_include
    ON phone_number_usage (phone_number_id, service_id) INCLUDE (usage_count, last_used_at);
    """,
    "ANALYZE phone_number_usage;",
]

def main():
    conn = psycopg.connect(
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        host=settings.DB_HOST,
        port=str(settings.DB_PORT),
        autocommit=True,
    )
    cur = conn.cursor()
    for sql in DDL:
        print(">>>", sql.strip().splitlines()[0])
        cur.execute(sql)
    cur.close(); conn.close()
    print("OK: index created & analyzed")

if __name__ == "__main__":
    main()

