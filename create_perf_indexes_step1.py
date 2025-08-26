# create_perf_indexes_step1.py
import psycopg
from src.settings import settings

DDL = [
    # 1) Для запроса DISTINCT providers JOIN phone_numbers WHERE country & flags
    #   Покрываем фильтр (country_id, active/free) и выдаём provider_id + порядок
    """
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pn_country_provider_active_free
    ON phone_numbers (country_id, provider_id, sort_order)
    WHERE is_active IS TRUE AND is_in_use IS FALSE;
    """,

    # 2) Для выборки свободных номеров по конкретному provider_id в стране
    """
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pn_provider_country_active_free
    ON phone_numbers (provider_id, country_id, sort_order)
    WHERE is_active IS TRUE AND is_in_use IS FALSE;
    """,

    # Анализ статистики, чтобы планировщик сразу «увидел» индексы
    "ANALYZE phone_numbers;",
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
    print("OK: indexes created/analyzed")

if __name__ == "__main__":
    main()
