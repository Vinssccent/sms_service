# db_introspect.py
import os, sys, traceback
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL

def main():
    # Загружаем .env
    try:
        from dotenv import load_dotenv
        load_dotenv(".env")
        print("ENV loaded from .env")
    except Exception:
        print("WARN: python-dotenv не установлен — продолжаю")

    # Берём настройки из проекта
    sys.path.insert(0, os.getcwd())
    from src.settings import settings

    url_obj = URL.create(
        drivername="postgresql+psycopg",
        username=str(settings.DB_USER).strip(),
        password=str(settings.DB_PASSWORD).strip(),
        host=str(settings.DB_HOST).strip(),
        port=int(settings.DB_PORT),
        database=str(settings.DB_NAME).strip(),
    )

    masked = f"postgresql+psycopg://{settings.DB_USER}:***@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    print(f"\nDetected DATABASE_URL: {masked}")

    connect_args = {}
    if str(settings.DB_HOST).strip() in {"127.0.0.1", "localhost"}:
        connect_args["sslmode"] = "disable"

    try:
        engine = create_engine(url_obj, future=True, connect_args=connect_args)
        insp = inspect(engine)

        print("\nТаблицы в БД:")
        tables = sorted(insp.get_table_names())
        for t in tables:
            print(f"  - {t}")

        print("\nПодробно по таблицам:")
        for t in tables:
            print(f"\n== {t} ==")
            cols = insp.get_columns(t)
            for c in cols:
                nullable = "NULL" if c.get("nullable", True) else "NOT NULL"
                default = c.get("default")
                print(f"  {c['name']}  {c.get('type')}  {nullable}  default={default}")

            try:
                idx = insp.get_indexes(t)
                for i in idx:
                    print(f"  [INDEX] {i.get('name')} -> {i.get('column_names')} unique={i.get('unique')}")
            except Exception:
                pass

            try:
                pks = insp.get_pk_constraint(t)
                if pks and pks.get("constrained_columns"):
                    print(f"  [PK] {pks['constrained_columns']}")
            except Exception:
                pass

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("\nБД соединение: OK")
    except Exception:
        print("\n======== Ошибка интроспекции БД ========")
        traceback.print_exc()

if __name__ == "__main__":
    main()
