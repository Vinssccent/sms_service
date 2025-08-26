# psycopg_diag.py
import os
from psycopg import connect

user = os.getenv("DB_USER") or "sms_user"
pwd  = os.getenv("DB_PASSWORD") or "2517grot"
host = os.getenv("DB_HOST") or "127.0.0.1"
port = int(os.getenv("DB_PORT") or "5432")
db   = os.getenv("DB_NAME") or "sms_db"

print("Trying psycopg.connect(...) directly...")
conn = connect(host=host, port=port, dbname=db, user=user, password=pwd, connect_timeout=5)
with conn.cursor() as cur:
    cur.execute("SELECT version(), current_user, current_database();")
    print(cur.fetchone())
conn.close()
print("OK")
