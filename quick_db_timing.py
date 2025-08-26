import time
from sqlalchemy import func
from src.database import SessionLocal
from src import models

def t(label, fn):
    t0=time.time()
    try:
        out=fn()
        ok=True
    except Exception as e:
        out=f"ERROR: {e}"
        ok=False
    dt=time.time()-t0
    print(f"{label}: {dt:.3f}s -> {out}")
    return ok, dt

db = SessionLocal()

# 1) Базовые count без join
t("COUNT sms_messages", lambda: db.query(func.count(models.SmsMessage.id)).order_by(None).scalar())
t("COUNT sessions",     lambda: db.query(func.count(models.Session.id)).order_by(None).scalar())
t("COUNT phone_numbers",lambda: db.query(func.count(models.PhoneNumber.id)).order_by(None).scalar())

# 2) Сложный DISTINCT как на /tools (за последние 24ч)
from datetime import datetime, timedelta
since = datetime.utcnow() - timedelta(days=1)

def count_last24_sms():
    return (db.query(func.count(models.SmsMessage.id))
              .filter(models.SmsMessage.received_at >= since)
              .order_by(None)
              .scalar())

def count_last24_unique_numbers():
    return (db.query(func.count(func.distinct(models.Session.phone_number_id)))
              .join(models.Session, models.SmsMessage.session_id == models.Session.id)
              .filter(models.SmsMessage.received_at >= since)
              .order_by(None)
              .scalar())

t("COUNT last24 sms", count_last24_sms)
t("COUNT last24 unique_numbers", count_last24_unique_numbers)

db.close()
