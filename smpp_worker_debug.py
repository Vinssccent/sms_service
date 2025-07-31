from src import models, smpp_worker
import threading, os
from src.database import SessionLocal

db = SessionLocal()
provider = db.query(models.Provider).filter(models.Provider.id == 1).first()
stop = threading.Event()
smpp_worker.run_smpp_provider_loop(provider, stop)
