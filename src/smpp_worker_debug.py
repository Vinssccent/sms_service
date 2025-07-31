# -*- coding: utf-8 -*-
"""
oddbg-скрипт: однократно подключается к провайдеру и
показывает «сырой» вывод smpplib.  Завершается ^C.
"""

import threading
import logging
import time

from src import models, smpp_worker
from src.database import SessionLocal

# ►  делаем вывод smpplib максимально подробным
logging.basicConfig(level="DEBUG")

# --- берём первого провайдера из базы ---------------------------------------
db = SessionLocal()
provider = db.query(models.Provider).filter(models.Provider.id == 1).first()
db.close()

if provider is None:
    raise SystemExit("❌   Провайдер с id=1 не найден")

stop_evt = threading.Event()

try:
    smpp_worker.run_smpp_provider_loop(provider, stop_evt)
except KeyboardInterrupt:
    print("\n⏹️  Interrupted by user")
finally:
    stop_evt.set()
    time.sleep(1)

