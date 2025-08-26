import time
import smpplib.client
from smpplib import exceptions

HOST, PORT = "127.0.0.1", 40000
DONE = {"ok": False}

def on_submit_sm_resp(pdu=None, **kwargs):
    # pdu — это ИМЕННО submit_sm_resp от сервера
    print(
        "submit_sm_resp:",
        "status=", getattr(pdu, "status", None),
        "command_status=", getattr(pdu, "command_status", None),
        "command=", getattr(pdu, "command", None),
        "message_id=", getattr(pdu, "message_id", None),
    )
    DONE["ok"] = True

c = smpplib.client.Client(HOST, PORT)
c.connect()
c.bind_transceiver(system_id=b"test", password=b"")

# Повесим обработчик именно на ответ submit_sm_resp
c.set_message_sent_handler(on_submit_sm_resp)

# Отправляем СМС на НАШ номер — ожидаем submit_sm_resp со статусом 0
c.send_message(
    source_addr=b"TESTSENDER",
    destination_addr=b"+79001234567",
    short_message=b"Code 5678",
)

# Пытаемся дочитать ответ
for _ in range(50):
    try:
        c.read_once()
    except exceptions.PDUError as e:
        # Если вдруг код != 0, библиотека кинет исключение — напечатаем и выйдем
        print("PDUError:", e)
        break
    if DONE["ok"]:
        break
    time.sleep(0.05)

c.unbind()
c.disconnect()
