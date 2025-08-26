# smpp_test_orphan_v2.py
import smpplib.client

HOST = "127.0.0.1"
PORT = 40000

got = {"done": False}

def on_submit_sm_resp(resp_pdu, **kwargs):
    # Это именно ответ сервера
    print(
        "submit_sm_resp:",
        "status=", getattr(resp_pdu, "status", None),
        "command_status=", getattr(resp_pdu, "command_status", None),
        "command=", getattr(resp_pdu, "command", None),
    )
    got["done"] = True

c = smpplib.client.Client(HOST, PORT)
c.connect()
c.bind_transceiver(system_id=b"test", password=b"")

# Повесим хендлер на ответ
c.set_message_sent_handler(on_submit_sm_resp)

# Отправляем «живой» трафик (должно вернуться 69)
c.send_message(
    source_addr=b"TESTSENDER",
    destination_addr=b"+79001234567",
    short_message=b"Your code is 1234",
)

# Крутим чтение, пока не придёт submit_sm_resp
for _ in range(30):
    c.read_once()
    if got["done"]:
        break

c.unbind()
c.disconnect()
