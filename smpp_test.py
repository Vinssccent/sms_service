# /root/smpp_test.py - ФИНАЛЬНАЯ, ОТЛАЖЕННАЯ ВЕРСИЯ
import smpplib.client
import logging
import time
import sys

# --- НАСТРОЙКИ ТЕСТА ---
# Сервер запущен на той же машине, поэтому 127.0.0.1 (localhost) - это правильно.
YOUR_SERVER_IP = '127.0.0.1' 
YOUR_SERVER_PORT = 40000

# Для BIND: библиотека ожидает строки (str)
SYSTEM_ID = 'test_client_final'
PASSWORD = 'test_password_final'

# !!! ВАЖНО: ЗАМЕНИТЕ ЭТОТ НОМЕР НА АКТУАЛЬНЫЙ ИЗ ВАШЕЙ СЕССИИ !!!
# Для send_message: адрес получателя тоже должен быть строкой (str)
TARGET_PHONE_NUMBER = '+38267678799' # <-- ЗАМЕНИТЕ ЭТОТ НОМЕР!

# --- КОНЕЦ НАСТРОЕК ---

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
log = logging.getLogger()

client = smpplib.client.Client(YOUR_SERVER_IP, YOUR_SERVER_PORT, allow_unknown_opt_params=True)

try:
    log.info("--- Подключаюсь к вашему серверу... ---")
    client.connect()
    
    log.info("--- Авторизуюсь (BIND_TRANSCEIVER)... ---")
    pdu = client.bind_transceiver(system_id=SYSTEM_ID, password=PASSWORD)
    
    log.info(f"Ответ на BIND: статус={pdu.status}")
    if pdu.status != 0:
        raise Exception(f"Авторизация не удалась, сервер ответил ошибкой: {pdu.status}")
    
    log.info("--- УСПЕШНО АВТОРИЗОВАЛСЯ (BOUND_TRX) ---")

    # --- ТЕСТ №1: Отправка "правильного" СМС ---
    log.info("\n--- Отправляю СМС, которое ДОЛЖНО БЫТЬ ПРИНЯТО (SUCCESS)... ---")
    # Для send_message: адреса - строки (str), а текст сообщения - байты (bytes)
    client.send_message(
        source_addr='TikTok', 
        destination_addr=TARGET_PHONE_NUMBER,
        short_message=b'Your TikTok code is 12345',
    )
    
    resp_success = client.read_pdu()
    log.info("--- ПОЛУЧЕН ОТВЕТ НА ПЕРВОЕ СМС: ---")
    log.info(f"Статус ответа: {resp_success.status}")
    if resp_success.status != 0:
        log.warning("Ожидался статус 0 (OK), но получен другой!")

    time.sleep(2)

    # --- ТЕСТ №2: Отправка "неправильного" СМС ---
    log.info("\n--- Отправляю СМС, которое ДОЛЖНО БЫТЬ ОТВЕРГНУТО (REJECT)... ---")
    client.send_message(
        source_addr='SomeOtherService',
        destination_addr=TARGET_PHONE_NUMBER,
        short_message=b'This is a test message',
    )
    
    resp_reject = client.read_pdu()
    log.info("--- ПОЛУЧЕН ОТВЕТ НА ВТОРОЕ СМС: ---")
    log.info(f"Статус ответа: {resp_reject.status}")
    if resp_reject.status == 69:
        log.info("!!! УСПЕХ! СЕРВЕР ПРАВИЛЬНО ОТВЕТИЛ ОШИБКОЙ 69 !!!")
    else:
        log.error(f"!!! ПРОВАЛ! Ожидался статус 69 (REJECT), но получен {resp_reject.status} !!!")

except Exception as e:
    log.error(f"\n--- ОШИБКА ---")
    log.error(f"Текст ошибки: {str(e)}", exc_info=True)
    
finally:
    log.info("\n--- Завершение работы ---")
    try:
        if hasattr(client, 'state') and 'BOUND' in str(client.state):
             log.info("Отправляю UNBIND...")
             client.unbind()
             client.read_pdu()
        client.disconnect()
        log.info("Соединение закрыто.")
    except Exception as final_e:
        log.error(f"Ошибка при закрытии соединения: {final_e}")