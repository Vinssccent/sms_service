# -*- coding: utf-8 -*-
# Рабочий SMPP-сервер для smppy-0.3.x с учетом разных сигнатур вызова
import sys, os, logging
from typing import List, Union

# Добавляем корень проекта в sys.path, чтобы можно было запускать как файл
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smppy import Application, SmppClient
from src.database import SessionLocal
from src.smpp_worker import handle_incoming_sms, ESME_RINVSENDERID, ESME_ROK

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("smpp-server")


class PduAdapter:
    """Адаптер, чтобы передать данные в наш обработчик из smpp_worker."""
    def __init__(self, src: str, dst: str, txt: str):
        self._source_addr = src.encode("utf-8", errors="replace")
        self._dest_addr = dst.encode("utf-8", errors="replace")
        self._short_message = txt.encode("utf-8", errors="replace")

    @property
    def destination_addr(self): return self._dest_addr
    @property
    def source_addr(self):      return self._source_addr
    @property
    def short_message(self):    return self._short_message


class MySmppServer(Application):
    def __init__(self, name: str):
        super().__init__(name=name, logger=log)
        self.clients: List[SmppClient] = []

    # ---------------------- bind / unbind -----------------------------------
    async def handle_bound_client(self, *args, **kwargs) -> Union[SmppClient, None]:
        client = kwargs.get("client") or (args[0] if args else None)
        if client is None:
            log.warning("[BIND_TRX] client is None")
            return None
        log.info(f"[BIND_TRX] {getattr(client, 'system_id', '?')}")
        self.clients.append(client)
        return client

    async def handle_bound_receiver_client(self, *args, **kwargs):
        return await self.handle_bound_client(*args, **kwargs)

    async def handle_bound_transmitter_client(self, *args, **kwargs):
        return await self.handle_bound_client(*args, **kwargs)

    async def handle_unbound_client(self, *args, **kwargs):
        client = kwargs.get("client") or (args[0] if args else None)
        log.info(f"[UNBIND] {getattr(client, 'system_id', '?')}")
        if client in self.clients:
            self.clients.remove(client)

    # ---------------- enquire_link / generic_nack ---------------------------
    async def handle_enquire_link(self, *args, **kwargs):
        client = kwargs.get("client") or (args[0] if args else None)
        pdu    = kwargs.get("pdu")    or (args[1] if len(args) > 1 else None)
        if client and pdu:
            await client.send_pdu(pdu.make_response())

    async def handle_generic_nack(self, *args, **kwargs):
        client = kwargs.get("client") or (args[0] if args else None)
        pdu    = kwargs.get("pdu")    or (args[1] if len(args) > 1 else None)
        if client and pdu:
            # 0x00000003 = ESME_RINVCMDID
            await client.send_pdu(pdu.make_response(command_status=0x00000003))

    # ------------------------ submit_sm -------------------------------------
    async def handle_sms_received(self, *args, **kwargs):
        """
        Совместимо с вызовами smppy:
        - handle_sms_received(client=<...>, pdu=<...>)
        - handle_sms_received(client=<...>, source_number=..., dest_number=..., text=...)
        """
        client = kwargs.get("client") or (args[0] if args else None)
        pdu    = kwargs.get("pdu")    or (args[1] if len(args) > 1 else None)

        try:
            if pdu is not None:
                src = pdu.params.get(b"source_addr", b"").decode("utf-8", errors="replace")
                dst = pdu.params.get(b"destination_addr", b"").decode("utf-8", errors="replace")
                txt = pdu.params.get(b"short_message", b"").decode("utf-8", errors="replace")
            else:
                # Старые варианты вызова через именованные аргументы
                src = str(kwargs.get("source_number") or "")
                dst = str(kwargs.get("dest_number") or "")
                txt = str(kwargs.get("text") or "")

            if not src or not dst:
                log.warning("SMS ignored: missing source or destination")
                return

            log.info(f"SMPP GATEWAY: SMS '{txt}' от '{src}' к '{dst}'")

            db = SessionLocal()
            try:
                status = handle_incoming_sms(PduAdapter(src, dst, txt), db)
                # Если был оригинальный PDU — отвечаем провайдеру статусом
                if client and pdu:
                    await client.send_pdu(pdu.make_response(command_status=status))
            finally:
                db.close()

        except Exception as e:
            log.error(f"Error processing SMS: {e}")
            if client and pdu:
                try:
                    await client.send_pdu(pdu.make_response(command_status=ESME_RINVSENDERID))
                except Exception:
                    pass


if __name__ == "__main__":
    # Рекомендуемый запуск:
    #   (venv) python3 -m src.smpp_server
    # либо как файл:
    #   (venv) python3 src/smpp_server.py
    MySmppServer("MySmppServer").run(host="0.0.0.0", port=40000)
