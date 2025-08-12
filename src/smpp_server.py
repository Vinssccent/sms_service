# src/smpp_server.py
# -*- coding: utf-8 -*-
# Финальная версия SMPP-сервера с исправленной ошибкой подключения и надежной сборкой длинных SMS.
import sys, os, logging, time
from typing import List, Union, Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smppy import Application, SmppClient
from src.database import SessionLocal
from src.smpp_worker import handle_incoming_sms, ESME_ROK

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("smpp-server")

MESSAGE_PARTS: Dict[str, Dict[str, Any]] = {}
LAST_CLEANUP_TIME = time.time()
INCOMPLETE_MESSAGE_TIMEOUT = 300  # 5 минут

def _parse_udh(udh_prefixed: bytes) -> Union[tuple, None]:
    if not udh_prefixed: return None
    try:
        udhl = udh_prefixed[0]
        header_len = udhl + 1
        if len(udh_prefixed) < header_len:
            log.warning("UDH parsing failed: header is truncated.")
            return None
        pos, end = 1, header_len
        msg_ref = total_segments = seqnum = None
        while pos + 1 < end:
            iei = udh_prefixed[pos]
            iedl = udh_prefixed[pos + 1]
            if pos + 2 + iedl > end:
                log.warning("UDH parsing failed: IE data overflows header length.")
                break
            ie_data = udh_prefixed[pos + 2 : pos + 2 + iedl]
            if iei == 0x00 and iedl == 3:
                msg_ref, total_segments, seqnum = str(ie_data[0]), ie_data[1], ie_data[2]
                break
            elif iei == 0x08 and iedl == 4:
                msg_ref = str((ie_data[0] << 8) | ie_data[1])
                total_segments, seqnum = ie_data[2], ie_data[3]
                break
            pos += 2 + iedl
        if msg_ref is not None and total_segments is not None and seqnum is not None:
            return (msg_ref, total_segments, seqnum, header_len)
        log.debug("UDH found, but not for a concatenated message.")
        return None
    except IndexError:
        log.error("UDH parsing failed due to IndexError.", exc_info=True)
        return None

def _cleanup_incomplete_messages():
    global LAST_CLEANUP_TIME
    now = time.time()
    if now - LAST_CLEANUP_TIME > 60:
        keys_to_delete = [ref for ref, data in MESSAGE_PARTS.items() if now - data['timestamp'] > INCOMPLETE_MESSAGE_TIMEOUT]
        if keys_to_delete:
            log.info(f"Running cleanup task, found {len(keys_to_delete)} timed out message parts.")
            for key in keys_to_delete:
                log.warning(f"Incomplete SMS with ref {key} timed out. Discarding parts.")
                del MESSAGE_PARTS[key]
        LAST_CLEANUP_TIME = now

class PduAdapter:
    def __init__(self, src: str, dst: str, txt: str):
        self._source_addr = src.encode("utf-8", errors="replace")
        self._dest_addr = dst.encode("utf-8", errors="replace")
        self._short_message = txt.encode("utf-8", errors="replace")
    @property
    def destination_addr(self): return self._dest_addr
    @property
    def source_addr(self): return self._source_addr
    @property
    def short_message(self): return self._short_message

class MySmppServer(Application):
    def __init__(self, name: str):
        super().__init__(name=name, logger=log)
        self.clients: List[SmppClient] = []

    async def handle_bound_client(self, *args, **kwargs) -> Union[SmppClient, None]:
        client = kwargs.get("client") or (args[0] if args else None)
        if client is None:
            log.warning("[BIND] client is None")
            return None
        log.info(f"[BIND] {getattr(client, 'system_id', '?')}")
        if client not in self.clients:
            self.clients.append(client)
        return client

    async def handle_bound_receiver_client(self, *args, **kwargs): return await self.handle_bound_client(*args, **kwargs)
    async def handle_bound_transmitter_client(self, *args, **kwargs): return await self.handle_bound_client(*args, **kwargs)

    async def handle_unbound_client(self, *args, **kwargs):
        client = kwargs.get("client") or (args[0] if args else None)
        if client:
            log.info(f"[UNBIND] {getattr(client, 'system_id', '?')}")
            if client in self.clients: self.clients.remove(client)

    async def handle_enquire_link(self, *args, **kwargs):
        client = kwargs.get("client") or (args[0] if args else None)
        pdu    = kwargs.get("pdu")    or (args[1] if len(args) > 1 else None)
        if client and pdu: await client.send_pdu(pdu.make_response())

    async def handle_generic_nack(self, *args, **kwargs):
        client = kwargs.get("client") or (args[0] if args else None)
        pdu    = kwargs.get("pdu")    or (args[1] if len(args) > 1 else None)
        if client and pdu: await client.send_pdu(pdu.make_response(command_status=0x00000003))

    async def handle_sms_received(self, *args, **kwargs):
        _cleanup_incomplete_messages()
        client = kwargs.get("client") or (args[0] if args else None)
        pdu    = kwargs.get("pdu")    or (args[1] if len(args) > 1 else None)
        try:
            full_text = None
            src = ""
            dst = ""
            if pdu is not None and hasattr(pdu, 'params'):
                params = pdu.params
                src = params.get(b"source_addr", b"").decode("ascii", "replace").strip("\x00")
                dst = params.get(b"destination_addr", b"").decode("ascii", "replace").strip("\x00")
                short_message_bytes = params.get(b'short_message', b'')
                data_coding = params.get(b'data_coding', b'\x00')[0]
                esm_class = params.get(b'esm_class', b'\x00')[0]
                if (esm_class & 0x40):
                    udh_info = _parse_udh(short_message_bytes)
                    if udh_info:
                        msg_ref, total_segments, segment_seqnum, udh_total_len = udh_info
                        storage_key = f"{src}-{msg_ref}"
                        log.info(f"Received part {segment_seqnum}/{total_segments} for message ref {storage_key}.")
                        part_bytes = short_message_bytes[udh_total_len:]
                        if storage_key not in MESSAGE_PARTS:
                            MESSAGE_PARTS[storage_key] = {'parts': {}, 'total': total_segments, 'timestamp': time.time(), 'dc': data_coding}
                        MESSAGE_PARTS[storage_key]['parts'][segment_seqnum] = part_bytes
                        bucket = MESSAGE_PARTS[storage_key]
                        if len(bucket['parts']) == bucket['total']:
                            log.info(f"All parts for message ref {storage_key} received. Reassembling...")
                            ordered_parts = [bucket['parts'].get(i) for i in range(1, bucket['total'] + 1)]
                            if all(part is not None for part in ordered_parts):
                                full_message_bytes = b"".join(ordered_parts)
                                final_dc = bucket['dc']
                                if final_dc == 8:
                                    if len(full_message_bytes) % 2 == 1: full_message_bytes += b'\x00'
                                    full_text = full_message_bytes.decode('utf-16-be', errors='replace')
                                else:
                                    full_text = full_message_bytes.decode('latin-1', errors='replace')
                            else:
                                log.error(f"Failed to assemble message {storage_key}: missing parts. Discarding.")
                            del MESSAGE_PARTS[storage_key]
                    else:
                        udhl = short_message_bytes[0] + 1
                        payload = short_message_bytes[udhl:]
                        full_text = payload.decode('latin-1', errors='replace')
                else:
                    if data_coding == 8:
                        full_text = short_message_bytes.decode('utf-16-be', errors='replace')
                    else:
                        full_text = short_message_bytes.decode('latin-1', errors='replace')
            else:
                src = str(kwargs.get("source_number") or "")
                dst = str(kwargs.get("dest_number") or "")
                full_text = str(kwargs.get("text") or "")
            if full_text is not None:
                full_text = full_text.replace('\x00', '').strip()
                if not src or not dst:
                    log.warning("SMS ignored: missing source or destination")
                    if client and pdu: await client.send_pdu(pdu.make_response(command_status=ESME_ROK))
                    return
                clean_text = full_text.replace('\n', ' ').replace('\r', '')
                log.info(f"SMPP GATEWAY: FULL SMS '{clean_text}' from '{src}' to '{dst}'")
                db = SessionLocal()
                try:
                    status = handle_incoming_sms(PduAdapter(src, dst, full_text), db)
                    if client and pdu:
                        await client.send_pdu(pdu.make_response(command_status=status))
                finally:
                    db.close()
            else:
                log.info("Stored SMS part. Waiting for the rest.")
                if client and pdu:
                    await client.send_pdu(pdu.make_response(command_status=ESME_ROK))
        except Exception as e:
            log.error(f"Critical error processing SMS: {e}", exc_info=True)
            if client and pdu:
                try:
                    await client.send_pdu(pdu.make_response(command_status=ESME_ROK))
                except Exception as send_err:
                    log.error(f"Failed to send error response: {send_err}")

if __name__ == "__main__":
    server = MySmppServer("MySmppServer")
    log.info(f"Starting {server.name} on 0.0.0.0:40000...")
    server.run(host="0.0.0.0", port=40000)