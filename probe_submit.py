#!/usr/bin/env python3
import argparse, logging, socket, time
from smpplib import client, smpp

def read_submit_resp(c, per_try_timeout=5):
    """Ждём именно submit_sm_resp с таймаутом per_try_timeout сек."""
    end = time.time() + per_try_timeout
    while time.time() < end:
        r = c.read_pdu()
        if r and getattr(r, "command", "") == "submit_sm_resp":
            return r
    raise socket.timeout("wait submit_sm_resp timeout")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--sysid", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--src", default="TikTok")
    ap.add_argument("--file", required=True)
    ap.add_argument("--timeout", type=float, default=10)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO)

    c = client.Client(args.host, args.port)
    c.connect()
    # В smpplib сокет в _socket:
    try:
        c._socket.settimeout(args.timeout)
    except Exception:
        pass

    # Bind как TRANSMITTER (для отправки submit_sm)
    c.bind_transmitter(
        system_id=args.sysid.encode("ascii", "ignore"),
        password=args.password.encode("ascii", "ignore"),
        system_type=b"",
        interface_version=0x34,
    )
    print("BIND TX OK")

    with open(args.file) as f:
        numbers = [ln.strip() for ln in f if ln.strip()]

    for msisdn in numbers:
        # Формируем PDU через make_pdu и указываем client=c (иначе будет ошибка с default_client)
        p = smpp.make_pdu(
            "submit_sm",
            client=c,
            source_addr_ton=0x05, source_addr_npi=0x00,
            dest_addr_ton=0x01, dest_addr_npi=0x01,
            source_addr=args.src.encode("ascii", "ignore"),
            destination_addr=msisdn.encode("ascii", "ignore"),
            short_message=b"probe",
            registered_delivery=0,
        )
        c.send_pdu(p)

        try:
            resp = read_submit_resp(c, per_try_timeout=args.timeout)
            status = getattr(resp, "status", None)
            try:
                msg_id = (resp.message_id or b"").decode("ascii", "ignore")
            except Exception:
                msg_id = ""
            print(f"{msisdn}\tstatus={status}\tmsg_id={msg_id}")
        except socket.timeout:
            print(f"{msisdn}\tstatus=TIMEOUT\tmsg_id=")

    # аккуратно закрываемся
    try: c.unbind()
    except Exception: pass
    try: c.disconnect()
    except Exception: pass

if __name__ == "__main__":
    main()
