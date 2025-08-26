import smpplib.client, smpplib.consts
c = smpplib.client.Client('127.0.0.1', 40000)
c.connect()
c.bind_transceiver(system_id=b'test', password=b'')
pdu = c.send_message(
    source_addr_ton=smpplib.consts.SMPP_TON_ALNUM,
    source_addr=b'TESTSENDER',
    dest_addr_ton=smpplib.consts.SMPP_TON_INTL,
    destination_addr=b'+79001234567',
    short_message=b'Your code is 1234',
)
print("submit_sm_resp status:", pdu.status)  # Ожидаем: 69
c.unbind(); c.disconnect()
