from itch.parser import MessageParser

parser = MessageParser()
with open('data/01302019.NASDAQ_ITCH50', 'rb') as f:
    for msg in parser.parse_file(f):
        if getattr(msg, 'order_reference_number', None) == 5966:
            if msg.message_type == b'A':
                print(f"ADD: shares={msg.shares}, price={msg.decode_price('price')}")
            elif msg.message_type == b'E':
                print(f"EXECUTED: executed_shares={msg.executed_shares}, match_number={msg.match_number}")
            elif msg.message_type == b'D':
                print("DELETE: (no shares field on this message type)")