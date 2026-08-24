import socket
import struct
from constants import *
from itch.parser import MessageParser
from itch.messages import *
from instrument_directory import InstrumentDirectory
from functools import singledispatch
import multiprocessing

HOST = ""
PORT = 30000

MSG_LEN_FORMAT = ">H"
HEADER_SIZE = 20


def receiver(raw_queue):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    mreq = socket.inet_aton("229.0.0.1") + socket.inet_aton("192.168.0.13")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
    print("Listening now...")
    while True:
        raw_queue.put(sock.recv(1500))

def processor(raw_queue):
    latest_sequence = 0

    parser = MessageParser(b"AFECXDUPQR")

    instrument_directory = InstrumentDirectory()
    orders = {}

    @singledispatch
    def handle_message(msg):
        pass

    @handle_message.register(StockDirectoryMessage)
    def _(msg):
        stock_locate = instrument_directory.symbol_for(msg.stock_locate)
        symbol = msg.decode().stock
        instrument_directory.register(stock_locate, symbol)

    @handle_message.register(AddOrderMessage)
    @handle_message.register(AddOrderNoMPIAttributionMessage)
    @handle_message.register(AddOrderMPIDAttribution)
    def _(msg):
        orders[msg.order_reference_number] = {
            "instrument": instrument_directory.symbol_for(msg.stock_locate),
            "side": msg.buy_sell_indicator,
            "price": msg.price,
            "quantity": msg.shares,
        }
        

    @handle_message.register(OrderExecutedMessage)
    def _(msg):
        current_quantity = orders[msg.order_reference_number]["quantity"]
        order_quantity = msg.executed_shares
        if order_quantity >= current_quantity:
            del orders[msg.order_reference_number]
        else:
            orders[msg.order_reference_number]["quantity"] -= msg.executed_shares

    @handle_message.register(OrderCancelMessage)
    def _(msg):
        orders[msg.order_reference_number]["quantity"] -= msg.cancelled_shares

    @handle_message.register(OrderReplaceMessage)
    def _(msg):
        old_entry = orders.pop(msg.order_reference_number)
        orders[msg.new_order_reference_number] = {
                "instrument": old_entry["instrument"],
                "side": old_entry["side"],
                "price": msg.price,
                "quantity": msg.shares,
            }

    @handle_message.register(OrderDeleteMessage)
    def _(msg):
        del orders[msg.order_reference_number]

        
    while True:
        packet = raw_queue.get()
        session_raw, sequence, count = struct.unpack(HEADER_FORMAT, packet[:HEADER_SIZE])
        if sequence != latest_sequence + 1:
            print("SOEMTHING WENT WRONG!")
            print(sequence)
            break
        latest_sequence = sequence
        messages = []
        offset = HEADER_SIZE

        for _ in range(count):
            (length,) = struct.unpack(MSG_LEN_FORMAT, packet[offset:offset + MSG_LEN_SIZE])
            offset += MSG_LEN_SIZE
            messages.append(packet[offset:offset + length])
            offset += length

        for message in messages:
            message_type = parser.get_message_type(message)
            
            handle_message(message_type)



def main():
    
    raw_queue = multiprocessing.Queue()

    receiver_process = multiprocessing.Process(target=receiver, args=(raw_queue,), daemon=True)
    processor_process = multiprocessing.Process(target=processor, args=(raw_queue,), daemon=True)

    receiver_process.start()
    processor_process.start()

    receiver_process.join()

       
if __name__ == "__main__":
    main()
