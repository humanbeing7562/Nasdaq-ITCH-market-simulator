import socket
import struct
from constants import *
from itch.parser import MessageParser
from itch.messages import *
from instrument_directory import InstrumentDirectory
from functools import singledispatch
import multiprocessing
from events import *
import time
import threading


HOST = ""
PORT = 30000

MSG_LEN_FORMAT = ">H"
HEADER_SIZE = 20
from datetime import datetime
    

def receiver(raw_queue):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    mreq = socket.inet_aton("229.0.0.1") + socket.inet_aton("192.168.0.6")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
    print("Listening now...")
    expected = 1
    while True:
        packet = sock.recv(1500)
        seq = struct.unpack_from(">10xQ", packet, 0)[0]
        if seq == 51 and seq > expected :
            print(f"RECEIVER SAW GAP: expected {expected}, got {seq} (missing {seq - expected})")
        expected = max(expected, seq + 1)
        raw_queue.put((time.time_ns(), packet))

def processor(raw_queue):

    def retransmit_listener(sock, raw_queue):
            while True:
                packet, addr = sock.recvfrom(65536)
                ts_recv = time.time_ns()
                raw_queue.put((ts_recv, packet))
    
    latest_sequence = 0

    parser = MessageParser(b"AFECXDUPQR")

    instrument_directory = InstrumentDirectory()
    orders = {}

    @singledispatch
    def handle_message(msg, sequence, ts_recv):
        return []

    @handle_message.register(StockDirectoryMessage)
    def _(msg, sequence, ts_recv):
        stock_locate = msg.stock_locate
        symbol = msg.decode().stock
        instrument_directory.register(stock_locate, symbol)
        return []

    @handle_message.register(AddOrderMessage)
    @handle_message.register(AddOrderNoMPIAttributionMessage)
    @handle_message.register(AddOrderMPIDAttribution)
    def _(msg, sequence, ts_recv):
        instrument = instrument_directory.symbol_for(msg.stock_locate)
        orders[msg.order_reference_number] = {
            "instrument": instrument,
            "side": msg.buy_sell_indicator,
            "price": msg.price,
            "quantity": msg.shares,
        }
        event = Event("ADD", msg.timestamp, ts_recv, sequence, msg.order_reference_number, msg.shares, msg.buy_sell_indicator, instrument, msg.price)
        return [event]
        

    @handle_message.register(OrderExecutedMessage)
    def _(msg, sequence, ts_recv):
        current_quantity = orders[msg.order_reference_number]["quantity"]
        order_quantity = msg.executed_shares
        side = orders[msg.order_reference_number]["side"]
        price = orders[msg.order_reference_number]["price"]
        if order_quantity >= current_quantity:
            del orders[msg.order_reference_number]
        else:
            orders[msg.order_reference_number]["quantity"] -= msg.executed_shares
        instrument = instrument_directory.symbol_for(msg.stock_locate)
        event = Event("EXECUTE", msg.timestamp, ts_recv, sequence, msg.order_reference_number, order_quantity, side, instrument, price)
        return [event]

    @handle_message.register(OrderCancelMessage)
    def _(msg, sequence, ts_recv):
        instrument = instrument_directory.symbol_for(msg.stock_locate)
        orders[msg.order_reference_number]["quantity"] -= msg.cancelled_shares
        event = Event("CANCEL", msg.timestamp, ts_recv, sequence, msg.order_reference_number, msg.cancelled_shares, orders[msg.order_reference_number]["side"], instrument, orders[msg.order_reference_number]["price"])
        return [event]
 
    @handle_message.register(OrderReplaceMessage)
    def _(msg, sequence, ts_recv):
        old_entry = orders.pop(msg.order_reference_number)
        instrument = instrument_directory.symbol_for(msg.stock_locate)
        orders[msg.new_order_reference_number] = {
                "instrument": old_entry["instrument"],
                "side": old_entry["side"],
                "price": msg.price,
                "quantity": msg.shares,
            }
        event_cancel = Event("R-CANCEL", msg.timestamp, ts_recv, sequence, msg.order_reference_number, old_entry["quantity"], old_entry["side"], instrument, old_entry["price"])
        event_add = Event("R-ADD", msg.timestamp, ts_recv, sequence, msg.new_order_reference_number, msg.shares, old_entry["side"], instrument, msg.price)
        return [event_cancel, event_add]
        

    @handle_message.register(OrderDeleteMessage)
    def _(msg, sequence, ts_recv):
        instrument = instrument_directory.symbol_for(msg.stock_locate)
        event = Event("DELETE", msg.timestamp, ts_recv, sequence, msg.order_reference_number, orders[msg.order_reference_number]["quantity"], 
                      orders[msg.order_reference_number]["side"], instrument, orders[msg.order_reference_number]["price"])
        
        del orders[msg.order_reference_number]
        return [event]

    
    expected_sequence = 1

    def parse_and_apply(sequence, count, packet, ts_recv, offset):
        nonlocal expected_sequence
        messages = []
        pos = HEADER_SIZE
        for _ in range(count):
            (length,) = struct.unpack(MSG_LEN_FORMAT, packet[pos:pos + MSG_LEN_SIZE])
            pos += MSG_LEN_SIZE
            messages.append(packet[pos:pos + length])
            pos += length

        for i, message in enumerate(messages[offset:], start=offset):
            msg_sequence = sequence + i
            message_type = parser.get_message_type(message)
            for event in handle_message(message_type, msg_sequence, ts_recv):
                pass

        expected_sequence = sequence + count

    REQUEST_FORMAT = '>10sQH'  
    def build_request(session, sequence, count):
        return struct.pack(REQUEST_FORMAT, session, sequence, count)

    def true_gap_extent(expected_sequence, sequence, pending):
        probe = expected_sequence
        while probe < sequence and probe not in pending:
            probe += 1
        return probe - expected_sequence
    
    requested = set()
    MAX_PENDING = 1024
    pending = {}
    retransmit_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    retransmit_sock.bind(('', 0))
    threading.Thread(target=retransmit_listener, args=(retransmit_sock, raw_queue), daemon=True).start()
    while True:
        ts_recv, packet = raw_queue.get()

        session_raw, sequence, count = struct.unpack(HEADER_FORMAT, packet[:HEADER_SIZE])

        if sequence + count <= expected_sequence:
            # does this actually happen?
            continue
        
        if sequence > expected_sequence:
            # make a tcp retransmission request on missing sequence
            gap_count = true_gap_extent(expected_sequence, sequence, pending)
            if expected_sequence not in requested:
                print(f"MAKING REQUEST FOR {expected_sequence}")
                request = build_request(session_raw, expected_sequence, gap_count)
                retransmit_sock.sendto(request, (IP, RETRANSMIT_PORT))
                requested.update(range(expected_sequence, expected_sequence + gap_count))
            pending[sequence] = (ts_recv, packet)
            continue

        offset = expected_sequence - sequence
        parse_and_apply(sequence, count, packet, ts_recv, offset)
        while expected_sequence in pending:
            buf_ts_recv, buf_packet = pending.pop(expected_sequence)
            _, buf_seq, buf_count = struct.unpack(HEADER_FORMAT, buf_packet[:HEADER_SIZE])
            parse_and_apply(buf_seq, buf_count, buf_packet, buf_ts_recv, offset=0)

    
def main():
    
    raw_queue = multiprocessing.Queue()

    receiver_process = multiprocessing.Process(target=receiver, args=(raw_queue,), daemon=True)
    processor_process = multiprocessing.Process(target=processor, args=(raw_queue,), daemon=True)

    receiver_process.start()
    processor_process.start()

    receiver_process.join()

       
if __name__ == "__main__":
    main()
