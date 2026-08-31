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
from ring_buffer import *
from multiprocessing import shared_memory
from order_book import consumer
from logger import logger

HOST = ""
PORT = 30000

MSG_LEN_FORMAT = ">H"
HEADER_SIZE = 20



def receiver(raw_queue):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    mreq = socket.inet_aton("229.0.0.1") + socket.inet_aton(IP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
    print("Listening now...")
    while True:
        packet = sock.recv(1500)
        raw_queue.put((time.time_ns(), packet))

def processor(raw_queue, shm_name, capacity, instrument_map):

    shm = shared_memory.SharedMemory(name=shm_name, create=False)
    ring = Ring(shm, capacity)

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
        instrument_map[stock_locate] = symbol
        return []

    @handle_message.register(AddOrderMessage)
    @handle_message.register(AddOrderNoMPIAttributionMessage)
    @handle_message.register(AddOrderMPIDAttribution)
    def _(msg, sequence, ts_recv):
        orders[msg.order_reference_number] = msg.buy_sell_indicator
        
        while not (ring.write(
            (Action.ADD, 
             msg.timestamp, 
             ts_recv, sequence, 
             msg.order_reference_number, 
             msg.shares, 
             Side.BID if msg.buy_sell_indicator == b"B" else Side.ASK, 
             msg.stock_locate, 
             msg.price)
        )):
            pass
        

    @handle_message.register(OrderExecutedMessage)
    def _(msg, sequence, ts_recv):

        while not( ring.write(
                    (Action.EXECUTE, 
                     msg.timestamp, 
                     ts_recv, sequence, 
                     msg.order_reference_number, 
                     msg.executed_shares, 
                     255, # no bid/side ask for executed message
                     msg.stock_locate, 
                     -1) # no price for executed message
                )):
            pass             

    @handle_message.register(OrderCancelMessage)
    def _(msg, sequence, ts_recv):
        while not ( ring.write(
                    (Action.CANCEL, 
                    msg.timestamp, 
                    ts_recv, sequence, 
                    msg.order_reference_number, 
                    msg.cancelled_shares, 
                    255, # no bid/side ask for cancelled message
                    msg.stock_locate, 
                    -1) # no price for cancelled message
                )):
            pass
    
 
    @handle_message.register(OrderReplaceMessage)
    def _(msg, sequence, ts_recv):

        side = orders[msg.order_reference_number]
        orders[msg.new_order_reference_number] = side
        del orders[msg.order_reference_number]
        while not (ring.write(
                        (Action.R_CANCEL, 
                            msg.timestamp, 
                            ts_recv, sequence, 
                            msg.order_reference_number, 
                            0, # no old quantity for replaced-cancelled message 
                            Side.BID if side == b"B" else Side.ASK, # no bid/side ask for replaced-cancelled message
                            msg.stock_locate, 
                            0) # no price for replaced-cancelled message
                    )):
            pass
       
        while not (ring.write(
                            (Action.R_ADD, 
                             msg.timestamp, 
                             ts_recv, sequence, 
                             msg.new_order_reference_number, 
                             msg.shares, 
                             Side.BID if side == b"B" else Side.ASK, # no bid/side ask for replaced-cancelled message
                             msg.stock_locate, 
                             msg.price)
                        )):
            pass
        

    @handle_message.register(OrderDeleteMessage)
    def _(msg, sequence, ts_recv):
        
        del orders[msg.order_reference_number]
        while not (ring.write(
                    (Action.DELETE, 
                        msg.timestamp, 
                        ts_recv, sequence, 
                        msg.order_reference_number, 
                        0, # no quantity for deleted orders 
                        255, # no bid/side ask for deleted orders
                        msg.stock_locate, 
                        0) # no price for deleted orders
                )):
            pass
       

    
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
            handle_message(message_type, msg_sequence, ts_recv)

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
    manager = multiprocessing.Manager()
    instrument_map = manager.dict()
    capacity = 262144  
    shm_size =  8 + 8 + (MAX_CONSUMERS * 8) + MAX_CONSUMERS + (capacity * EVENT.itemsize)
    shm = shared_memory.SharedMemory(create=True, size=shm_size)
    shm.buf[:] = b'\x00' * shm_size
    raw_queue = multiprocessing.Queue()

    receiver_process = multiprocessing.Process(target=receiver, args=(raw_queue,), daemon=True)
    processor_process = multiprocessing.Process(
        target=processor, 
        args=(raw_queue, shm.name, capacity, instrument_map)
    )
    consumer_process = multiprocessing.Process(
        target=consumer, 
        args=(shm.name, capacity, instrument_map)
    )
    logger_process = multiprocessing.Process(
        target=logger,
        args=(shm.name, capacity, instrument_map)
    )

    receiver_process.start()
    processor_process.start()
    consumer_process.start()
    logger_process.start()
    receiver_process.join()

       
if __name__ == "__main__":
    main()
