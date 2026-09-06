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
from trade_relay import trade_relay
from ws_server import ws_server
import gc

HOST = ""
PORT = 30000

MSG_LEN_FORMAT = ">H"
HEADER_SIZE = 20

def create_or_replace_shm(name, size):
    try:
        old = shared_memory.SharedMemory(name=name, create=False)
        old.close()
        old.unlink()
        time.sleep(0.5)
    except Exception:
        pass
    return shared_memory.SharedMemory(name=name, create=True, size=size)

def get_shm(name, size):
    try:
        shm = shared_memory.SharedMemory(name=name, create=True, size=size)
    except FileExistsError:
        shm = shared_memory.SharedMemory(name=name, create=False)
        if shm.size < size:
            shm.close()
            shm.unlink()
            time.sleep(0.5)
            shm = shared_memory.SharedMemory(name=name, create=True, size=size)
    shm.buf[:size] = b'\x00' * size
    return shm

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

    orders = {}
    instrument_directory = InstrumentDirectory()

    # message type bytes
    TYPE_R = ord('R')  # stock directory
    TYPE_A = ord('A')  # add order
    TYPE_F = ord('F')  # add order MPID
    TYPE_E = ord('E')  # order executed
    TYPE_C = ord('C')  # order executed with price
    TYPE_X = ord('X')  # order cancel
    TYPE_D = ord('D')  # order delete
    TYPE_U = ord('U')  # order replace

    def handle_message(msg, msg_seq, ts_recv):
        msg_type = msg[0]
        stock_locate = int.from_bytes(msg[1:3], 'big')
        timestamp = int.from_bytes(msg[5:11], 'big')

        if msg_type == TYPE_R:
            symbol = msg[11:19].decode('ascii').strip()
            instrument_directory.register(stock_locate, symbol)
            instrument_map[stock_locate] = symbol

        elif msg_type == TYPE_A or msg_type == TYPE_F:
            order_ref = struct.unpack('>Q', msg[11:19])[0]
            buy_sell = msg[19:20]
            shares = struct.unpack('>I', msg[20:24])[0]
            price = struct.unpack('>I', msg[32:36])[0]
            orders[order_ref] = (buy_sell, price)
            while not ring.write((
                Action.ADD, timestamp, ts_recv, msg_seq, order_ref, shares,
                Side.BID if buy_sell == b'B' else Side.ASK, stock_locate, price
            )):
                pass

        elif msg_type == TYPE_E:
            order_ref = struct.unpack('>Q', msg[11:19])[0]
            executed_shares = struct.unpack('>I', msg[19:23])[0]
            side, price = orders[order_ref]
            while not ring.write((
                Action.EXECUTE, timestamp, ts_recv, msg_seq, order_ref, executed_shares,
                Side.BID if side == b'B' else Side.ASK, stock_locate, price
            )):
                pass

        elif msg_type == TYPE_C:
            order_ref = struct.unpack('>Q', msg[11:19])[0]
            executed_shares = struct.unpack('>I', msg[19:23])[0]
            exec_price = struct.unpack('>I', msg[32:36])[0]
            while not ring.write((
                Action.EXECUTE, timestamp, ts_recv, msg_seq, order_ref, executed_shares,
                255, stock_locate, exec_price
            )):
                pass

        elif msg_type == TYPE_X:
            order_ref = struct.unpack('>Q', msg[11:19])[0]
            cancelled_shares = struct.unpack('>I', msg[19:23])[0]
            while not ring.write((
                Action.CANCEL, timestamp, ts_recv, msg_seq, order_ref, cancelled_shares,
                255, stock_locate, -1
            )):
                pass

        elif msg_type == TYPE_D:
            order_ref = struct.unpack('>Q', msg[11:19])[0]
            del orders[order_ref]
            while not ring.write((
                Action.DELETE, timestamp, ts_recv, msg_seq, order_ref, 0,
                255, stock_locate, 0
            )):
                pass

        elif msg_type == TYPE_U:
            old_ref = struct.unpack('>Q', msg[11:19])[0]
            new_ref = struct.unpack('>Q', msg[19:27])[0]
            shares = struct.unpack('>I', msg[27:31])[0]
            price = struct.unpack('>I', msg[31:35])[0]
            side, _ = orders[old_ref]
            orders[new_ref] = (side, price)
            del orders[old_ref]
            side_val = Side.BID if side == b'B' else Side.ASK
            while not ring.write((
                Action.R_CANCEL, timestamp, ts_recv, msg_seq, old_ref, 0,
                side_val, stock_locate, 0
            )):
                pass
            while not ring.write((
                Action.R_ADD, timestamp, ts_recv, msg_seq, new_ref, shares,
                side_val, stock_locate, price
            )):
                pass

    def parse_and_apply(sequence, count, packet, ts_recv, offset):
        nonlocal expected_sequence
        pos = HEADER_SIZE
        for i in range(count):
            (length,) = struct.unpack(MSG_LEN_FORMAT, packet[pos:pos + MSG_LEN_SIZE])
            pos += MSG_LEN_SIZE
            msg = packet[pos:pos + length]
            pos += length

            if i < offset:
                continue

            handle_message(msg, sequence + i, ts_recv)

        expected_sequence = sequence + count

    expected_sequence = 1

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

        if expected_sequence % 50000 < 6:
            cursor_count = int(ring.consumer_count[0])
            cursors = [(i, int(ring.cursors[i]), bool(ring.gating_flags[i])) for i in range(cursor_count)]
            print(f"seq={expected_sequence}, queue={raw_queue.qsize()}, write={int(ring.write_seq[0])}, cursors={cursors}")

        while expected_sequence in pending:
            buf_ts_recv, buf_packet = pending.pop(expected_sequence)
            _, buf_seq, buf_count = struct.unpack(HEADER_FORMAT, buf_packet[:HEADER_SIZE])
            parse_and_apply(buf_seq, buf_count, buf_packet, buf_ts_recv, offset=0)



    
def main():
    for name in [SNAPSHOT_SHM_NAME, TRADE_SHM_NAME]:
        try:
            old = shared_memory.SharedMemory(name=name, create=False)
            old.close()
            old.unlink()
        except FileNotFoundError as e:
            print(f"{e}")
        except Exception:
            time.sleep(0.1)

    manager = multiprocessing.Manager()
    instrument_map = manager.dict()
    capacity = 262144  
    shm_size = 8 + 8 + (MAX_CONSUMERS * 8) + MAX_CONSUMERS + (MAX_CONSUMERS * 8) + (capacity * EVENT.itemsize)
    shm = shared_memory.SharedMemory(create=True, size=shm_size)
    shm.buf[:] = b'\x00' * shm_size

    ring = Ring(shm, capacity)
    ring.depends_on[:] = -1
    book_id = ring.register(gating=True, name="Order book")
    trade_relay_id = ring.register(gating=True, name="Trade relay", depends_on=book_id)
    logger_id = ring.register(gating=False, name="Logger")


    trade_ring_capacity = 65536
    trade_ring_size = 8 + 8 + (MAX_CONSUMERS * 8) + MAX_CONSUMERS + (MAX_CONSUMERS * 8) + (trade_ring_capacity * EVENT.itemsize)
    trade_ring_shm = shared_memory.SharedMemory(name=TRADE_SHM_NAME, create=True, size=trade_ring_size)
    trade_ring_shm.buf[:] = b'\x00' * trade_ring_size

    trade_ring = Ring(trade_ring_shm, trade_ring_capacity)
    ws_consumer_id = trade_ring.register(gating=True, name="WS trade consumer")

    raw_queue = multiprocessing.Queue()

    snapshot_size = MAX_INSTRUMENTS * SNAPSHOT_DTYPE.itemsize
    snapshot_shm = shared_memory.SharedMemory(name=SNAPSHOT_SHM_NAME, create=True, size=snapshot_size)
    snapshot_shm.buf[:] = b'\x00' * snapshot_size

    processes = [
        multiprocessing.Process(target=receiver, args=(raw_queue,), daemon=True),
        multiprocessing.Process(target=processor, args=(raw_queue, shm.name, capacity, instrument_map)),
        multiprocessing.Process(target=consumer, args=(shm.name, capacity, instrument_map, book_id)),
        multiprocessing.Process(target=logger, args=(shm.name, capacity, instrument_map, logger_id)),
        multiprocessing.Process(target=trade_relay, args=(shm.name, capacity, trade_relay_id, trade_ring_shm.name, trade_ring_capacity)),
        multiprocessing.Process(target=ws_server, args=(trade_ring_shm.name, trade_ring_capacity, ws_consumer_id, instrument_map))
    ]

    for p in processes:
        p.start()
    try:
        processes[0].join()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()

        for p in processes:
            p.join(timeout=3)

        time.sleep(1.5)

        shm.close()
        snapshot_shm.close()
        trade_ring_shm.close()
        try:
            shm.unlink()
            snapshot_shm.unlink()
            trade_ring_shm.unlink()
        except Exception as e:
            print(f"Warning during unlinking: {e}")
        print("All processes stopped, shared memory cleaned up.")

       
if __name__ == "__main__":
    main()
