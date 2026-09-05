import struct
import socket
import threading
from itch.parser import MessageParser
from constants import *

parser = MessageParser()
MESSAGE_COUNT = 1
SESSION_ID = b"0020190130"
itch_file_path = 'data/01302019.NASDAQ_ITCH50'

HEADER_FORMAT = ">10sQH"
BODY_FORMAT = ">H"
REQUEST_FORMAT = ">10sQH"
MARKET_OPEN_NS = 34_200_000_000_000
RETRANSMIT_PORT = 30001
BROKEN_SEQUENCES = set() # {50, 120, 121, 123, 125}   # hardcoded, withheld on purpose
broken_packets = {}                  
BOOK_TYPES = {b'A', b'F', b'E', b'C', b'X', b'D', b'U', b'P', b'Q', b'B', b'R'}

def read_and_pack_raw(itch_file_path, batch_size=20):
    BOOK_TYPES = {ord('A'), ord('F'), ord('E'), ord('C'), ord('X'), ord('D'), ord('U'), ord('P'), ord('Q'), ord('B'), ord('R')}
    WATCH_SYMBOLS = {b'SPY     ', b'AAPL    ', b'MSFT    ', b'NVDA    ', b'TSLA    ', b'AMD     ', b'QQQ     ', b'AMZN    '}

    with open(itch_file_path, 'rb') as f:
        data = f.read()

    pos = 0
    sequence = 0
    batch_bodies = []
    batch_start_seq = 1
    batch_ts = 0
    watched_locates = set()

    while pos + 2 <= len(data):
        length = int.from_bytes(data[pos:pos+2], 'big')
        pos += 2
        if pos + length > len(data):
            break
        msg_bytes = data[pos:pos+length]
        pos += length

        if msg_bytes[0] == ord('R'):
            symbol = msg_bytes[11:19]
            if symbol in WATCH_SYMBOLS:
                locate = int.from_bytes(msg_bytes[1:3], 'big')
                watched_locates.add(locate)
                # fall through to send it
            else:
                continue

        if msg_bytes[0] not in BOOK_TYPES:
            continue

        locate = int.from_bytes(msg_bytes[1:3], 'big')
        if locate not in watched_locates:
            continue

        sequence += 1

        if len(batch_bodies) == 0:
            batch_start_seq = sequence
            batch_ts = int.from_bytes(msg_bytes[5:11], 'big')

        batch_bodies.append(struct.pack(BODY_FORMAT, length) + msg_bytes)

        if len(batch_bodies) >= batch_size:
            header = struct.pack(HEADER_FORMAT, SESSION_ID, batch_start_seq, len(batch_bodies))
            yield batch_start_seq, header + b''.join(batch_bodies), batch_ts
            batch_bodies = []

    if batch_bodies:
        header = struct.pack(HEADER_FORMAT, SESSION_ID, batch_start_seq, len(batch_bodies))
        yield batch_start_seq, header + b''.join(batch_bodies), batch_ts

def read_and_pack(itch_file_path=itch_file_path):
    with open(itch_file_path, 'rb') as itch_file:
        sequence = 0
        for message in parser.parse_file(itch_file):
            if message.message_type not in BOOK_TYPES:
                continue
            sequence += 1
            length = message.message_size
            header = struct.pack(HEADER_FORMAT, SESSION_ID, sequence, MESSAGE_COUNT)
            body = struct.pack(BODY_FORMAT, length) + message.to_bytes()
            full_packet = header + body
            yield sequence, full_packet, message.timestamp

import time

def spin_wait_until(target_ns, threshold_ns=2_000_000):
    now = time.perf_counter_ns()
    remaining = target_ns - now
    if remaining <= 0:
        return
    if remaining > threshold_ns:
        time.sleep((remaining - threshold_ns) / 1e9)
    while time.perf_counter_ns() < target_ns:
        pass


def retransmit_server(bind_ip=IP):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((bind_ip, RETRANSMIT_PORT))
    print("Retransmit server listening...")
    while True:
        data, addr = sock.recvfrom(65536)
        session, start_seq, count = struct.unpack(REQUEST_FORMAT, data)
        print(f"retransmit request: start={start_seq} count={count} from {addr}")
        for seq in range(start_seq, start_seq + count):
            packet = broken_packets.get(seq)
            if packet is not None:
                sock.sendto(packet, addr)
            else:
                print(f"  no stored packet for {seq} (not withheld, or already gone)")


def broadcast(itch_file_path=itch_file_path, speed=100):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    sock.bind((IP, 0))

    threading.Thread(target=retransmit_server, daemon=True).start()

    first_ts = None
    start_perf = None

    for sequence, packet, ts_event in read_and_pack_raw(itch_file_path, batch_size=10):
        if first_ts is None:
            first_ts = ts_event
            start_perf = time.perf_counter_ns()
        
        target = start_perf + (ts_event - first_ts) // speed
        spin_wait_until(target)

        if sequence in BROKEN_SEQUENCES:
            broken_packets[sequence] = packet
            continue   # dont send to main feed, trigger sequence gap branch.

        sock.sendto(packet, ("229.0.0.1", 30000))

    print("SESSION ENDED!")

if __name__ == "__main__":
    broadcast()