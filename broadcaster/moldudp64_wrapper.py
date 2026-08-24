import struct
from itch.parser import MessageParser
import socket

parser = MessageParser()
MESSAGE_COUNT = 1
SESSION_ID = b"0020190130"
itch_file_path = '../data/01302019.NASDAQ_ITCH50'

HEADER_FORMAT = ">10sQH"
BODY_FORMAT = ">H"

def read_and_pack(itch_file_path=itch_file_path):
    with open(itch_file_path, 'rb') as itch_file:
        for sequence, message in enumerate(parser.parse_file(itch_file), start=1):
            length = message.message_size
            header = struct.pack(HEADER_FORMAT, SESSION_ID, sequence, MESSAGE_COUNT)
            body = struct.pack(BODY_FORMAT, length) + message.to_bytes()
            full_packet = header + body
            yield full_packet, message.timestamp

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

def broadcast(itch_file_path=itch_file_path, speed=1000 ):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    
    sock.bind(("192.168.0.13", 0))

    first_ts = None
    start_perf = None

    for packet, ts_event in read_and_pack(itch_file_path):
        if first_ts is None:
            first_ts = ts_event
            start_perf = time.perf_counter_ns()

        target = start_perf + (ts_event - first_ts) // speed
        spin_wait_until(target)

        sock.sendto(packet, ("229.0.0.1", 30000))

if __name__ == "__main__":
    broadcast()
