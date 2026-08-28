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

RETRANSMIT_PORT = 30001
BROKEN_SEQUENCES = {50, 120, 121, 123, 125}   # hardcoded, withheld on purpose
broken_packets = {}                  

def read_and_pack(itch_file_path=itch_file_path):
    with open(itch_file_path, 'rb') as itch_file:
        for sequence, message in enumerate(parser.parse_file(itch_file), start=1):
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

    for sequence, packet, ts_event in read_and_pack(itch_file_path):
        if first_ts is None:
            first_ts = ts_event
            start_perf = time.perf_counter_ns()

        target = start_perf + (ts_event - first_ts) // speed
        spin_wait_until(target)

        if sequence in BROKEN_SEQUENCES:
            broken_packets[sequence] = packet
            continue   # dont send to main feed, trigger sequence gap branch.

        sock.sendto(packet, ("229.0.0.1", 30000))


if __name__ == "__main__":
    broadcast()