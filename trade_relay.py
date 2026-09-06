from multiprocessing import shared_memory
from ring_buffer import Ring
from constants import *
import numpy as np


def trade_relay(shm_name, capacity, consumer_id, trade_ring_shm_name, trade_ring_capacity):
    shm = shared_memory.SharedMemory(name=shm_name, create=False)
    ring = Ring(shm, capacity)

    trade_shm = shared_memory.SharedMemory(name=trade_ring_shm_name, create=False)
    trade_ring = Ring(trade_shm, trade_ring_capacity)

    count = 0
    while True:
        result = ring.read(consumer_id)
        if result is None:
            continue

        if result["action"] != Action.EXECUTE:
            continue

        while not trade_ring.write((
            result["action"],
            result["ts_event"],
            result["ts_recv"],
            result["sequence"],
            result["order_id"],
            result["quantity"],
            result["side"],
            result["instrument_id"],
            result["price"],
        )):
            pass

        count += 1
        if count % 1000 == 0:
            print(f"TRADES: {count} relayed, trade_ring write_seq={int(trade_ring.write_seq[0])}")