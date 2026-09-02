
from multiprocessing import shared_memory
from ring_buffer import Ring
from constants import * 
import time
import numpy as np
from collections import defaultdict


def trade_relay(shm_name, capacity, trade_queue):
    shm = shared_memory.SharedMemory(name=shm_name, create=False)
    ring = Ring(shm, capacity)
    consumer_id = ring.register(gating=True, name="OHCLV consumer")
    # count = 0

    count = 0
    while True:
        result = ring.read(consumer_id)
        if result is None:
            continue

        if result["action"] != Action.EXECUTE:
            continue

        trade_queue.put((
            int(result["instrument_id"]),
            int(result["price"]),
            int(result["quantity"]),
            int(result["ts_event"]),
        ))

        count += 1
        if count % 100_000 == 0:
            print(f"TRADES: {count} relayed")

            



        