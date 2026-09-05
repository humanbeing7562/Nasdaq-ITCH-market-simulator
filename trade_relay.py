from multiprocessing import shared_memory
from ring_buffer import Ring
from constants import *
import numpy as np


def trade_relay(shm_name, capacity, consumer_id):
    shm = shared_memory.SharedMemory(name=shm_name, create=False)
    ring = Ring(shm, capacity)

    trade_shm = shared_memory.SharedMemory(name=TRADE_SHM_NAME, create=False)
    write_seq = np.ndarray(1, dtype=np.uint64, buffer=trade_shm.buf[0:8])
    trades = np.ndarray(TRADE_BUFFER_SIZE, dtype=TRADE_DTYPE, buffer=trade_shm.buf[8:])
    mask = TRADE_BUFFER_SIZE - 1

    count = 0
    while True:
        result = ring.read(consumer_id)
        if result is None:
            continue

        if result["action"] != Action.EXECUTE:
            continue

        slot = write_seq[0] & mask
        trades[slot]['instrument_id'] = result['instrument_id']
        trades[slot]['price'] = result['price']
        trades[slot]['quantity'] = result['quantity']
        trades[slot]['ts_event'] = result['ts_event']
        write_seq[0] += 1

        count += 1
        if count % 100_000 == 0:
            print(f"TRADES: {count} relayed")