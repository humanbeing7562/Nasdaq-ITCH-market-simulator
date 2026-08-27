import numpy as np
from multiprocessing import shared_memory

EVENT = np.dtype([
    ("action", np.uint8),
    ("ts_event", np.int64),
    ("ts_recv", np.int64),
    ("sequence", np.uint64),
    ("order_id", np.uint64),
    ("quantity", np.int32),
    ("side", np.uint8),
    ("instrument_id", np.uint32),
    ("price", np.int64)
])

class Ring:
    def __init__(self, shm, capacity):
        if not (capacity > 0 and (capacity & (capacity - 1)) == 0):
            raise ValueError("Capacity should be a power of two to simplify memory initialization")

        self.capacity = capacity
        self.ring = np.ndarray(capacity, dtype=EVENT, buffer=shm.buf[8:])
        self.write_seq = np.ndarray(1, dtype=np.uint64, buffer=shm.buf[0:8])
        self.mask = capacity - 1
        self.shm = shm


    def write(self, data):
        self.ring[self.write_seq[0] & self.mask] = (data)
        self.write_seq += 1


    def read(self, cursor):
        if self.write_seq[0] - cursor > self.capacity:
            return ("LAPPED", self.write_seq[0])
        if cursor < self.write_seq[0]:
            return self.ring[cursor & self.mask].copy()
        return None
    
