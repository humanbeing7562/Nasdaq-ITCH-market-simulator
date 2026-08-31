import numpy as np
from constants import * 
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
        self.write_seq = np.ndarray(1, dtype=np.uint64, buffer=shm.buf[0:8]) # need ndarray here because uint64 alone wont be mutable.
        self.consumer_count = np.ndarray(1, dtype=np.uint64, buffer=shm.buf[8:16]) # need ndarray here because uint64 alone wont be mutable.
        self.cursors = np.ndarray(MAX_CONSUMERS, dtype=np.uint64, buffer=shm.buf[16:48])
        self.gating_flags = np.ndarray(MAX_CONSUMERS, dtype=np.uint8, buffer=shm.buf[48:52])
        self.ring = np.ndarray(capacity, dtype=EVENT, buffer=shm.buf[52:])
        self.mask = capacity - 1
        self.shm = shm
        self._cached_min_gated = 0

    def _scan_gating_min(self):
        count = int(self.consumer_count[0])
        min_gated = self.write_seq[0]
        for i in range(count):
            if self.gating_flags[i]:
                min_gated = min(min_gated, self.cursors[i])
        return min_gated

    def register(self, gating=False, name=""):
        idx = self.consumer_count[0]
        self.consumer_count[0] += 1
        self.gating_flags[idx] = 1 if gating else 0
        self.cursors[idx] = self.write_seq[0] 
        print(f"{name} has been registered as consumer")
        return int(idx)

    def write(self, data):
        if self.write_seq[0] - self._cached_min_gated >= self.capacity:
            self._cached_min_gated = self._scan_gating_min()
            if self.write_seq[0] - self._cached_min_gated >= self.capacity:
                # print(f"RING FULL: write_seq={self.write_seq[0]}, min_gated={self._cached_min_gated}, depth={self.write_seq[0] - self._cached_min_gated}") # uncomment this if you want to see debug prints of lapping
                return False  # genuinely full
        
        self.ring[self.write_seq[0] & self.mask] = data
        self.write_seq[0] += 1
        return True


    def read(self, consumer_id):
        cursor = self.cursors[consumer_id]
        write_pos = self.write_seq[0]

        if cursor >= write_pos:
            return None

        if write_pos - cursor > self.capacity:
            # lapped
            self.cursors[consumer_id] = write_pos
            return ("LAPPED", int(write_pos - cursor))

        event = self.ring[cursor & self.mask].copy()
        self.cursors[consumer_id] = cursor + 1
        return event
