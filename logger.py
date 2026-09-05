from multiprocessing import shared_memory
from ring_buffer import Ring
from constants import Action

def format_event(event, instrument_map):
    instrument_id = int(event['instrument_id'])
    symbol = instrument_map.get(instrument_id, f"UNK:{instrument_id}")
    return (
        f"{event['sequence']}|"
        f"{event['ts_event']}|"
        f"{event['ts_recv']}|"
        f"{Action(event['action']).name}|"
        f"{symbol}|"
        f"{event['order_id']}|"
        f"{event['side']}|"
        f"{event['price']}|"
        f"{event['quantity']}\n"
    )

def logger(shm_name, capacity, instrument_map, consumer_id):
    shm = shared_memory.SharedMemory(name=shm_name, create=False)   
    ring = Ring(shm, capacity)
    # consumer_id = ring.register(gating=False, name="Logger")
    print("Logger book listening now...")
    buffer = []
    FLUSH_INTERVAL = 10000

    with open("events.log", "a") as f:
        while True:
            result = ring.read(consumer_id)
            if result is None:
                if buffer:
                    f.write("".join(buffer))
                    f.flush()
                    buffer.clear()
                continue
            elif isinstance(result, tuple):
                buffer.append(f"GAP: missed {result[1]} events\n")
            else:
                buffer.append(format_event(result, instrument_map))

            if len(buffer) >= FLUSH_INTERVAL:
                f.write("".join(buffer))
                f.flush()
                buffer.clear()