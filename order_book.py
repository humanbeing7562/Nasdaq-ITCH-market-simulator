
from multiprocessing import shared_memory
from ring_buffer import Ring
from sortedcontainers import SortedDict
from constants import * 
import time
import numpy as np

def attach_snapshot_shm():
    shm = shared_memory.SharedMemory(name=SNAPSHOT_SHM_NAME, create=False)
    snapshots = np.ndarray(MAX_INSTRUMENTS, dtype=SNAPSHOT_DTYPE, buffer=shm.buf)
    return shm, snapshots

def publish_snapshots(books, snapshots):
    now = time.time_ns()
    for instrument_id, book in books.items():
        slot = snapshots[instrument_id]
 
        slot['seqlock'] += 1
 
        slot['bid_price'][:] = 0
        slot['bid_qty'][:] = 0
        slot['ask_price'][:] = 0
        slot['ask_qty'][:] = 0
 
        bids = list(book.bid_prices.items())[-MBP_DEPTH:]
        for i, (price, qty) in enumerate(reversed(bids)):
            slot['bid_price'][i] = price
            slot['bid_qty'][i] = qty
 
        asks = list(book.ask_prices.items())[:MBP_DEPTH]
        for i, (price, qty) in enumerate(asks):
            slot['ask_price'][i] = price
            slot['ask_qty'][i] = qty
 
        slot['timestamp'] = now
 
        slot['seqlock'] += 1

def read_snapshot(snapshots, instrument_id):
    # don't think we need this function here 
    # but just leaving it here as a sample
    slot = snapshots[instrument_id]
 
    seq1 = int(slot['seqlock'])
    if seq1 % 2 == 1:
        return None 
 
    data = {
        'bid_price': slot['bid_price'].copy(),
        'bid_qty': slot['bid_qty'].copy(),
        'ask_price': slot['ask_price'].copy(),
        'ask_qty': slot['ask_qty'].copy(),
        'timestamp': int(slot['timestamp']),
    }
 
    seq2 = int(slot['seqlock'])
    if seq2 != seq1:
        return None
 
    return data

class Book:

    __slots__ = ("bid_prices", "ask_prices")
    
    def __init__(self):    
        self.bid_prices = SortedDict()
        self.ask_prices = SortedDict()

    @property
    def best_bid(self):
        keys = self.bid_prices.keys()
        if len(keys) == 0:
            return 0
        return keys[-1]

    @property
    def best_ask(self):
        keys = self.ask_prices.keys()
        if len(keys) == 0:
            return 0
        return keys[0]

orders = {}    

books = {}

def print_book(book, instrument_id, top_n=5):
    print(f"\n{'=' * 50}")
    print(f"  Instrument: {instrument_id}")
    print(f"{'=' * 50}")
    print(f"  {'ASK':>10}  {'Price':>12}  {'Qty':>10}")
    print(f"  {'-' * 36}")

    asks = list(book.ask_prices.items())[:top_n]
    for price, qty in reversed(asks):
        print(f"  {'':>10}  {price:>12}  {qty:>10}")

    print(f"  {'--- spread ---':^36}")

    bids = list(book.bid_prices.items())[-top_n:]
    for price, qty in reversed(bids):
        print(f"  {qty:>10}  {price/10000:>12}")

    print(f"{'=' * 50}\n")

def get_side_prices(book, side):
    return book.bid_prices if side == Side.BID else book.ask_prices

def decrement_level(side_prices, price, qty):
    side_prices[price] -= qty
    if side_prices[price] <= 0:
        del side_prices[price]

def consumer(shm_name, capacity, instrument_map):
    shm = shared_memory.SharedMemory(name=shm_name, create=False)
    ring = Ring(shm, capacity)
    consumer_id = ring.register(gating=True, name="Order book")
    # count = 0

    snapshot_shm, snapshots = attach_snapshot_shm()
    last_publish = time.monotonic()

    print("Order book listening now...")
    while True:
        result = ring.read(consumer_id)
        if result is None:
            continue

        # count += 1

        if result['action'] == Action.ADD:
            order_id = result['order_id']
            price = result['price']
            qty = result['quantity']
            side = result['side']
            instrument_id = result['instrument_id']

            orders[order_id] = {
                "instrument": instrument_id,
                "side": side,
                "price": price,
                "quantity": qty,
            }
            book = books.setdefault(instrument_id, Book())
            side_prices = get_side_prices(book, side)
            side_prices[price] = side_prices.get(price, 0) + qty

        elif result['action'] == Action.EXECUTE:
            order_id = result['order_id']
            order = orders[order_id]
            executed_qty = result['quantity']
            book = books[order['instrument']]
            decrement_level(get_side_prices(book, order['side']), order['price'], executed_qty)

            order['quantity'] -= executed_qty
            if order['quantity'] <= 0:
                del orders[order_id]

        elif result['action'] == Action.CANCEL:
            order_id = result['order_id']
            order = orders[order_id]
            cancelled_qty = result['quantity']
            book = books[order['instrument']]
            decrement_level(get_side_prices(book, order['side']), order['price'], cancelled_qty)

            order['quantity'] -= cancelled_qty
            if order['quantity'] <= 0:
                del orders[order_id]

        elif result['action'] == Action.DELETE:
            order_id = result['order_id']
            order = orders[order_id]
            remaining_qty = order['quantity']
            book = books[order['instrument']]
            decrement_level(get_side_prices(book, order['side']), order['price'], remaining_qty)
            del orders[order_id]

        elif result['action'] == Action.R_CANCEL:
            order_id = result['order_id']
            order = orders[order_id]
            remaining_qty = order['quantity']
            book = books[order['instrument']]
            decrement_level(get_side_prices(book, order['side']), order['price'], remaining_qty)
            del orders[order_id]

        elif result['action'] == Action.R_ADD:
            order_id = result['order_id']
            price = result['price']
            qty = result['quantity']
            instrument_id = result['instrument_id']

            side = result["side"]
            orders[order_id] = {
                "instrument": instrument_id,
                "side": side,
                "price": price,
                "quantity": qty,
            }
            book = books.setdefault(instrument_id, Book())
            side_prices = get_side_prices(book, side)
            side_prices[price] = side_prices.get(price, 0) + qty


        now = time.monotonic()
        if now - last_publish >= SNAPSHOT_INTERVAL:
            publish_snapshots(books, snapshots)
            last_publish = now
            print(read_snapshot(snapshots, 5628))
        

        # WATCH_INSTRUMENT = 5628 # just to limit printing...

        # if count % 1000000 == 0:
        #     if WATCH_INSTRUMENT is None:
        #         for inst_id, book in books.items():
        #             if len(book.bid_prices) > 10 and len(book.ask_prices) > 10:
        #                 WATCH_INSTRUMENT = inst_id
        #                 break
            
        #     if WATCH_INSTRUMENT in books:
        #         book = books[WATCH_INSTRUMENT]
        #         print(f"{instrument_map.get(WATCH_INSTRUMENT)}: best_bid=${book.best_bid/10000:.2f}, best_ask=${book.best_ask/10000:.2f}")