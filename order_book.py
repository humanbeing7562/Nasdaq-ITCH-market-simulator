
from multiprocessing import shared_memory
from ring_buffer import Ring
from sortedcontainers import SortedDict
from constants import * 

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
    cursor = 0
    count = 0
    lapped_count = 0
    print("Consumer listening now...")
    while True:
        result = ring.read(cursor)
        if result is None:
            continue
        cursor += 1
        count += 1
        if isinstance(result, tuple) and result[0] == "LAPPED":
            lapped_count += 1
            print(f"LAPPED: cursor={cursor}, jumped to={result[1]}")
            cursor = result[1]
            continue

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

        

        WATCH_INSTRUMENT = 5628 # just to limit printing...

        if count % 1000000 == 0:
            if WATCH_INSTRUMENT is None:
                for inst_id, book in books.items():
                    if len(book.bid_prices) > 10 and len(book.ask_prices) > 10:
                        WATCH_INSTRUMENT = inst_id
                        break
            
            if WATCH_INSTRUMENT in books:
                book = books[WATCH_INSTRUMENT]
                print(f"{instrument_map.get(WATCH_INSTRUMENT)}: best_bid=${book.best_bid/10000:.2f}, best_ask=${book.best_ask/10000:.2f}")