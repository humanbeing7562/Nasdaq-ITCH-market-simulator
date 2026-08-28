# Nasdaq TotalView-ITCH5.0 Market Data Simulator

This repository contains an attempt to create an industrial grade market data processing module with order book maintaining capabilities using [data samples](https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/) provided by Nasdaq.

The modules created will be as follows:

## Broadcaster (Module 0)

This is to simulate a real broadcasting service (i.e., to be "Nasdaq" market exchange who actually provides the ITCH data as if it were live trading) which broadcasts over a UDP multicast.

This module also comes with capabilities to wrap the provided ITCH data with [MoldUDP64](https://essenceia.github.io/projects/moldudp64/) (in particular, to add message formatting, sequencing and message counts).

Errors are also being intentionally introduced (i.e., skipping certain sequences) to mimic real and live trading situations to also encourage error handlings on the receiver's side. This is currently being done by hardcoding certain sequences to be skipped. This also requires the broadcaster to have another functionality to be able to retransmit certain requested data sequences which have been recently created.

In the future, this broadcaster will also include order book snapshotting to support error cases where the receiver might have crashed and thus needs to rebuild the entire session's order book or if, for example, too many sequences have been skipped on the receiver so that an entire order book snapshot can be asked instead of having to check for every single missing sequence.

To support the current testing, this module has also been fitted with pacing capabilities to mimic the actual real `ts_event` from the ITCH data as well as speed scaling to enhance the testing time (i.e., so we don't have to wait for an entire day to test an entire day's worth of data).

## Feed Handler (Module 1)

The purpose of this module is to attach to the broadcaster above in order to receive the data and process them accordingly (i.e., based on the event message type).

This receiver currently spawns 2 processes while sharing one `raw_queue`:
- A `receiver` (waiting on `sock.recv()` from the broadcaster) which fills in the `raw_queue`
- A `processor` which consumes the `raw_queue` and processes the messages accordingly and keeps a basic order tracking dictionary (keyed by `order_reference_number`) of everything that has happened based on each message's type (through Python's `functools.singledispatch`)

The processor is also currently capable of doing sequence error handling (i.e., where sequences are skipped due to simulated packet losses). When a message gets parsed by the processor, an `Event` will also be tracked for the future modules. 

## Ring Buffer (Module 2) (IN PROGRESS)

Purpose of this module will be to move decoded events from the feed handler thread to the strategy thread (maybe process instead?).

You could argue that this could've been part of module 1 but there's a potential performance and memory space issue -- Module 1's job is to drain the network queue as much as possible, if it handles too much processing, the queue could consume too much memory.

Note that this module should **NEVER** make the feed handler (i.e., producer) wait -- producer needs to keep going as the broadcaster goes.

The way it is currently implemented is by instantiating a fixed size `list` where each element is also `fixed` in size. We will then keep track of a variable `write_sequence` which is used by the producer to know which index of the ring buffer it should write to (and incrementing it every write). The `write_sequence` will also be masked every time to ensure it doesn't go out of bounds. The full implementation (as of 28 Aug 2026) can be found below:

```python
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
```

In terms of reading, to support a Single-Producer-Multi-Consumer architecture, each module that tries to read must provide their own `cursor` to indicate which specific index that they want. Note that since the data itself is a ring that gets constantly rewritten, there is a possibility that a reader might have been "lapped". The ring itself can check if you are trying to read something that is lapped. Currently, the only precaution done is by increasing the size of the ring -- apart from that, it is currently assumed that a reader should not be too slow (in terms of processing) to not get lapped.

## Book (Module 3) (IN PROGRESS)

This is one example of the consumers of the ring buffer implemented above. The goal of this module (for now) is to keep both market-by-order (MBO) and market-by-price-1 (MBP-1) data by reading and reconstructing data from the ring buffer. It is noted that this order book should also keep track for **each** stock symbol, rather than just tracking a certain few.

To implement this as efficiently as possible, we create a `Book` class to keep track for each stock symbol:

```python
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
```

The book will keep track of bid and ask prices, along with their quantity per each price. We use `SortedDict` to keep track of them as it is currently the most efficient way (O(log n)) to work with constantly changing prices and quantity. As we do want the best bid and ask prices, `SortedDict` allows us to grab either the first/last index for ask/bid prices respectively to get their best prices. Other ideas were explored, however, this is the best one currently possible. (Any feedback on this would be more than welcome)

To keep track of multiple stock symbols, we store all the `Book`s into another dictionary `books` and update it accordingly depending on the event types we receive. For example, when we receive an `ADD` event, we will create the `Book` for the stock symbol (if it hasn't already been created yet) and add the new price based on the ask/bid side:

```python
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
```

This structure, while not optimal, gives us a good enough performance to process the incoming events at 100x real speed without getting lapped by the feed handler (Module 1).

Note that since we have not built an order snapshotting capability on the broadcaster side, the order book would always need to start listening from the start of the session (i.e., run the feed_handler (which orchestrates the other processes required) before running the broadcaster). 

## Strategy (Module 4) (IN PROGRESS) (NOT A FULL TRADING STRATEGY, BUT JUST TO MIMIC BUYS AND SELLS)

(TBC)

## Order Gateway (Module 5) (IN PROGRESS)

(TBC)

## Risk Mitigation (Module 6) (IN PROGRESS)

(TBC)

