# Nasdaq TotalView-ITCH5.0 Market Data Simulator

Market data feed handler simulator parsing real Nasdaq ITCH 5.0 data over reconstructed MoldUDP64 transport, with an LMAX Disruptor-style shared-memory ring buffer feeding gating and non-gating consumers into per-instrument order book construction. Built in Python as a learning vehicle for production feed handler architecture — not a performance claim.

The modules created will be as follows in order to create a full market data feed handler:

## Broadcaster (Module 0)

This is to simulate a real broadcasting service (i.e., to be "Nasdaq" market exchange who actually provides the ITCH data as if it were live trading) which broadcasts over a UDP multicast.

This module also comes with capabilities to wrap the provided ITCH data with [MoldUDP64](https://essenceia.github.io/projects/moldudp64/) (in particular, to add message formatting, sequencing and message counts). Note that the packet parsing is done raw now to improve speed and performance (as compared to using a built message parser previously). The broadcaster also comes with `batch_size` capability to include multiple messages in a single `MoldUDP64` packet as Nasdaq does with theirs.

Errors are also being intentionally introduced (i.e., skipping certain sequences) to mimic real and live trading situations to also encourage error handlings on the receiver's side. This is currently being done by hardcoding certain sequences to be skipped. This also requires the broadcaster to have another functionality to be able to retransmit certain requested data sequences which have been recently created.

In the future, this broadcaster will also include order book snapshotting to support error cases where the receiver might have crashed and thus needs to rebuild the entire session's order book or if, for example, too many sequences have been skipped on the receiver so that an entire order book snapshot can be asked instead of having to check for every single missing sequence.

To support the current testing, this module has also been fitted with pacing capabilities to mimic the actual real `ts_event` from the ITCH data as well as speed scaling to enhance the testing time (i.e., so we don't have to wait for an entire day to test an entire day's worth of data).

## Feed Handler (Module 1)

The purpose of this module is to attach to the broadcaster above in order to receive the data and process them accordingly (i.e., based on the event message type).

This receiver currently spawns 2 processes while sharing one `raw_queue`:
- A `receiver` (waiting on `sock.recv()` from the broadcaster) which fills in the `raw_queue`
- A `processor` which consumes the `raw_queue` and processes the messages accordingly and keeps a basic order tracking dictionary (keyed by `order_reference_number`) of everything that has happened based on each message's type. The processor dispatches on the raw message type byte and extracts fields via struct.unpack at known offsets, bypassing the MessageParser object construction and singledispatch type lookup that were used previously. This eliminated per-message Python object allocation on the hot path.

The processor is also currently capable of doing sequence error handling (i.e., where sequences are skipped due to simulated packet losses). When a message gets parsed by the processor, an `Event` will also be tracked for the future modules. 

## Ring Buffer (Module 2)

Moves decoded events from the feed handler to downstream consumers via a shared-memory ring buffer. The ring is a fixed-size pre-allocated numpy structured array backed by `multiprocessing.shared_memory`, with power-of-two capacity and bitwise slot indexing. The producer writes events and increments a monotonic `write_seq`; consumers each hold an independent cursor tracked in shared memory via `register()`.

This module should never make the producer wait — with the exception of [gating consumers](#gating-mechanism).

### Gating Mechanism

Each consumer registers as either gating or non-gating, answering one question: is this consumer allowed to slow the system down?

The book builder gates — if it gets lapped, it misses deltas and the book is silently corrupt. The logger doesn't gate — missed events are a logging gap, not a correctness failure. Before every write, the producer checks whether it would overwrite data the slowest gating consumer hasn't read. If so, `write()` returns `False` and the producer spins until the consumer catches up. The consequence is that memory grows in the `raw_queue` if the gating consumer is slower than the producer, which is the correct trade-off: visible backpressure instead of silent data corruption.

The producer caches the minimum gating cursor locally and only rescans when the cache suggests the ring might be full, so the common-path `write()` does one subtraction and one comparison with no shared memory reads.

The implementation follows the core mechanisms from the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/): single-producer multi-consumer with independent cursors in shared memory, gating vs non-gating consumer distinction, and cached minimum scan. See [ring_buffer.py](ring_buffer.py) for the full implementation.

### Dependency Gating

Beyond the producer-consumer gating above, consumers can also gate on other consumers via `depends_on`. A consumer registered with `depends_on=book_id` will not advance past any event the book builder hasn't finished processing. This implements the LMAX Disruptor's dependency graph — ordered consumer pipelines over a single shared buffer with no intermediate queues or copies.

The trade relay uses this: it reads the same main ring as the book builder but is chained behind it, so it can only see events the book builder has already applied. This guarantees that trades sent to the browser always correspond to a book state that has already been published — consistency between the two data streams is structural, not accidental.

The implementation is a two-line addition to `read()`: look up the dependency's cursor, return `None` if the caller hasn't fallen behind it yet. The `write()` path and `_scan_gating_min` are unchanged — dependency gating controls consumer-to-consumer ordering, not producer backpressure.

## Book (Module 3a)

Consumes ring buffer events as a gating consumer and reconstructs per-instrument order books. Each instrument will have their prices and quantities mapped in a dictionary to support constant time insertions and deletions. To support MBP-10 capabilities, the dictionaries are then sorted every 200ms in `publish_snapshots` which could then be used by other processes. We found a significant performance improvement this way (i.e., O(1) insertions/deletions with occasional O(n log n) sorting) compared to using `SortedDict` (O(log n) insertions/deletions with O(1) sorting).

A global `orders` dict (`order_id → {instrument, side, price, quantity}`) lives here, not in the feed handler, because ITCH cancel/delete/execute messages don't carry price or side — those fields must be looked up from the original add. Events are dispatched by action type (ADD, CANCEL, DELETE, EXECUTE, R_CANCEL, R_ADD) — see [order_book.py](order_book.py) for the full implementation.

Note that since we have not built an order snapshotting capability on the broadcaster side, the order book would always need to start listening from the start of the session (i.e., run the feed_handler (which orchestrates the other processes required) before running the broadcaster). 

The book builder now publishes MBP-10 snapshots to shared memory every 200ms. A flat numpy array of 65,536 snapshot slots (one per ITCH stock_locate) is allocated by the main process and mapped into the book builder via multiprocessing.shared_memory. Every 200ms, publish_snapshots() iterates all active books, extracts the top 10 bid and ask levels from each dictionary, and writes them into the corresponding slot. The 200ms interval reflects that the intended downstream consumers are browser-based displays — human eyes cannot track individual price level changes faster than this, so writing more frequently would be wasted work. 

Writes are protected by a seqlock — the counter increments to odd before writing and back to even after, so any reader that observes an odd value or a changed value between its two reads knows it caught a torn read and retries. This is the publication boundary between the hot path and future cold-path consumers (WebSocket server, dashboard); the book builder doesn't know or care who reads the snapshots.

## OHLCV Aggregator (Module 3b)

### Part 1 -- Ring consumer
Consumes the ring buffer as a gating consumer, filtering for `Action.EXECUTE` events — the normalized form of both executed order types after the feed handler has resolved prices and written them to the ring. These trade events are written into a shared-memory trade buffer (a fixed-size numpy structured array with a monotonic write counter, identical in structure to the main ring but dedicated to trades). This replaced an earlier `multiprocessing.Queue` approach where pickle serialization per trade was the bottleneck; the shared-memory path has zero serialization overhead.

### Part 2 -- WebSocket OHLCV Aggregator
Polls the shared-memory trade buffer every 50ms, reading all new entries since the last poll via an independent read cursor. Trades are batched into a single WebSocket message (up to 1000 per batch) to reduce frame overhead and React render count. OHLCV bar aggregation is handled entirely on the frontend — the server streams raw trades and the browser buckets them into whatever timeframe the user selects.

Note: the relay and websocket are split into separate processes not because the relay is on the hot path (it isn't — OHLCV is a monitoring concern, not a trading one), but because the relay's job is a tight ring-reading loop that shouldn't be blocked by websocket I/O.

### Part 3 -- React/Vite Visualizer

Single-page React app scaffolded with Vite. Two panels: a candlestick chart (TradingView Lightweight Charts) with a volume histogram, and an MBP-10 price ladder showing bid/ask depth. A single WebSocket connection to the backend delivers both trade batches and book snapshots. OHLCV bars are aggregated client-side from raw trade events, with a selectable timeframe (1s, 5s, 30s, 1m, 5m). The chart includes a crosshair overlay displaying OHLCV and volume values on hover.

## Logger (Module 3c)

To further test the Single-Producer-Multi-Consumer architecture and ensure that it is working correctly, a simple logging module has been made. Note that it won't log every single event as it receives but instead do it in batches because I/O per event is a bottleneck at this message rate. The logger writes to a file `events.log` which has been listed under `.gitignore` due to the file size.

## Planned

- Strategy (Module 4) — consume book state, emit order intents
- Order Gateway (Module 5) — exchange session management, order state machine
- Risk Gate (Module 6) — pre-trade checks, kill switch

## Notes

For more information, you may refer to [build_log.md](docs/build-log.md) for a dated record of design decisions, bugs encountered (and solutions), as well as implementation details.