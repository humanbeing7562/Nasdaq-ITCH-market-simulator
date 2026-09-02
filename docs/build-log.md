# Feed Handler Simulator — Build Log

Running day-by-day log of what's actually been built, not a study plan. New entries get appended at the bottom.

---

## 2026-08-19 — Wire format & synthetic replay
**Built:**
- `wire.py` — 64-byte `EVENT` numpy dtype + 24-byte packet header, encode/decode functions.
- `publish.py` — synthetic record generator with exponential inter-arrival spacing, spin-wait pacer, `ts_event` rebasing onto current epoch, `--speed` divisor arg.
- `listen.py` — UDP receiver validating sequence numbers, counting gaps/duplicates, reporting avg/max latency.

**Learned / decided:**
- Windows `time.sleep()` has ~15.6ms granularity — sub-millisecond pacing needs spin-waiting.
- Pushed back on an over-staged build plan; agreed to prove plumbing and pacing first, deferring gap recovery, A/B arbitration, and impairment layers.

**Status:** synthetic plumbing working end-to-end.

---

## 2026-08-24 — Real ITCH data + MoldUDP64 transport
**Built:**
- `broadcaster/moldudp64_wrapper.py` — reads a real Nasdaq ITCH 5.0 sample file via `itchfeed`, assigns 1-based sequence numbers in file order, wraps each message in a MoldUDP64 packet (20-byte header: 10s session + Q sequence + H count, then 2-byte length prefix + payload), broadcasts over UDP multicast (`229.0.0.1:30000`). Paced using real `ts_event` deltas scaled by `--speed`, hybrid sleep+spin.
- `feed_handler.py` — two `multiprocessing.Process`es: `receiver` (just `sock.recv()` → queue) and `processor` (unpacks header, validates sequence, decodes via `parser.get_message_type()`, dispatches through `functools.singledispatch`, maintains an `orders` dict keyed by order_ref, emits `Event` records). Replace messages emit CANCEL+ADD.

**The `EVENT` record layer:**
- Only book-mutating message types produce a well-formed `EVENT` — the `AFECXDUPQ` set (Add, Executed, Executed-with-price, Cancel, Delete, Replace, Trade, Cross Trade). Everything else (session markers, symbol directory, halts, auction/compliance metadata) either doesn't touch the book or has no price/side/quantity to report, so it's structurally excluded, not just filtered out as noise.
- Built the `order_reference_number → {instrument, side, price}` lookup table populated on Add, since Cancel/Delete/Execute messages don't carry price/side/instrument themselves — those get filled in from the table to produce a self-contained `EVENT`.
- Worked through the Replace case specifically: it's really two things happening at once (an order dying and a new one being born). Landed on emitting it as two events — CANCEL for the old id, ADD for the new — rather than a single MODIFY, because that's the most faithful to what actually happened on the book and means a downstream book builder can apply both mechanically without special-casing replace at all.
- For now, decoded `EVENT` records are just printed / collected into a list to eyeball — no consumer downstream yet.

**Why this matters going forward:** the `EVENT` record is the actual interface boundary of the whole project. Every module downstream — the shared-memory ring buffer (Module 2), the book builder (Module 3), a future strategy/journaller/logger — consumes this exact struct and nothing upstream of it. Getting the shape and semantics right now (fixed fields, one order id per event, book-mutating messages only) is what lets those modules be built against a stable contract later instead of special-casing ITCH message types themselves. It's also the natural unit for a future journal/replay log (Databento-style) if this ever gets recorded to disk for backtesting.

**Learned / decided (bugs found the hard way):**
- Sequence numbers live in the MoldUDP64 transport layer, not in ITCH itself — Nasdaq's downloadable sample files strip that layer entirely, so it had to be reconstructed by hand.
- Windows multicast: bind to `""`, not the group address, then join via `IP_ADD_MEMBERSHIP`; `IP_MULTICAST_IF` must be set explicitly on the sender.
- Unpaced sender + slow receive loop caused UDP receive buffer overflow (silent packet loss). Fixed with pacing + `SO_RCVBUF` + splitting receive from processing into separate *processes* — threads weren't enough due to GIL contention.
- ITCH field-name gotchas: `OrderExecutedMessage.executed_shares` not `.shares`; `OrderCancelMessage.cancelled_shares` not `.shares`; `OrderReplaceMessage` omits `side`/`stock` (must inherit from the old order entry) but does carry `stock_locate`.
- Fixed a reversed comparison bug in the executed-quantity handler.

**Not built yet (as of this session):** gap *handling* (detection only — currently prints and breaks), reorder buffer, A/B arbitration, recovery/snapshot, book builder (Module 3), ring buffer (Module 2).

**Status:** real ITCH data flowing end-to-end over reconstructed MoldUDP64, correctly normalized into `EVENT` records; gap detection present but no recovery; nothing consumes the events yet.

---

## 2026-08-25 — Gap-handling theory + retransmit recovery
**Focus:** working through the theory for sequence gap handling — reorder buffer design, freeze semantics, and the three parallel recovery paths — ahead of implementing them.

**Debugging:** chased a bad-sequence bug in `feed_handler.py`'s gap-guard branch (`sequence + count <= expected_sequence`), narrowing down whether it was a header-parse mismatch or a logic bug in the guard.

**Built:** gap recovery via MoldUDP64 retransmit request — implemented and confirmed working end-to-end including chained gaps. Explicitly deferred afterward.

**Status:** Module 1 complete at the happy-path level. Gap detection and retransmit recovery working; reorder buffer, A/B arbitration, and snapshot resync deferred.

---

## 2026-08-26 — Ring buffer planning, orders dict placement
**Decided:**
- Ring buffer (shared memory, per Module 2 design) will be built next, as a standalone module — before touching the `orders` dict situation in feed_handler.py.
- The `orders` dict currently living in feed_handler.py's `processor()` is full order state (instrument/side/price/quantity), not just an existence/membership check — confirmed this belongs in book.py, not the feed handler, per the Module 1 / Module 3 split.
- Sequencing: build ring.py first (prove plumbing), then slowly strip `orders` dict usages out of feed_handler.py's message handlers, rebuilding that state in book.py instead. Not doing both at once.

**Status:** planning session, no new production files.

---

## 2026-08-27 — Module 2: Shared-memory SPMC ring buffer
**Built:**
- `ring_buffer.py` — `Ring` class backed by `multiprocessing.shared_memory`. `EVENT` numpy structured dtype with fields: action (uint8), ts_event (int64), ts_recv (int64), sequence (uint64), order_id (uint64), quantity (int32), side (uint8), instrument_id (uint32), price (int64). Power-of-two capacity enforced via `n & (n-1) == 0`. Slot indexing uses `& mask`. `write_seq` stored as a 1-element numpy uint64 array overlaid on the first 8 bytes of shared memory; event buffer overlaid starting at byte 8.
- `write()` assigns tuples directly to structured array slots, increments `write_seq[0]`.
- `read()` takes an external consumer-owned cursor, checks `cursor < write_seq[0]`, returns `.copy()` to prevent stale-view bugs. Lapping detection added: `write_seq - cursor > capacity` returns a `("LAPPED", new_cursor)` tuple so the consumer can skip forward.
- `enums.py` — pulled `Action` (ADD=1, CANCEL=2, DELETE=3, EXECUTE=4, R_CANCEL=5, R_ADD=6) and `Side` (BID=0, ASK=1) out of feed_handler.py to break a circular import with order_book.py.
- Wired into `feed_handler.py`: processor creates a `Ring` from shared memory name, all `singledispatch` handlers write events directly to the ring instead of creating `Event` objects. Parent `main()` creates shared memory, spawns receiver, processor, and consumer as separate processes.
- `orders` dict in feed_handler.py stripped down to `order_id -> buy_sell_indicator` (raw byte only) — just enough to stamp the correct side onto R_ADD/R_CANCEL events. Full order state moved to the book builder.
- Instrument directory shared across processes via `multiprocessing.Manager().dict()` — processor writes `stock_locate -> symbol` mappings, consumer reads them for display.

**Learned / decided:**
- `Side.UNKNOWN = 255` not needed — resolved the R_ADD side problem by keeping a lightweight `order_sides` dict in the feed handler (order_id -> raw buy_sell_indicator byte). Replace handler transfers side to new order ID before deleting old. Every ring event is now self-contained.
- Terminal printing was the bottleneck causing consumer lapping — printing per-event or even every 10k events was too frequent. Reduced to every 500k–1M events.
- Initial capacity of 1024 was far too small — producer could wrap it in milliseconds. Bumped to 262144 (2^18), then to 4194304 (2^22) for safety.
- `Manager()` must be created inside `main()` on Windows, not at module level — Windows `multiprocessing` uses spawn, so every child re-imports the module and tries to create its own Manager, causing a recursive spawn error.
- Write-side full check explicitly deferred — producer overwrites unconditionally per design (non-gating consumer model).

**Status:** Module 2 complete. Three-process architecture working: receiver → processor (writes to ring) → consumer (reads from ring). Events flow across process boundaries via shared memory with zero laps at current pacing.

---

## 2026-08-28 — Module 3: Book builder (order_book.py)
**Built:**
- `order_book.py` — consumer process pulled out of feed_handler.py into its own file. Spin-reads the shared-memory ring and builds the order book.
- `Book` class with `__slots__` — per-instrument book holding `bid_prices` and `ask_prices` as `SortedDict` (from `sortedcontainers`). `best_bid` and `best_ask` as `@property` accessors: `bid_prices.keys()[-1]` and `ask_prices.keys()[0]`, with empty-dict guards returning 0.
- `books` dict — `stock_locate -> Book`, lazily created via `setdefault()` on first order for each instrument.
- `orders` dict — global, `order_id -> {instrument, side, price, quantity}`. Full order state rebuilt here from ring events. Needed because ITCH cancel/delete/execute messages don't carry price or side.
- Helper functions: `get_side_prices(book, side)` returns the correct SortedDict for bid or ask. `decrement_level(side_prices, price, qty)` decrements and auto-deletes the key when quantity hits zero.
- All six event types handled: ADD (insert order, increment level), EXECUTE (decrement level and order qty, delete order if fully filled), CANCEL (decrement level and order qty, delete order if fully cancelled), DELETE (remove remaining qty from level, delete order), R_CANCEL (remove remaining qty from level, delete order), R_ADD (insert new order, increment level — side now carried in the ring event directly).
- Display: `print_book()` function showing top N ask/bid levels with prices divided by 10000 for human-readable dollar values. Instrument symbols resolved via the shared `instrument_map` from `multiprocessing.Manager().dict()`.

**Learned / decided:**
- SortedDict over separate dict + SortedList — one structure handles both `price -> qty` mapping and sorted key access. Eliminates the consistency bug class where two parallel structures disagree.
- `__slots__` on `Book` for memory efficiency and attribute access speed — minor in Python, but good habit and catches typos via AttributeError.
- ITCH prices are fixed-point integers with four implied decimal places (raw 4195000 = $419.50). Stored as raw integers internally, divided by 10000 only at display boundary. Experienced the float subtraction problem firsthand: `round(ask/10000, 2) - round(bid/10000, 2)` produced wrong spread due to IEEE 754 representation. Fix: compute spread on raw integers, divide once for display.
- Level deletion on zero quantity is mandatory regardless of data structure — an empty level left in the SortedDict would make best_bid/best_ask point at a price with nothing there.
- Consumer lapping still possible if print frequency is too high or ring capacity too small. Increased capacity to 2^22 and reduced print frequency to every 1M events. Unknown-order guard (`if order_id not in orders: continue`) discussed but deliberately omitted to surface errors loudly.
- MoldUDP64/ITCH has no encryption — plaintext binary over private colo network. Security is physical/network-layer, not application-layer. Sequence numbers provide integrity detection (not authentication). A/B feed divergence could theoretically detect injection but isn't currently checked.

**Verified working:** real ITCH data flowing through the full pipeline — broadcaster → receiver → processor → ring → book builder. Books building correctly for real stocks (AAPL at ~$162, AMZN at ~$1618, NVDA at ~$134) with plausible depth and tight spreads.

**Not built / deferred:**
- Queue position tracking (FIFO order within a price level) — have aggregate qty but not individual order positioning per level.
- Snapshot resync — tearing down and rebuilding a book after a gap. Would require building a separate order book + orders dict in Module 1, deferred.
- Unknown-order guard — deliberately omitted to catch errors early.
- `best_bid`/`best_ask` edge case when SortedDict is empty — basic guard returning 0 in place, but 0 is a valid (if meaningless) price.

**Status:** Module 3 core complete. Full three-process pipeline working end-to-end: feed handler (stateless aside from sequence tracking and lightweight order_sides dict) → shared-memory ring → book builder (owns all market state). Architecture matches the Module 1/2/3 separation from the design doc.

---

## 2026-08-31 — Gating, non-gating consumers, and the LMAX Disruptor

**Context:** compared the ring buffer implementation against the LMAX Disruptor paper (Thompson et al., 2011) and similar C++ ITCH feed handler repos on GitHub. Identified the core Disruptor features missing from the current design, then implemented gating.

**Built:**
- Expanded shared memory layout from `[write_seq][event buffer]` to `[write_seq 8B][consumer_count 8B][cursors MAX_CONSUMERS×8B][gating_flags MAX_CONSUMERS×1B][event buffer]`. All new fields are numpy arrays backed by shared memory slices, same pattern as `write_seq`. `MAX_CONSUMERS = 4` defined in `constants.py`, pre-allocated regardless of how many consumers register.
- `Ring.register(gating=True/False)` — assigns a cursor slot in shared memory, sets the gating flag, initializes the cursor to current `write_seq`. Returns a consumer ID (integer index). Consumer count tracked in shared memory so both producer and consumer processes can see it.
- `Ring.read(consumer_id)` replaces `Ring.read(cursor)` — the ring now owns the cursor in shared memory. Consumer no longer tracks its own cursor as a local Python integer. The ring looks up the cursor by consumer ID, reads the event, advances the cursor in shared memory, and returns the event.
- `Ring.write(data)` — now checks `write_seq - min_gating_cursor >= capacity` before writing. If true, returns `False` (ring full relative to the slowest gating consumer). Producer spins with `while not ring.write(...): pass` in every message handler.
- `Ring._scan_gating_min()` — scans all registered cursor slots where the gating flag is set, returns the minimum. Only called when the cached value suggests the ring might be full.
- Cached `_cached_min_gated` — plain Python int (not in shared memory, producer-local). Since cursors only move forward, a stale cache is always conservative. The producer only rescans when the cache says the ring looks full. Common-path `write()` does one subtraction and one comparison with no shared memory reads.
- Logger process — non-gating consumer. Registers with `gating=False`. Buffers events in a Python list, flushes to disk every N events or when the ring is empty. Pipe-delimited format. `format_event()` resolves instrument IDs via the shared `instrument_map`.
- Lapping detection restored for non-gating consumers: `read()` checks `write_seq - cursor > capacity` and returns `("LAPPED", gap_size)` so the consumer can log the gap and jump forward. Gating consumers can never hit this path — the producer spins instead.
- Shared memory zeroed on creation in `main()` (`shm.buf[:] = b'\x00' * shm_size`) to ensure all counters, cursors, and flags start at 0.
- `shm_size` calculation in `main()` updated to include the new fields: `8 + 8 + (MAX_CONSUMERS * 8) + MAX_CONSUMERS + (capacity * EVENT.itemsize)`.
- Removed dead code: walrus operator captures of `ring.write()` return values (now just `True`/`False`), stale `return [event]` lines in handlers, old lapping detection block in the book builder consumer (can't be lapped anymore).

**Tested and verified:**
- Added `time.sleep(0.001)` to the book builder to artificially slow it. Producer printed `RING FULL` messages and spun — confirmed gating prevents lapping. Removed the sleep afterward.
- At 100x replay speed, observed the backpressure chain: consumer slower than producer → ring fills → producer spins on `write()` → processor stops draining `raw_queue` → `raw_queue` grows unboundedly (processor process reached ~818 MB). This is correct behavior — gating shifts the cost from silent data corruption to visible memory growth. At 1x (live market speed) the consumer would likely keep up and the queue would stay small.
- After broadcaster finishes, the pipeline continues processing the backlog. NVDA prices still updating minutes after the broadcaster stopped — the processor is draining queued packets through the ring into the book builder. Pipeline finishes when the backlog clears.
- Profiled all six Python processes via Task Manager: identified processor (~818 MB, highest CPU — ITCH parsing + gating spin), book builder (~251 MB — orders dict + SortedDicts), logger (~38 MB), receiver (~41 MB), main (~37 MB, idle on `join()`), Manager (~36 MB, idle).

**Learned / decided:**
- The LMAX Disruptor is SPMC on one ring with independent cursors — not SPSC. The C++ GitHub repos (harris2001/UltraLowLatencyFeedHandler, mickelsamuel/dpdk-itch5-feedhandler) use SPSC because they're portfolio projects scoping down to one consumer. The Disruptor's dependency graph (consumers gating on other consumers, not just the producer) is the genuinely novel contribution beyond the ring buffer itself.
- Gating vs non-gating is a per-consumer decision answering one question: "is this consumer allowed to slow the system down?" Book builder gates (missed deltas corrupt the book). Logger doesn't (missed events are a logging gap, not a correctness failure).
- `raw_queue` (`multiprocessing.Queue`) cannot have a `maxsize` — blocking the receiver means not draining the socket, which means kernel buffer overflow, which means packet drops. The queue must be unbounded because it buffers between the network (which you can't slow down) and the processor (which might stall on gating).
- Consumer cursors must be in shared memory (numpy arrays backed by `shm.buf` slices), not local Python variables, because the producer and consumers are separate processes with separate address spaces. A local variable is invisible across process boundaries — same reason `write_seq` is a numpy view into shared memory.
- `np.uint64(0)` is an immutable scalar — `+= 1` rebinds the name without touching shared memory. `np.ndarray(1, dtype=np.uint64, buffer=shm.buf[...])` is a view — `[0] += 1` mutates the shared memory bytes in place. This is why every shared field uses ndarray, not bare numpy scalars.
- Scanning all gating cursors to find the minimum doesn't need a min-heap — `MAX_CONSUMERS` is 4, so the scan is 4 comparisons. A heap would add maintenance cost on every `read()` (cursor advance) to save a few comparisons on a scan that only runs when the cache says the ring might be full.
- `gating_flags` dtype must be `np.uint8`, not `np.uint64` — the allocated byte range (4 bytes for MAX_CONSUMERS=4) doesn't fit 4 uint64 values.

**Still missing vs the Disruptor (deferred):**
- Batch consumption — consumer reads `write_seq` once and processes all available events in a tight loop instead of checking per event.
- Torn-read double-check (seqlock) — non-gating consumers can be lapped mid-copy. Current `read()` checks before the copy but not after.
- Dependency graph — consumers gating on other consumers' cursors, not just the producer's.
- Wait strategies — pluggable spin/yield/sleep/block per consumer.
- End-of-session detection — broadcaster finishes but all processes hang forever. MoldUDP64's `0xFFFF` message count should propagate a shutdown signal.

**Status:** Ring buffer now implements LMAX Disruptor-style gating. Book builder is a gating consumer (can never be lapped). Logger is a non-gating consumer (can be lapped, logs gaps). Producer spins on full ring rather than overwriting. Five-process architecture: receiver → processor → ring → {book builder (gating), logger (non-gating)}, plus main and Manager.

## 2026-09-02 — Trade relay, WebSocket server, feed handler fixes

**Context:** Preparing the pipeline for a frontend visualization layer. Needed to get trade events and book snapshots out to a browser.

**Feed handler fixes:**
- Discovered `OrderExecutedWithPriceMessage` (ITCH type 'C') was never registered — trades where hidden/reserve orders get price improvement were silently dropped. Registered the handler. The book mutation is identical to type 'E' (look up order by ID, decrement quantity at the resting price level). The execution price field only matters downstream for OHLCV/trade logs, not for the book.
- Expanded `orders` dict in the feed handler from `order_id → buy_sell_indicator` to `order_id → (buy_sell_indicator, price)`. Same lifecycle (populated on Add, updated on Replace, deleted on Delete/full execution). This lets the feed handler stamp resolved trade prices onto `Action.EXECUTE` ring events so downstream consumers don't need their own order lookups. Previously type 'E' ring events carried `-1` for price.
- Side is now also stamped onto execution ring events instead of `255` (UNKNOWN), since the feed handler has the info from the orders dict anyway.
- Noted but deferred: the feed handler doesn't track remaining quantity, so it never cleans up `orders` on partial execution. Slow memory leak over a session — tolerable for the sample file, needs a fix for longer runs.

**Key decision:** OHLCV bar aggregation belongs on the frontend, not in a ring consumer process. OHLCV is a monitoring/visualization concern, not a trading one. The frontend receives raw trade events and builds bars at whatever timeframe the user selects — timeframe switching is instant with no backend changes.

**Built:**
- `trade_relay.py` — gating ring consumer that filters for `Action.EXECUTE` events and forwards `(instrument_id, price, quantity, ts_event)` tuples to a `multiprocessing.Queue`. Gating because missing a trade means wrong bars (completeness), not because it's latency-sensitive. The work per event is trivial (one comparison, occasional `queue.put()`), so it should never be the bottleneck gating consumer.
- `ws_server.py` — asyncio WebSocket server using the `websockets` library. Two concurrent tasks via `asyncio.gather`:
  - `broadcast_trades` — reads from `trade_queue` using `loop.run_in_executor()` to bridge blocking `queue.get()` with the async event loop. Broadcasts each trade as JSON to all connected clients.
  - `broadcast_snapshots` — every 200ms, iterates `instrument_map`, reads each instrument's MBP-10 snapshot from shared memory using the seqlock pattern, bundles all into one JSON message, and broadcasts. Skips instruments with timestamp 0 (never written) and torn reads.
- `MAX_CONSUMERS` bumped from 4 to 8 to accommodate the new trade relay consumer. Fixed a bug where cursor slice bounds in `Ring.__init__` were hardcoded for `MAX_CONSUMERS=4` — replaced magic numbers with computed offsets from `MAX_CONSUMERS`.

**Bugs encountered:**
- Naming the WebSocket server file `websocket.py` shadowed the `websockets` library import. Renamed.
- Missing `await` on `asyncio.gather()` in the `run()` function caused the server to start, fire off tasks, immediately exit the `async with` block, and cancel everything. Surfaced as `CancelledError` on `asyncio.sleep()` in the snapshot task — the real error was masked because the gather's future exception was never retrieved.

**Architecture:**
- Seven-process system: receiver → processor → ring → {book builder (gating), 
                                             → logger (non-gating), 
                                             → trade relay (gating)} + WebSocket server (reads snapshot shared memory + trade queue). 
- Trade relay → `multiprocessing.Queue` → WebSocket is the correct split because the relay's job is a tight ring-reading loop that shouldn't be blocked by WebSocket I/O. The queue is manageable because trade volume is a small fraction of total events (most are adds/cancels/deletes).
- WebSocket server is not a ring consumer. It reads two data sources: trade queue (for live trades) and snapshot shared memory (for book state). Decoupled from the ring entirely.

**On the horizon:**
- Frontend: React + Vite + TradingView Lightweight Charts for candlesticks, custom price ladder for MBP-10 depth. Scaffolding deferred to weekend.
- WebSocket backfill: server should aggregate 1-second bars in memory so clients connecting mid-session get bar history. Deferred until frontend exists.
- Feed handler `orders` cleanup on partial execution.

**Status:** Full pipeline live from UDP multicast through to browser console. Trade events and MBP-10 book snapshots streaming over WebSocket. Frontend visualization is the next milestone.