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

(TBC)

## Book (Module 3) (IN PROGRESS)

(TBC)

## Strategy (Module 4) (IN PROGRESS) (NOT A FULL TRADING STRATEGY, BUT JUST TO MIMIC BUYS AND SELLS)

(TBC)

## Order Gateway (Module 5) (IN PROGRESS)

(TBC)

## Risk Mitigation (Module 6) (IN PROGRESS)

(TBC)

