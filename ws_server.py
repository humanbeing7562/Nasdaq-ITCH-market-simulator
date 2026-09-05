import asyncio
import json
import websockets
from multiprocessing import shared_memory
from constants import *
import numpy as np


async def broadcast_trades(server, trade_shm, instrument_map):
    write_seq = np.ndarray(1, dtype=np.uint64, buffer=trade_shm.buf[0:8])
    trades = np.ndarray(TRADE_BUFFER_SIZE, dtype=TRADE_DTYPE, buffer=trade_shm.buf[8:])
    mask = TRADE_BUFFER_SIZE - 1

    read_pos = int(write_seq[0])

    while True:
        await asyncio.sleep(0.05)

        current_write = int(write_seq[0])
        if current_write == read_pos:
            continue

        # if we fell behind more than buffer size, jump forward
        if current_write - read_pos > TRADE_BUFFER_SIZE:
            read_pos = current_write - TRADE_BUFFER_SIZE

        batch = []
        while read_pos < current_write and len(batch) < 1000:
            slot = read_pos & mask
            t = trades[slot]
            symbol = instrument_map.get(int(t['instrument_id']), str(int(t['instrument_id'])))
            batch.append({
                "symbol": symbol,
                "price": int(t['price']),
                "quantity": int(t['quantity']),
                "ts_event": int(t['ts_event']),
            })
            read_pos += 1

        if batch:
            msg = json.dumps({"type": "trade_batch", "trades": batch})
            clients = list(server.connections)
            if clients:
                await asyncio.gather(
                    *[client.send(msg) for client in clients],
                    return_exceptions=True
                )


async def broadcast_snapshots(server, snapshots, instrument_map):
    while True:
        await asyncio.sleep(0.2)

        clients = list(server.connections)
        if not clients:
            continue

        book_msgs = []
        for stock_locate, symbol in list(instrument_map.items()):
            data = read_snapshot(snapshots, stock_locate)
            if data is None:
                continue
            if int(data['timestamp']) == 0:
                continue

            book_msgs.append({
                "type": "book",
                "symbol": symbol,
                "bids": [
                    {"price": int(data['bid_price'][i]), "qty": int(data['bid_qty'][i])}
                    for i in range(10) if int(data['bid_price'][i]) != 0
                ],
                "asks": [
                    {"price": int(data['ask_price'][i]), "qty": int(data['ask_qty'][i])}
                    for i in range(10) if int(data['ask_price'][i]) != 0
                ],
            })

        if book_msgs:
            msg = json.dumps({"type": "book_update", "books": book_msgs})
            await asyncio.gather(
                *[client.send(msg) for client in clients],
                return_exceptions=True
            )


def read_snapshot(snapshots, instrument_id):
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


async def run(instrument_map, host="0.0.0.0", port=8765):
    snapshot_shm = shared_memory.SharedMemory(name=SNAPSHOT_SHM_NAME, create=False)
    snapshots = np.ndarray(MAX_INSTRUMENTS, dtype=SNAPSHOT_DTYPE, buffer=snapshot_shm.buf)

    trade_shm = shared_memory.SharedMemory(name=TRADE_SHM_NAME, create=False)

    async with websockets.serve(lambda ws: ws.wait_closed(), host, port, origins=None) as server:
        print(f"WebSocket server running on ws://{host}:{port}")
        await asyncio.gather(
            broadcast_trades(server, trade_shm, instrument_map),
            broadcast_snapshots(server, snapshots, instrument_map),
        )


def ws_server(instrument_map):
    asyncio.run(run(instrument_map))