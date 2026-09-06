import asyncio
import json
import websockets
from multiprocessing import shared_memory
from ring_buffer import Ring
from constants import *
import numpy as np

trade_history = []

async def handle_client(ws):
    if trade_history:
        msg = json.dumps({"type": "trade_batch", "trades": trade_history})
        await ws.send(msg)
    await ws.wait_closed()

async def broadcast_trades(server, trade_ring, ws_consumer_id, instrument_map):
    while True:
        await asyncio.sleep(0.05)
        # print(f"WS poll: trade_ring write_seq={int(trade_ring.write_seq[0])}, read_pos={ws_consumer_id}, cursor={int(trade_ring.cursors[ws_consumer_id])}")
        batch = []
        while len(batch) < 1000:
            result = trade_ring.read(ws_consumer_id)
            if result is None:
                break

            instrument_id = int(result['instrument_id'])
            symbol = instrument_map.get(instrument_id, str(instrument_id))
            batch.append({
                "symbol": symbol,
                "price": int(result['price']),
                "quantity": int(result['quantity']),
                "ts_event": int(result['ts_event']),
            })

        if not batch:
            continue  # keep the loop going

        trade_history.extend(batch)
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


async def run(trade_ring_shm_name, trade_ring_capacity, ws_consumer_id, instrument_map, host="0.0.0.0", port=8765):
    snapshot_shm = shared_memory.SharedMemory(name=SNAPSHOT_SHM_NAME, create=False)
    snapshots = np.ndarray(MAX_INSTRUMENTS, dtype=SNAPSHOT_DTYPE, buffer=snapshot_shm.buf)

    trade_shm = shared_memory.SharedMemory(name=trade_ring_shm_name, create=False)
    trade_ring = Ring(trade_shm, trade_ring_capacity)

    async with websockets.serve(handle_client, host, port, origins=None) as server:
        print(f"WebSocket server running on ws://{host}:{port}")
        await asyncio.gather(
            broadcast_trades(server, trade_ring, ws_consumer_id, instrument_map),
            broadcast_snapshots(server, snapshots, instrument_map),
        )


def ws_server(trade_ring_shm_name, trade_ring_capacity, ws_consumer_id, instrument_map):
    asyncio.run(run(trade_ring_shm_name, trade_ring_capacity, ws_consumer_id, instrument_map))