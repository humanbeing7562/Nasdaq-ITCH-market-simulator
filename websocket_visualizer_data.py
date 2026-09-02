import asyncio
import json
import websockets
from multiprocessing import shared_memory
from constants import SNAPSHOT_SHM_NAME, SNAPSHOT_DTYPE, MAX_INSTRUMENTS
from order_book import read_snapshot
import numpy as np


async def broadcast_trades(websocket_server, trade_queue, instrument_map):
    loop = asyncio.get_event_loop()

    while True:
        instrument_id, price, quantity, ts_event = await loop.run_in_executor(
            None, trade_queue.get
        )

        symbol = instrument_map.get(instrument_id, str(instrument_id))

        msg = json.dumps({
            "type": "trade",
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "ts_event": ts_event,
        })

        clients = list(websocket_server.connections)
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
        for stock_locate, symbol in instrument_map.items():
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


async def run(trade_queue, instrument_map, host="0.0.0.0", port=8765):
    snapshot_shm = shared_memory.SharedMemory(name=SNAPSHOT_SHM_NAME, create=False)
    snapshots = np.ndarray(MAX_INSTRUMENTS, dtype=SNAPSHOT_DTYPE, buffer=snapshot_shm.buf)
 
    async with websockets.serve(lambda ws: ws.wait_closed(), host, port) as server:
        print(f"WebSocket server running on ws://{host}:{port}")
        await asyncio.gather(
            broadcast_trades(server, trade_queue, instrument_map),
            broadcast_snapshots(server, snapshots, instrument_map),
        )


def ws_server(trade_queue, instrument_map):
    asyncio.run(run(trade_queue, instrument_map))