import asyncio
import json
import websockets
from multiprocessing import shared_memory
from constants import SNAPSHOT_SHM_NAME, SNAPSHOT_DTYPE, MAX_INSTRUMENTS
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


async def run(trade_queue, instrument_map, host="0.0.0.0", port=8765):
    async with websockets.serve(lambda ws: ws.wait_closed(), host, port) as server:
        print(f"WebSocket server running on ws://{host}:{port}")
        await broadcast_trades(server, trade_queue, instrument_map)


def ws_server(trade_queue, instrument_map):
    asyncio.run(run(trade_queue, instrument_map))