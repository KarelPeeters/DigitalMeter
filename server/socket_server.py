import asyncio
import functools

import simplejson
import websockets
from janus import Queue as JQueue
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

from server.data import MultiSeries, DataStore


async def handler(websocket, store: DataStore):
    print(f"Accepted connection from {websocket.remote_address}")

    queue = JQueue()

    try:
        initial_series: MultiSeries = store.add_broadcast_queue_get_data(queue)
        response = {"type": "initial", "series": initial_series.to_json()}
        print(
            f"Sending response type 'initial' with series {list(initial_series.map.keys())} to {websocket.remote_address}")
        await websocket.send(simplejson.dumps(response, ignore_nan=True))

        while True:
            update_series: MultiSeries = await queue.async_q.get()
            response = {"type": "update", "series": update_series.to_json()}
            print(
                f"Sending response type 'update' with series {list(update_series.map.keys())} to {websocket.remote_address}")
            await websocket.send(simplejson.dumps(response, ignore_nan=True))

    except (ConnectionClosedError, ConnectionClosedOK):
        print(f"Client disconnected {websocket.remote_address}")
    finally:
        store.remove_broadcast_queue(queue)


def socket_server_main(store: DataStore):
    async def async_main():
        async with websockets.serve(functools.partial(handler, store=store), "", 8001):
            await asyncio.Future()  # run forever

    print("Starting socket server")
    asyncio.run(async_main())
