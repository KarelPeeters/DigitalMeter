#!/usr/bin/env python

import asyncio
import functools
import itertools
import json
import math
import random
import time
from dataclasses import dataclass
from threading import Thread, Lock
from typing import Set, Callable, Dict

import janus
import websockets
from websockets.exceptions import ConnectionClosedOK


class Broadcast:
    def __init__(self):
        self.lock = Lock()
        self.sync_queues: Set[janus.SyncQueue] = set()

    def send(self, x):
        with self.lock:
            for queue in self.sync_queues:
                queue.put(x)

    def add_queue(self, queue: janus.SyncQueue):
        with self.lock:
            self.sync_queues.add(queue)

    def remove_queue(self, queue: janus.SyncQueue):
        with self.lock:
            self.sync_queues.remove(queue)


async def handler(websocket, broadcast: Broadcast):
    print("Accepted connection!")

    queue = janus.Queue()
    broadcast.add_queue(queue.sync_q)

    try:
        while True:
            # immediately get multiple items to save network overhead
            data = [await queue.async_q.get()]
            while True:
                try:
                    data.append(queue.async_q.get_nowait())
                except janus.AsyncQueueEmpty:
                    break

            data_json = [point.to_json() for point in data]
            print(f"Sending {data_json}")
            await websocket.send(json.dumps(data_json))

    except ConnectionClosedOK:
        broadcast.remove_queue(queue.sync_q)


async def async_main(broadcast: Broadcast):
    async with websockets.serve(functools.partial(handler, broadcast=broadcast), "", 8001):
        await asyncio.Future()  # run forever


def dummy_generator(broadcast: Broadcast):
    for _ in itertools.count():
        time.sleep(0.2)

        t = time.time()

        ya = math.sin(t * 0.1) + random.random() * 0.1
        yb = math.sin(t * 0.2) + random.random() * 0.2
        yc = math.sin(t * 0.5) + random.random() * 0.05

        point = GraphPoint(t, {"a": ya, "b": yb, "c": yc})
        broadcast.send(point)


@dataclass
class GraphPoint:
    x: float
    y_all: Dict[str, float]

    def to_json(self) -> Dict:
        return {
            "t": self.x,
            "y_all": self.y_all,
        }


def run_grapher(generator: Callable[[Broadcast], None]):
    broadcast = Broadcast()

    thread = Thread(target=generator, args=(broadcast,))
    thread.start()

    asyncio.run(async_main(broadcast))


if __name__ == "__main__":
    run_grapher(dummy_generator)
