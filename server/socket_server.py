import asyncio
import functools
import itertools
import json
import math
import random
import sqlite3
import time
from dataclasses import dataclass
from queue import Queue as QQueue
from sqlite3 import Connection
from threading import Thread, Lock
from typing import Set, Optional, Callable, List

import websockets
from janus import Queue as JQueue
from websockets.exceptions import ConnectionClosedOK

from parse import Message


@dataclass
class Series:
    window_size: int
    bucket_size: int

    timestamps: List[int]
    instant_power_1: List[float]
    instant_power_2: List[float]
    instant_power_3: List[float]

    @staticmethod
    def empty(window_size: int, bucket_size: int):
        return Series(window_size, bucket_size, [], [], [], [])

    def to_json(self):
        return {
            "window_size": self.window_size,
            "timestamps": self.timestamps,
            "values": {
                "p1": self.instant_power_1,
                "p2": self.instant_power_2,
                "p3": self.instant_power_3,
            }
        }

    def clone(self):
        return Series(
            self.window_size,
            self.bucket_size,
            list(self.timestamps),
            list(self.instant_power_1),
            list(self.instant_power_2),
            list(self.instant_power_3),
        )

    def _drop_old(self):
        if len(self.timestamps) == 0:
            return

        last = self.timestamps[-1]
        kept_index = next((i for i, t in enumerate(self.timestamps) if last - t < self.window_size), 0)
        arrays = [self.timestamps, self.instant_power_1, self.instant_power_2, self.instant_power_3]
        for arr in arrays:
            del arr[:kept_index]

    def extend_items(self, items):
        for t, p1, p2, p3 in items:
            self.timestamps.append(t)
            self.instant_power_1.append(p1)
            self.instant_power_2.append(p2)
            self.instant_power_3.append(p3)
        self._drop_old()

    def append_msg(self, msg: Message):
        self.timestamps.append(msg.timestamp)
        self.instant_power_1.append(msg.instant_power_1)
        self.instant_power_2.append(msg.instant_power_2)
        self.instant_power_3.append(msg.instant_power_3)
        self._drop_old()


@dataclass
class MultiSeries:
    short: Series
    long: Series

    def to_json(self):
        return {
            "short": self.short.to_json(),
            "long": self.long.to_json(),
        }

    def clone(self):
        return MultiSeries(
            short=self.short.clone(),
            long=self.long.clone(),
        )


@dataclass
class BroadcastMessage:
    fast: Series
    medium: Series
    slow: Series


# TODO send nan for missing values instead of nothing, so JS doesn't just interpolate

# TODO implement this is a lazy way
def fetch_series(database: Connection, last_timestep: int, window_size: int, bucket_size: int) -> Series:
    oldest = last_timestep // bucket_size * bucket_size - window_size - bucket_size
    newest = last_timestep // bucket_size * bucket_size - bucket_size

    print(f"last timestep: {last_timestep}")
    print("fetching with args", (window_size, oldest))

    items = database.execute(
        "WITH const as (SELECT ? as bucket_size, ? as oldest, ? as newest) "
        "SELECT timestamp / bucket_size * bucket_size, "
        "AVG(instant_power_1),"
        "AVG(instant_power_2),"
        "AVG(instant_power_3)"
        "FROM meter_samples, const "
        "WHERE timestamp / bucket_size * bucket_size BETWEEN oldest AND newest "
        "GROUP BY timestamp / bucket_size "
        "ORDER BY timestamp ",
        (bucket_size, oldest, newest)
    )

    series = Series.empty(window_size, bucket_size)
    series.extend_items(items)

    return series


class Tracker:
    def __init__(self, history_window_size: int):
        self.history_window_size = history_window_size
        self.last_timestamp: Optional[int] = None

        self.series = MultiSeries(
            Series.empty(20, 1),
            Series.empty(200, 4),
        )

    def process_message(self, database: Connection, msg: Message):
        is_first = self.last_timestamp is None
        self.last_timestamp = msg.timestamp

        # TODO implement caching for this, or at least don't do it every time
        self.series.short = fetch_series(
            database, self.last_timestamp,
            self.series.short.window_size, self.series.short.bucket_size
        )
        self.series.long = fetch_series(
            database, self.last_timestamp,
            self.series.long.window_size, self.series.long.bucket_size
        )

        # TODO get this to properly match the factors used instead of just mucking with time formatting
        #  maybe something with abs and mod can do the trick?
        # TODO we need separate window_size and bucket_size

        # return MultiSeries(
        #     Series(
        #         self.series.short.window_size,
        #         [msg.timestamp], [msg.instant_power_1], [msg.instant_power_2], [msg.instant_power_3]
        #     ),
        #     Series(self.series.long.window_size, [], [], [], []),
        # )

        # todo we're sending a lot of duplicate values here...
        return self.series.clone()

    def get_history(self) -> MultiSeries:
        return self.series.clone()


class DataStore:
    def __init__(self, database: Connection, history_window_size: int):
        self.database = database
        self.tracker = Tracker(history_window_size)
        self.history_window_size = history_window_size

        self.lock = Lock()
        self.broadcast_queues: Set[JQueue] = set()

        self.database.execute(
            "CREATE TABLE IF NOT EXISTS meter_samples("
            "    timestamp INTEGER PRIMARY KEY,"
            "    instant_power_1 REAL,"
            "    instant_power_2 REAL,"
            "    instant_power_3 REAL"
            ")"
        )
        self.database.execute(
            "CREATE TABLE IF NOT EXISTS meter_peaks("
            "    timestamp INTEGER PRIMARY KEY, "
            "    instant_power_total REAL"
            ")"
        )
        self.database.commit()

    def process_message(self, msg: Message):
        with self.lock:
            print(f"Processing message {msg}")

            # add to database
            if msg.timestamp is not None:
                self.database.execute(
                    "INSERT OR REPLACE INTO meter_samples VALUES(?, ?, ?, ?)",
                    (msg.timestamp, msg.instant_power_1, msg.instant_power_2, msg.instant_power_3),
                )
            if msg.peak_power_timestamp is not None:
                self.database.execute(
                    "INSERT OR REPLACE INTO meter_peaks VALUES(?, ?)",
                    (msg.peak_power_timestamp, msg.peak_power)
                )
            self.database.commit()

            # update trackers
            # careful, we've already added the new values to the database
            update_series = self.tracker.process_message(self.database, msg)

            # broadcast update series to sockets
            for queue in self.broadcast_queues:
                queue.sync_q.put(update_series)

    def add_broadcast_queue_get_data(self, queue: JQueue) -> MultiSeries:
        with self.lock:
            self.broadcast_queues.add(queue)
            return self.tracker.get_history()

    def remove_broadcast_queue(self, queue: JQueue):
        with self.lock:
            self.broadcast_queues.remove(queue)


def run_dummy_parser(message_queue: QQueue):
    for _ in itertools.count():
        time.sleep(1)

        t = time.time()

        ya = math.sin(t * 0.1) + random.random() * 0.1
        yb = math.sin(t * 0.2) + random.random() * 0.2
        yc = math.sin(t * 0.5) + random.random() * 0.05

        msg = Message(int(t), ya, yb, yc, math.nan, 0)
        message_queue.put(msg)


async def handler(websocket, store: DataStore):
    print(f"Accepted connection from {websocket.remote_address}")

    queue = JQueue()

    try:
        series = store.add_broadcast_queue_get_data(queue)
        response = {
            "type": "initial",
            "series": series.to_json()
        }

        print(f"Sending {response} to {websocket.remote_address}")
        await websocket.send(json.dumps(response))

        while True:
            update_series = await queue.async_q.get()
            # TODO change back to "update"
            response = {"type": "initial", "series": update_series.to_json()}

            print(f"Sending {response} to {websocket.remote_address}")
            await websocket.send(json.dumps(response))

    # TODO add other exceptions that should be ignored
    except ConnectionClosedOK:
        pass
    finally:
        store.remove_broadcast_queue(queue)


def run_asyncio_main(store: DataStore):
    async def async_main():
        async with websockets.serve(functools.partial(handler, store=store), "", 8001):
            await asyncio.Future()  # run forever

    asyncio.run(async_main())


def run_message_processor(store: DataStore, message_queue: QQueue):
    while True:
        message = message_queue.get()
        store.process_message(message)


def run_socket_server(generator: Callable[[QQueue], None], database_path: str, ):
    message_queue = QQueue()

    database = sqlite3.connect(database_path)
    store = DataStore(database, 60)

    Thread(target=generator, args=(message_queue,)).start()
    Thread(target=run_asyncio_main, args=(store,)).start()

    run_message_processor(store, message_queue)


if __name__ == '__main__':
    run_socket_server(run_dummy_parser, "dummy.db")
    print("Main has ended")
