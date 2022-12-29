import asyncio
import functools
import itertools
import math
import random
import sqlite3
import time
from dataclasses import dataclass
from queue import Queue as QQueue
from sqlite3 import Connection
from threading import Thread, Lock
from typing import Set, Optional, Callable, List, Dict

import simplejson
import websockets
from janus import Queue as JQueue
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

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
            "bucket_size": self.bucket_size,

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
        newest = self.timestamps[-1]
        self.drop_before(newest - self.window_size)

    def drop_before(self, oldest):
        kept_index = next((i for i, t in enumerate(self.timestamps) if t >= oldest), 0)
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
    map: Dict[str, Series]

    def to_json(self):
        return {
            name: series.to_json() for name, series in self.map.items()
        }

    def clone(self):
        return MultiSeries({
            name: series.clone() for name, series in self.map.items()
        })


# TODO send nan for missing values instead of nothing, so JS doesn't just interpolate

def bucket_bounds(window_size: int, bucket_size: int, timestamp: int) -> (int, int):
    oldest = timestamp // bucket_size * bucket_size - window_size - bucket_size
    newest = timestamp // bucket_size * bucket_size - bucket_size
    return oldest, newest


def fetch_series_items(database: Connection, bucket_size: int, oldest: int, newest: int):
    return database.execute(
        "WITH const as (SELECT ? as bucket_size, ? as oldest, ? as newest) "
        "SELECT timestamp / bucket_size * bucket_size, "
        "AVG(instant_power_1),"
        "AVG(instant_power_2),"
        "AVG(instant_power_3)"
        "FROM meter_samples, const "
        "WHERE oldest < timestamp / bucket_size * bucket_size AND timestamp / bucket_size * bucket_size <= newest "
        "GROUP BY timestamp / bucket_size "
        "ORDER BY timestamp ",
        (bucket_size, oldest, newest)
    ).fetchall()


class Tracker:
    def __init__(self, history_window_size: int):
        self.history_window_size = history_window_size
        self.last_timestamp: Optional[int] = None

        self.multi_series = MultiSeries({
            "minute": Series.empty(60, 1),
            "hour": Series.empty(60 * 60, 10),
            "day": Series.empty(24 * 60 * 60, 24 * 10),
            "week": Series.empty(7 * 24 * 60 * 60, 7 * 24 * 10),
        })

    def process_message(self, database: Connection, msg: Message):
        prev_timestamp = self.last_timestamp
        self.last_timestamp = msg.timestamp

        delta_multi_series = MultiSeries({})

        for key in self.multi_series.map:
            series = self.multi_series.map[key]
            curr_oldest, curr_newest = bucket_bounds(series.window_size, series.bucket_size, self.last_timestamp)

            if prev_timestamp is None:
                # fetch the entire series
                new_items = fetch_series_items(database, series.bucket_size, curr_oldest, curr_newest)
            else:
                # only fetch new buckets
                _, prev_newest = bucket_bounds(series.window_size, series.bucket_size, prev_timestamp)
                if curr_newest == prev_newest:
                    continue
                else:
                    new_items = fetch_series_items(database, series.bucket_size, prev_newest, curr_newest)

            # put into cached series
            series.extend_items(new_items)

            # put into delta series
            delta_series = Series.empty(series.window_size, series.bucket_size)
            delta_series.extend_items(new_items)
            delta_multi_series.map[key] = delta_series

        return delta_multi_series

    def get_history(self) -> MultiSeries:
        return self.multi_series.clone()


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
            "    timestamp_str TEXT,"
            "    instant_power_1 REAL,"
            "    instant_power_2 REAL,"
            "    instant_power_3 REAL"
            ")"
        )
        self.database.execute(
            "CREATE TABLE IF NOT EXISTS meter_peaks("
            "    timestamp INTEGER PRIMARY KEY, "
            "    timestamp_str TEXT,"
            "    instant_power_total REAL"
            ")"
        )
        self.database.commit()

    def process_message(self, msg: Message):
        with self.lock:
            print(f"Processing message {msg}")

            # add to database
            start = time.perf_counter()
            if msg.timestamp is not None:
                self.database.execute(
                    "INSERT OR REPLACE INTO meter_samples VALUES(?, ?, ?, ?, ?)",
                    (msg.timestamp, msg.timestamp_str, msg.instant_power_1, msg.instant_power_2, msg.instant_power_3),
                )
            if msg.peak_power_timestamp is not None:
                self.database.execute(
                    "INSERT OR REPLACE INTO meter_peaks VALUES(?, ?, ?)",
                    (msg.peak_power_timestamp, msg.peak_power_timestamp_str, msg.peak_power)
                )
            self.database.commit()
            print(f"DB insert took {time.perf_counter() - start}")

            # update trackers
            # careful, we've already added the new values to the database
            start = time.perf_counter()
            update_series = self.tracker.process_message(self.database, msg)
            print(f"Tracker update took {time.perf_counter() - start}")

            # broadcast update series to sockets
            start = time.perf_counter()
            for queue in self.broadcast_queues:
                queue.sync_q.put(update_series)
            print(f"Broadcast took {time.perf_counter() - start}")

    def add_broadcast_queue_get_data(self, queue: JQueue) -> MultiSeries:
        with self.lock:
            self.broadcast_queues.add(queue)
            return self.tracker.get_history()

    def remove_broadcast_queue(self, queue: JQueue):
        with self.lock:
            self.broadcast_queues.remove(queue)


def run_dummy_parser(message_queue: QQueue):
    drop_count = 0

    for _ in itertools.count():
        time.sleep(1)

        # if drop_count > 0:
        #     print(f"Dropping message {drop_count}")
        #     drop_count -= 1
        #     continue
        # if random.random() < 0.05:
        #     drop_count = 5

        t = time.time()

        ya = math.sin(t * 0.1) + random.random() * 0.1
        yb = math.sin(t * 0.2) + random.random() * 0.2
        yc = math.sin(t * 0.5) + random.random() * 0.05

        msg = Message(int(t), "dummy", ya, yb, yc, math.nan, 0, "dummy")
        message_queue.put(msg)


async def handler(websocket, store: DataStore):
    print(f"Accepted connection from {websocket.remote_address}")

    queue = JQueue()

    try:
        initial_series: MultiSeries = store.add_broadcast_queue_get_data(queue)
        response = {"type": "initial", "series": initial_series.to_json()}
        print(
            f"Sending response type 'initial' with series {list(initial_series.map.keys())} to {websocket.remote_address}")
        # TODO replace nan with null in dumps
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
