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
from typing import Set, List, Optional

import serial
import websockets
from janus import Queue as JQueue
from websockets.exceptions import ConnectionClosedOK

from parse import Message, Parser


@dataclass
class Series:
    timestamps: List[int]
    instant_power_1: List[float]
    instant_power_2: List[float]
    instant_power_3: List[float]

    def to_json(self):
        return {
            "timestamps": self.timestamps,
            "values": {
                "p1": self.instant_power_1,
                "p2": self.instant_power_2,
                "p3": self.instant_power_3,
            }
        }


@dataclass
class BroadcastMessage:
    fast: Series
    medium: Series
    slow: Series


class Tracker:
    def __init__(self):
        self.last_timestamp: Optional[int] = None

        self.history = Series([], [], [], [])

    def process_message(self, database: Connection, msg: Message):
        is_first = self.last_timestamp is None
        self.last_timestamp = msg.timestamp

        history_window = 20
        first_timestamp = msg.timestamp - history_window

        if is_first:
            # load initial history from database
            history_items = database.execute(
                "SELECT timestamp, instant_power_1, instant_power_2, instant_power_3 from meter_samples "
                "WHERE (timestamp > ?)",
                (first_timestamp,),
            ).fetchall()

            print(f"Fetched {len(history_items)} history points")

            for (prev_timestamp, prev_p1, prev_p2, prev_p3) in history_items:
                self.history.timestamps.append(prev_timestamp)
                self.history.instant_power_1.append(prev_p1)
                self.history.instant_power_2.append(prev_p2)
                self.history.instant_power_3.append(prev_p3)

        # append history
        self.history.timestamps.append(msg.timestamp)
        self.history.instant_power_1.append(msg.instant_power_1)
        self.history.instant_power_2.append(msg.instant_power_2)
        self.history.instant_power_3.append(msg.instant_power_3)

        # cap length
        kept_index = next((i for i, t in enumerate(self.history.timestamps) if t - first_timestamp > 0), 0)
        print(f"Dropping {kept_index} items")
        arrays = [self.history.timestamps, self.history.instant_power_1, self.history.instant_power_2,
                  self.history.instant_power_3]
        for arr in arrays:
            del arr[:kept_index]


class DataStore:
    def __init__(self, database: Connection):
        self.database = database
        self.tracker = Tracker()

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

        # calculate slower data
        self.tracker.process_message(self.database, msg)

        # broadcast to sockets
        with self.lock:
            for queue in self.broadcast_queues:
                queue.sync_q.put(msg)

    def add_broadcast_queue_get_data(self, queue: JQueue) -> Series:
        with self.lock:
            self.broadcast_queues.add(queue)
            return self.tracker.history

    def remove_broadcast_queue(self, queue: JQueue):
        with self.lock:
            self.broadcast_queues.remove(queue)


def run_serial_parser(message_queue: QQueue, log):
    port = serial.Serial(
        port='/dev/ttyS0',
        baudrate=115200,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.EIGHTBITS,
        timeout=10,
    )
    parser = Parser()

    while True:
        line = port.readline()
        if len(line) == 0:
            print("Timeout")
            parser.reset()
            continue

        try:
            line_str = line.decode()
        except UnicodeDecodeError:
            print("Unicode decode error")
            parser.reset()
            continue

        if log is not None:
            log.write(line_str)

        raw_msg = parser.push_line(line_str)

        if raw_msg is not None:
            msg = Message.from_raw(raw_msg)
            message_queue.put(msg)


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
    initial_history = store.add_broadcast_queue_get_data(queue)

    try:
        message = {"type": "initial", "series": initial_history.to_json()}
        print(f"Sending {message} to {websocket.remote_address}")

        while True:
            update_series = await queue.async_q.get()
            message = {"type": "update", "series": update_series.to_json()}
            print(f"Sending {message} to {websocket.remote_address}")
            await websocket.send(json.dumps(message))

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


def main():
    use_dummy = True

    message_queue = QQueue()

    database = sqlite3.connect("dummy.db")
    store = DataStore(database)

    if use_dummy:
        Thread(target=run_dummy_parser, args=(message_queue,)).start()
    else:
        with open("log.txt", "a") as log:
            Thread(target=run_serial_parser, args=(message_queue, log)).start()

    Thread(target=run_asyncio_main, args=(store,)).start()

    run_message_processor(store, message_queue)


if __name__ == '__main__':
    main()
    print("Main has ended")
