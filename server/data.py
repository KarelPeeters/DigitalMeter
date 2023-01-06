import sqlite3
from dataclasses import dataclass
from threading import Lock
from typing import List, Dict, Optional, Set

from janus import Queue as JQueue

from parse import Message


class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)

        result = self.conn.execute("PRAGMA journal_mode=WAL;").fetchone()
        assert result == ("wal",), "Failed to switch to WAL mode"

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS meter_samples("
            "    timestamp INTEGER PRIMARY KEY,"
            "    timestamp_str TEXT,"
            "    instant_power_1 REAL,"
            "    instant_power_2 REAL,"
            "    instant_power_3 REAL"
            ")"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS meter_peaks("
            "    timestamp INTEGER PRIMARY KEY, "
            "    timestamp_str TEXT,"
            "    instant_power_total REAL"
            ")"
        )
        self.conn.commit()

    def insert(self, msg: Message):
        if msg.timestamp is not None:
            self.conn.execute(
                "INSERT OR REPLACE INTO meter_samples VALUES(?, ?, ?, ?, ?)",
                (msg.timestamp, msg.timestamp_str, msg.instant_power_1, msg.instant_power_2, msg.instant_power_3),
            )
        if msg.peak_power_timestamp is not None:
            self.conn.execute(
                "INSERT OR REPLACE INTO meter_peaks VALUES(?, ?, ?)",
                (msg.peak_power_timestamp, msg.peak_power_timestamp_str, msg.peak_power)
            )
        self.conn.commit()

    def fetch_series_items(self, bucket_size: int, oldest: Optional[int], newest: Optional[int]):
        """
        Fetch the buckets between `oldest` (inclusive) and `newest` (exclusive)`.
        """
        if oldest is not None and newest is not None:
            where_clause = f"WHERE {oldest} <= timestamp AND timestamp < {newest} "
        elif oldest is not None:
            where_clause = f"WHERE {oldest} <= timestamp "
        elif newest is not None:
            where_clause = f"WHERE timestamp < {newest} "
        else:
            where_clause = ""

        return self.conn.execute(
            "WITH const as (SELECT ? as bucket_size, ? as oldest, ? as newest) "
            "SELECT timestamp / bucket_size * bucket_size, "
            "AVG(instant_power_1),"
            "AVG(instant_power_2),"
            "AVG(instant_power_3)"
            "FROM meter_samples, const "
            f"{where_clause}"
            "GROUP BY timestamp / bucket_size "
            "ORDER BY timestamp ",
            (bucket_size, oldest, newest)
        )

    def close(self):
        self.conn.close()


@dataclass
class Buckets:
    window_size: int
    bucket_size: int

    def bucket_bounds(self, timestamp: int) -> (int, int):
        """
        Compute the bounds `min` (inclusive), `max` (exclusive) of all finished buckets,
        assuming the sample with `timestamp` is the latest one in the database.
        """
        oldest = (timestamp + 1) // self.bucket_size * self.bucket_size - self.window_size
        newest = (timestamp + 1) // self.bucket_size * self.bucket_size
        return oldest, newest


@dataclass
class Series:
    buckets: Buckets

    timestamps: List[int]
    instant_power_1: List[float]
    instant_power_2: List[float]
    instant_power_3: List[float]

    @staticmethod
    def empty(buckets: Buckets):
        return Series(buckets, [], [], [], [])

    def to_json(self):
        return {
            "window_size": self.buckets.window_size,
            "bucket_size": self.buckets.bucket_size,

            "timestamps": self.timestamps,
            "values": {
                "p1": self.instant_power_1,
                "p2": self.instant_power_2,
                "p3": self.instant_power_3,
            }
        }

    def clone(self):
        return Series(
            self.buckets,
            list(self.timestamps),
            list(self.instant_power_1),
            list(self.instant_power_2),
            list(self.instant_power_3),
        )

    def _drop_old(self):
        if len(self.timestamps) == 0:
            return
        newest = self.timestamps[-1]
        self.drop_before(newest - self.buckets.window_size)

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


class Tracker:
    def __init__(self):
        self.last_timestamp: Optional[int] = None

        self.multi_series = MultiSeries({
            "minute": Series.empty(Buckets(60, 1)),
            "hour": Series.empty(Buckets(60 * 60, 10)),
            "day": Series.empty(Buckets(24 * 60 * 60, 60)),
            "week": Series.empty(Buckets(7 * 24 * 60 * 60, 15 * 60)),
        })

    def process_message(self, database: Database, msg: Message):
        prev_timestamp = self.last_timestamp
        curr_timestamp = msg.timestamp
        self.last_timestamp = curr_timestamp

        delta_multi_series = MultiSeries({})

        for key in self.multi_series.map:
            series = self.multi_series.map[key]
            curr_oldest, curr_newest = series.buckets.bucket_bounds(curr_timestamp)

            if prev_timestamp is None:
                # fetch the entire series
                print(f"Fetching entire series for '{key}'")
                new_items = database.fetch_series_items(series.buckets.bucket_size, curr_oldest, curr_newest).fetchall()
            else:
                # only fetch new buckets if any
                _, prev_newest = series.buckets.bucket_bounds(prev_timestamp)
                if curr_newest == prev_newest:
                    continue
                else:
                    # print(f"Fetching new buckets for '{key}'")
                    new_items = database.fetch_series_items(series.buckets.bucket_size, prev_newest,
                                                            curr_newest).fetchall()

            # put into cached series
            series.extend_items(new_items)

            # put into delta series
            delta_series = Series.empty(series.buckets)
            delta_series.extend_items(new_items)
            delta_multi_series.map[key] = delta_series

        return delta_multi_series

    def get_history(self) -> MultiSeries:
        return self.multi_series.clone()


class DataStore:
    def __init__(self, database: Database):
        self.database = database
        self.tracker = Tracker()

        self.lock = Lock()
        self.broadcast_queues: Set[JQueue] = set()

    def process_message(self, msg: Message):
        with self.lock:
            # print(f"Processing message {msg}")

            # add to database
            self.database.insert(msg)

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
