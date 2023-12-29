import enum
import sqlite3
from dataclasses import dataclass
from threading import Lock
from typing import List, Dict, Optional, Set

from janus import Queue as JQueue

from parse import Message


@dataclass
class SeriesKindInfo:
    name: str
    table: str
    columns: List[str]
    unit_label: str


class SeriesKind(enum.Enum):
    POWER = SeriesKindInfo(
        name="power",
        table="meter_samples",
        columns=["instant_power_1", "instant_power_2", "instant_power_3"],
        unit_label="P (W)"
    )
    GAS = SeriesKindInfo(
        name="gas",
        table="gas_samples",
        columns=["volume"],
        unit_label="V (m^3)"
    )


def build_where_clause(oldest: Optional[int], newest: Optional[int]) -> str:
    if oldest is not None and newest is not None:
        where_clause = f"WHERE {oldest} <= timestamp AND timestamp < {newest} "
    elif oldest is not None:
        where_clause = f"WHERE {oldest} <= timestamp "
    elif newest is not None:
        where_clause = f"WHERE timestamp < {newest} "
    else:
        where_clause = ""
    return where_clause


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
            "    instant_power_3 REAL,"
            "    voltage_1 REAL,"
            "    voltage_2 REAL,"
            "    voltage_3 REAL"
            ")"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS meter_peaks("
            "    timestamp INTEGER PRIMARY KEY, "
            "    timestamp_str TEXT,"
            "    instant_power_total REAL"
            ")"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS gas_samples("
            "    timestamp INTEGER PRIMARY KEY,"
            "    timestamp_str TEXT,"
            "    volume REAL"
            ")"
        )
        self.conn.commit()

    def insert(self, msg: Message):
        if msg.timestamp is not None:
            self.conn.execute(
                "INSERT OR REPLACE INTO meter_samples VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (msg.timestamp, msg.timestamp_str, msg.instant_power_1, msg.instant_power_2, msg.instant_power_3,
                 msg.voltage_1, msg.voltage_2, msg.voltage_3),
            )
        if msg.peak_power_timestamp is not None:
            self.conn.execute(
                "INSERT OR REPLACE INTO meter_peaks VALUES(?, ?, ?)",
                (msg.peak_power_timestamp, msg.peak_power_timestamp_str, msg.peak_power)
            )
        if msg.gas_timestamp is not None:
            self.conn.execute(
                "INSERT OR REPLACE INTO gas_samples VALUES(?, ?, ?)",
                (msg.gas_timestamp, msg.gas_timestamp_str, msg.gas_volume)
            )
        self.conn.commit()

    # TODO decide a proper API for this, this kinda sucks
    #   maybe just have separate functions for power and gas, which then call an internal function?
    def fetch_series_items(self, kind: SeriesKind, bucket_size: int, oldest: Optional[int], newest: Optional[int]):
        """
        Fetch the buckets between `oldest` (inclusive) and `newest` (exclusive)`.
        """
        where_clause = build_where_clause(oldest, newest)

        # TODO change this to be "if recently there are multiple items per bucket"
        if bucket_size == 1:
            return self.conn.execute(
                "WITH const as (SELECT ? as oldest, ? as newest) "
                f"SELECT timestamp, {', '.join(kind.value.columns)} "
                f"FROM {kind.value.table}, const "
                f"{where_clause}"
                "ORDER BY timestamp ",
                (oldest, newest)
            )
        else:
            averages = ",\n".join(f"AVG({item})" for item in kind.value.columns)
            return self.conn.execute(
                "WITH const as (SELECT ? as bucket_size, ? as oldest, ? as newest) "
                "SELECT timestamp / bucket_size * bucket_size, "
                f"{averages}"
                f"FROM {kind.value.table}, const "
                f"{where_clause}"
                "GROUP BY timestamp / bucket_size "
                "ORDER BY timestamp ",
                (bucket_size, oldest, newest)
            )

    def close(self):
        self.conn.close()


@dataclass
class Buckets:
    window_size: Optional[int]
    bucket_size: int

    def bucket_bounds(self, timestamp: int) -> (int, int):
        """
        Compute the bounds `min` (inclusive), `max` (exclusive) of all finished buckets,
        assuming the sample with `timestamp` is the latest one in the database.
        """
        assert self.window_size is not None, "Cannot get bounds of bucket without window size"

        oldest = (timestamp + 1) // self.bucket_size * self.bucket_size - self.window_size
        newest = (timestamp + 1) // self.bucket_size * self.bucket_size
        return oldest, newest


@dataclass
class Series:
    kind: SeriesKind
    buckets: Buckets

    timestamps: List[int]
    values: List[List[float]]

    @staticmethod
    def empty(kind: SeriesKind, buckets: Buckets):
        return Series(kind=kind, buckets=buckets, timestamps=[], values=[[] for _ in kind.value.columns])

    def to_json(self):
        return {
            "window_size": self.buckets.window_size,
            "bucket_size": self.buckets.bucket_size,
            "kind": self.kind.value.name,
            "unit_label": self.kind.value.unit_label,

            "timestamps": self.timestamps,
            "values": self.values,
        }

    def clone(self):
        return Series(
            kind=self.kind,
            buckets=self.buckets,

            timestamps=list(self.timestamps),
            values=[list(x) for x in self.values]
        )

    def _drop_old(self):
        if len(self.timestamps) == 0 or self.buckets.window_size is None:
            return
        newest = self.timestamps[-1]
        self.drop_before(newest - self.buckets.window_size)

    def drop_before(self, oldest):
        kept_index = next((i for i, t in enumerate(self.timestamps) if t >= oldest), 0)

        del self.timestamps[:kept_index]
        for arr in self.values:
            del arr[:kept_index]

    def extend_items(self, items):
        for line in items:
            timestamp, *values = line
            self.timestamps.append(timestamp)
            for i, value in enumerate(values):
                self.values[i].append(value)
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
            "minute": Series.empty(SeriesKind.POWER, Buckets(60, 1)),
            "hour": Series.empty(SeriesKind.POWER, Buckets(60 * 60, 10)),
            "day": Series.empty(SeriesKind.POWER, Buckets(24 * 60 * 60, 60)),
            "week": Series.empty(SeriesKind.POWER, Buckets(7 * 24 * 60 * 60, 15 * 60)),
            "gas": Series.empty(SeriesKind.GAS, Buckets(7 * 24 * 60, 1)),
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
                new_items = (database.fetch_series_items(
                    series.kind, series.buckets.bucket_size, curr_oldest, curr_newest
                ).fetchall())
            else:
                # only fetch new buckets if any
                _, prev_newest = series.buckets.bucket_bounds(prev_timestamp)
                if curr_newest == prev_newest:
                    continue
                else:
                    # print(f"Fetching new buckets for '{key}'")
                    new_items = database.fetch_series_items(
                        series.kind, series.buckets.bucket_size, prev_newest, curr_newest
                    ).fetchall()

            # put into cached series
            series.extend_items(new_items)

            # put into delta series
            delta_series = Series.empty(series.kind, series.buckets)
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
