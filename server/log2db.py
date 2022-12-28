import itertools
import os
import sqlite3
import sys
import time

from parse import Parser, Message


def iter_messages(path: str):
    parser = Parser()

    with open(path, "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            raw_msg = parser.push_line(line)
            if raw_msg is not None and raw_msg.is_clean:
                msg = Message.from_raw(raw_msg)
                yield msg


def message_to_tuple(msg: Message):
    return msg.timestamp, msg.timestamp_str, msg.instant_power_1, msg.instant_power_2, msg.instant_power_3


def main():
    assert len(sys.argv) == 3, "Usage: 'log2db log_path db_path'"

    path_log = sys.argv[1]
    path_db = sys.argv[2]

    assert not os.path.exists(path_db), f"Database path '{path_db}' already exists"

    connection = sqlite3.connect(path_db)

    print("Creating tables")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS meter_samples("
        "    timestamp INTEGER PRIMARY KEY,"
        "    timestamp_str TEXT,"
        "    instant_power_1 REAL,"
        "    instant_power_2 REAL,"
        "    instant_power_3 REAL"
        ")"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS meter_peaks("
        "    timestamp INTEGER PRIMARY KEY, "
        "    timestamp_str TEXT,"
        "    instant_power_total REAL"
        ")"
    )
    connection.commit()

    print("Inserting items")
    chunk_size = 1024
    messages = iter_messages(path_log)

    count = 0
    start = time.perf_counter()

    while True:
        chunk = list(itertools.islice(messages, chunk_size))
        if len(chunk) == 0:
            break

        connection.executemany(
            "INSERT OR REPLACE INTO meter_samples  VALUES(?, ?, ?, ?, ?)",
            [message_to_tuple(msg) for msg in chunk if msg.timestamp is not None]
        )
        connection.executemany(
            "INSERT OR REPLACE INTO meter_peaks VALUES(?, ?, ?)",
            [(msg.peak_power_timestamp, msg.peak_power_timestamp_str, msg.peak_power) for msg in chunk if
             msg.peak_power_timestamp is not None]
        )

        count += len(chunk)
        print(f"Inserted {count} values, {count / (time.perf_counter() - start)} values/s")

    connection.commit()


if __name__ == "__main__":
    main()
