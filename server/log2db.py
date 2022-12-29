import argparse
import itertools
import os
import sqlite3
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
    parser = argparse.ArgumentParser(prog="log2db")
    parser.add_argument("path_log")
    parser.add_argument("path_db")
    parser.add_argument("--update", action="store_true")
    args = parser.parse_args()

    path_log: str = args.path_log
    path_db: str = args.path_db
    update: bool = args.update

    db_exists = os.path.exists(path_db)

    if db_exists:
        if update:
            print("Updating existing DB")
        else:
            assert False, f"Database path '{path_db}' already exists and --update was not passed"

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

        # commit in loop to give potential other processes occasional access
        connection.commit()


if __name__ == "__main__":
    main()
