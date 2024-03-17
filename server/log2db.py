import argparse
import itertools
import os
import sqlite3
import time

from inputs.parse import Parser, MeterMessage


def iter_messages(path: str):
    parser = Parser()

    with open(path, "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            raw_msg = parser.push_line(line)
            if raw_msg is not None and raw_msg.is_clean:
                offset = f.tell()
                msg = MeterMessage.from_raw(raw_msg)
                yield offset, msg


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
        "    instant_power_3 REAL,"
        "    voltage_1 REAL,"
        "    voltage_2 REAL,"
        "    voltage_3 REAL"
        ")"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS meter_peaks("
        "    timestamp INTEGER PRIMARY KEY, "
        "    timestamp_str TEXT,"
        "    instant_power_total REAL"
        ")"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS gas_samples("
        "    timestamp INTEGER PRIMARY KEY,"
        "    timestamp_str TEXT,"
        "    volume REAL"
        ")"
    )
    connection.commit()

    print("Inserting items")
    chunk_size = 1024
    file_size = os.path.getsize(path_log)
    messages = iter_messages(path_log)

    count = 0
    start = time.perf_counter()
    prev = start

    while True:
        chunk_info = list(itertools.islice(messages, chunk_size))
        if len(chunk_info) == 0:
            break

        chunk = [msg for _, msg in chunk_info]
        last_offset = chunk_info[-1][0]

        connection.executemany(
            "INSERT OR REPLACE INTO meter_samples  VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (msg.timestamp, msg.timestamp_str, msg.instant_power_1, msg.instant_power_2, msg.instant_power_3,
                 msg.voltage_1, msg.voltage_2, msg.voltage_3)
                for msg in chunk if msg.timestamp is not None
            ]
        )
        connection.executemany(
            "INSERT OR REPLACE INTO meter_peaks VALUES(?, ?, ?)",
            [
                (msg.peak_power_timestamp, msg.peak_power_timestamp_str, msg.peak_power)
                for msg in chunk if
                msg.peak_power_timestamp is not None
            ]
        )
        connection.executemany(
            "INSERT OR REPLACE INTO gas_samples VALUES(?, ?, ?)",
            [
                (msg.gas_timestamp, msg.gas_timestamp_str, msg.gas_volume)
                for msg in chunk if
                msg.gas_timestamp is not None
            ]
        )

        # commit frequently in loop to give potential other processes occasional access
        connection.commit()

        count += len(chunk)
        now = time.perf_counter()

        throughput = len(chunk) / (now - prev)
        time_left = (file_size - last_offset) / last_offset * (now - start)
        progress = last_offset / file_size

        prev = now

        print(f"Inserted {count} values, {throughput} values/s, progress {progress :.2}, left {time_left:.2f}s")


if __name__ == "__main__":
    main()
