import math
import re
import time
from dataclasses import dataclass
from typing import List, Optional

PATTERN_ITEM = re.compile(r"^(?P<key>\d+-\d+:\d+\.\d+\.\d+)(?P<full_value>.*)$")

PATTERN_VALUE_TST = re.compile(r"^\((?P<timestamp>\d{12})(?P<dst>[SW])\)\((?P<value>[^()]*)\)$")
PATTERN_VALUE_SINGLE = re.compile(r"^\((?P<value>[^()]*)\)$")

PATTERN_KWH = re.compile(r"(?P<number>\d+\.\d+)\*kW")


@dataclass
class MessageValue:
    value: str
    timestamp: Optional[str]

    @staticmethod
    def parse(full_value: str):
        m = re.match(PATTERN_VALUE_TST, full_value)
        if m:
            timestamp = m.groupdict()["timestamp"]
            dst = m.groupdict()["dst"] == "S"
            value = m.groupdict()["value"]

            try:
                timestamp = time.strptime(timestamp, "%y%m%d%H%M%S")
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", timestamp)
                if dst:
                    timestamp += " DST"
            except ValueError:
                print(f"WARNING: failed to parse timestamp '{timestamp}'")
                timestamp = None

            return MessageValue(value, timestamp)

        m = re.match(PATTERN_VALUE_SINGLE, full_value)
        if m:
            value = m.groupdict()["value"]
            return MessageValue(value, None)

        return MessageValue(full_value, None)


class RawMessage:
    def __init__(self, lines: List[str]):
        values = {}

        for line in lines:
            if line.startswith("!"):
                break

            m = PATTERN_ITEM.match(line)
            if not m:
                print(f"WARNING: failed to match '{line}'")
                continue

            key = m.groupdict()["key"]
            full_value = m.groupdict()["full_value"]

            value = MessageValue.parse(full_value)

            if key in values:
                print(f"WARNING: overriding key '{key}'")
            values[key] = value

        self.values = values


def parse_power(s: Optional[str]) -> float:
    if s is None:
        return math.nan

    m = re.match(PATTERN_KWH, s)
    if not m:
        return math.nan

    return float(m.group(1)) * 1000


@dataclass
class Message:
    instant_power_1: float
    instant_power_2: float
    instant_power_3: float
    peak_power: float
    peak_power_timestamp: str

    def __init__(self, msg: RawMessage):
        def map_value(x, f, d):
            return f(x.value) if x is not None else d

        self.instant_power_1 = map_value(msg.values.get("1-0:21.7.0"), parse_power, math.nan)
        self.instant_power_2 = map_value(msg.values.get("1-0:41.7.0"), parse_power, math.nan)
        self.instant_power_3 = map_value(msg.values.get("1-0:61.7.0"), parse_power, math.nan)

        peak_power = msg.values.get("1-0:1.6.0")
        self.peak_power = math.nan
        self.peak_power_timestamp = "unknown"
        if peak_power is not None:
            self.peak_power = parse_power(peak_power.value)
            if peak_power.timestamp is not None:
                self.peak_power_timestamp = peak_power.timestamp


class Parser:
    def __init__(self):
        self.wait_for_sync = True
        self.lines = []

    def reset(self):
        self.wait_for_sync = True
        self.lines = []

    def push_line(self, line: str) -> Optional[RawMessage]:
        line = line.strip()

        if len(line) == 0:
            self.wait_for_sync = False
            if len(self.lines) > 0:
                msg = RawMessage(self.lines)
                self.lines = []
                return msg
        else:
            if not self.wait_for_sync:
                self.lines.append(line)

        return None
