import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

PATTERN_ITEM = re.compile(r"^(?P<key>\d+-\d+:\d+\.\d+\.\d+)(?P<full_value>.*)$")

PATTERN_VALUE_TST = re.compile(r"^\((?P<timestamp>\d{12}[SW])\)\((?P<value>[^()]*)\)$")
PATTERN_VALUE_SINGLE = re.compile(r"^\((?P<value>[^()]*)\)$")

PATTERN_KWH = re.compile(r"(?P<number>\d+\.\d+)\*kW")


@dataclass
class Timestamp:
    date_time: datetime
    dst: bool
    full_str: str
    short_str: str


@dataclass
class MaybeTimeStamp:
    timestamp: Optional[Timestamp]

    def full_str(self):
        if self.timestamp is not None:
            return self.timestamp.full_str
        else:
            return "unknown"


@dataclass
class MessageValue:
    value: str
    timestamp: MaybeTimeStamp

    @staticmethod
    def parse(full_value: str):
        m = re.match(PATTERN_VALUE_TST, full_value)
        if m:
            timestamp = parse_timestamp(m.groupdict()["timestamp"])
            value = m.groupdict()["value"]

            return MessageValue(value, timestamp)

        m = re.match(PATTERN_VALUE_SINGLE, full_value)
        if m:
            value = m.groupdict()["value"]
            return MessageValue(value, MaybeTimeStamp(None))

        return MessageValue(full_value, MaybeTimeStamp(None))


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


def parse_timestamp(short_str: Optional[str]) -> MaybeTimeStamp:
    if short_str is None:
        return MaybeTimeStamp(None)

    try:
        dst = short_str.endswith("S")
        not_dst = short_str.endswith("W")
        if not (dst or not_dst):
            raise ValueError()

        date_time = datetime.strptime(short_str[:-1], "%y%m%d%H%M%S")

        full_str = date_time.strftime("%Y-%m-%d %H:%M:%S")
        if dst:
            full_str += " DST"

        return MaybeTimeStamp(Timestamp(date_time, dst, full_str, short_str))
    except ValueError:
        print(f"WARNING: failed to parse timestamp '{short_str}'")
        return MaybeTimeStamp(None)


@dataclass
class Message:
    timestamp: MaybeTimeStamp
    instant_power_1: float
    instant_power_2: float
    instant_power_3: float
    peak_power: float
    peak_power_timestamp: MaybeTimeStamp

    def __init__(self, msg: RawMessage):
        def map_value(x, f, d):
            return f(x.value) if x is not None else d

        self.timestamp = map_value(msg.values.get("0-0:1.0.0"), parse_timestamp, MaybeTimeStamp(None))

        self.instant_power_1 = map_value(msg.values.get("1-0:21.7.0"), parse_power, math.nan)
        self.instant_power_2 = map_value(msg.values.get("1-0:41.7.0"), parse_power, math.nan)
        self.instant_power_3 = map_value(msg.values.get("1-0:61.7.0"), parse_power, math.nan)

        peak_power = msg.values.get("1-0:1.6.0")
        self.peak_power = map_value(peak_power, parse_power, math.nan)
        self.peak_power_timestamp = peak_power.timestamp if peak_power is not None else MaybeTimeStamp(None)


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
