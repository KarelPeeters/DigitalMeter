import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional

PATTERN_ITEM = re.compile(r"^(?P<key>\d+-\d+:\d+\.\d+\.\d+)(?P<full_value>.*)$")

PATTERN_VALUE_TST = re.compile(r"^\((?P<timestamp>\d{12}[SW])\)\((?P<value>[^()]*)\)$")
PATTERN_VALUE_SINGLE = re.compile(r"^\((?P<value>[^()]*)\)$")

PATTERN_KWH = re.compile(r"^(?P<number>\d+\.\d+)\*kW$")


@dataclass
class MessageValue:
    value: str
    timestamp: int
    timestamp_str: str

    @staticmethod
    def parse(full_value: str):
        m = re.match(PATTERN_VALUE_TST, full_value)
        if m:
            timestamp_str = m.groupdict()["timestamp"]
            timestamp = parse_timestamp(timestamp_str)
            value = m.groupdict()["value"]

            return MessageValue(value, timestamp, timestamp_str)

        m = re.match(PATTERN_VALUE_SINGLE, full_value)
        if m:
            value = m.groupdict()["value"]
            return MessageValue(value, 0, "unknown")

        return MessageValue(full_value, 0, "unknown")


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
                # TODO just fail instead?
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


def parse_timestamp(short_str: Optional[str]) -> int:
    if short_str is None:
        return 0

    try:
        dst = short_str.endswith("S")
        not_dst = short_str.endswith("W")
        if not (dst or not_dst):
            raise ValueError()
        tz = timezone(timedelta(hours=+2 if dst else +1))

        date_time = datetime.strptime(short_str[:-1], "%y%m%d%H%M%S").replace(tzinfo=tz)
        return int(date_time.timestamp())
    except ValueError:
        print(f"WARNING: failed to parse timestamp '{short_str}'")
        return 0


@dataclass
class Message:
    timestamp: int
    timestamp_str: str
    instant_power_1: float
    instant_power_2: float
    instant_power_3: float
    peak_power: float
    peak_power_timestamp: int
    peak_power_timestamp_str: str

    @staticmethod
    def from_raw(msg: RawMessage):
        def map_value(x, f, d):
            return f(x.value) if x is not None else d

        timestamp_str = msg.values.get("0-0:1.0.0")
        timestamp = map_value(timestamp_str, parse_timestamp, None)

        instant_power_1 = map_value(msg.values.get("1-0:21.7.0"), parse_power, math.nan)
        instant_power_2 = map_value(msg.values.get("1-0:41.7.0"), parse_power, math.nan)
        instant_power_3 = map_value(msg.values.get("1-0:61.7.0"), parse_power, math.nan)

        peak_power_value = msg.values.get("1-0:1.6.0")
        peak_power = map_value(peak_power_value, parse_power, math.nan)
        peak_power_timestamp = peak_power_value.timestamp if peak_power_value is not None else None

        return Message(
            timestamp=timestamp,
            timestamp_str=timestamp_str.value if timestamp_str is not None else "unknown",
            instant_power_1=instant_power_1,
            instant_power_2=instant_power_2,
            instant_power_3=instant_power_3,
            peak_power=peak_power,
            peak_power_timestamp=peak_power_timestamp,
            peak_power_timestamp_str=peak_power_value.timestamp_str
        )


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
