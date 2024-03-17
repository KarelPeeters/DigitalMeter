import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Optional

PATTERN_ITEM = re.compile(r"^(?P<key>\d+-\d+:\d+\.\d+\.\d+)(?P<full_value>.*)$")

PATTERN_VALUE_TST = re.compile(r"^\((?P<timestamp>\d{12}[SW])\)\((?P<value>[^()]*)\)$")
PATTERN_VALUE_SINGLE = re.compile(r"^\((?P<value>[^()]*)\)$")

PATTERN_KWH = re.compile(r"^(?P<number>\d+\.\d+)\*kW$")
PATTERN_V = re.compile(r"^(?P<number>\d+\.\d+)\*V$")
PATTERN_M3 = re.compile(r"^(?P<number>\d+\.\d+)\*m3$")


@dataclass
class MessageValue:
    value: str
    timestamp: int
    timestamp_str: str

    @staticmethod
    def parse(full_value: str):
        m = PATTERN_VALUE_TST.match(full_value)
        if m:
            timestamp_str = m.groupdict()["timestamp"]
            timestamp = parse_timestamp(timestamp_str)
            value = m.groupdict()["value"]

            return MessageValue(value, timestamp, timestamp_str)

        m = PATTERN_VALUE_SINGLE.match(full_value)
        if m:
            value = m.groupdict()["value"]
            return MessageValue(value, 0, "unknown")

        return MessageValue(full_value, 0, "unknown")


class RawMessage:
    def __init__(self, lines: List[str]):
        values = {}
        is_clean = True

        for line in lines:
            if line.startswith("!"):
                break

            m = PATTERN_ITEM.match(line)
            if not m:
                print(f"WARNING: failed to match '{line}'")
                is_clean = False
                continue

            key = m.groupdict()["key"]
            full_value = m.groupdict()["full_value"]

            value = MessageValue.parse(full_value)

            if key in values:
                print(f"WARNING: overriding key '{key}'")
                is_clean = False
            values[key] = value

        self.values = values
        self.is_clean = is_clean


def parse_power(s: Optional[str]) -> float:
    if s is None:
        return math.nan

    m = PATTERN_KWH.match(s)
    if not m:
        return math.nan

    return float(m.group(1)) * 1000


def parse_voltage(s: Optional[str]) -> float:
    if s is None:
        return math.nan

    m = PATTERN_V.match(s)
    if not m:
        return math.nan

    return float(m.group(1)) * 1000


def parse_volume(s: Optional[str]) -> float:
    if s is None:
        return math.nan

    m = PATTERN_M3.match(s)
    if not m:
        return math.nan

    return float(m.group(1))


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
    voltage_1: float
    voltage_2: float
    voltage_3: float
    peak_power: float
    peak_power_timestamp: int
    peak_power_timestamp_str: str
    gas_volume: float
    gas_timestamp: int
    gas_timestamp_str: str

    @staticmethod
    def from_raw(msg: RawMessage):
        def map_value(x, f, d):
            return f(x.value) if x is not None else d

        timestamp_str = msg.values.get("0-0:1.0.0")
        timestamp = map_value(timestamp_str, parse_timestamp, None)

        instant_power_1 = map_value(msg.values.get("1-0:21.7.0"), parse_power, math.nan)
        instant_power_2 = map_value(msg.values.get("1-0:41.7.0"), parse_power, math.nan)
        instant_power_3 = map_value(msg.values.get("1-0:61.7.0"), parse_power, math.nan)

        voltage_1 = map_value(msg.values.get("1-0:32.7.0"), parse_power, math.nan)
        voltage_2 = map_value(msg.values.get("1-0:52.7.0"), parse_power, math.nan)
        voltage_3 = map_value(msg.values.get("1-0:72.7.0"), parse_power, math.nan)

        peak_power_value = msg.values.get("1-0:1.6.0")
        peak_power = map_value(peak_power_value, parse_power, math.nan)
        peak_power_timestamp = peak_power_value.timestamp if peak_power_value is not None else None

        gas_value = msg.values.get("0-1:24.2.3")

        return Message(
            timestamp=timestamp,
            timestamp_str=timestamp_str.value if timestamp_str is not None else "unknown",
            instant_power_1=instant_power_1,
            instant_power_2=instant_power_2,
            instant_power_3=instant_power_3,
            voltage_1=voltage_1,
            voltage_2=voltage_2,
            voltage_3=voltage_3,
            peak_power=peak_power,
            peak_power_timestamp=peak_power_timestamp,
            peak_power_timestamp_str=peak_power_value.timestamp_str if peak_power_value is not None else "unknown",
            gas_volume=map_value(gas_value, parse_volume, math.nan),
            gas_timestamp=gas_value.timestamp if gas_value is not None else None,
            gas_timestamp_str=gas_value.timestamp_str if gas_value is not None else "unknown"
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
