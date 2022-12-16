import math
import re
from dataclasses import dataclass
from typing import List, Optional

PATTERN_ITEM = re.compile(r"^(\d+-\d+:\d+.\d+.\d+)(?:\(([^()]*)\)|(.*))$")
PATTERN_KWH = re.compile(r"(\d+\.\d+)\*kW")


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

            key = m.group(1)
            value = m.group(2) if m.group(2) is not None else m.group(3)

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

    return float(m.group(1))


@dataclass
class Message:
    instant_power_1: float
    instant_power_2: float
    instant_power_3: float

    def __init__(self, msg: RawMessage):
        self.instant_power_1 = parse_power(msg.values.get("1-0:21.7.0"))
        self.instant_power_2 = parse_power(msg.values.get("1-0:41.7.0"))
        self.instant_power_3 = parse_power(msg.values.get("1-0:61.7.0"))


class Parser:
    def __init__(self):
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

