from datetime import datetime

import numpy as np
from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter, AutoDateFormatter, AutoDateLocator, ConciseDateFormatter

from parse import Parser, Message


def main():
    path = "log.txt"
    parser = Parser()

    timestamp = []
    instant_power_1 = []
    instant_power_2 = []
    instant_power_3 = []

    gas_timestamps = []
    gas_volumes = []

    with (open(path, "r") as f):
        while len(timestamp) < 5e3*100:
            line = f.readline()

            if len(line) == 0:
                break

            raw_msg = parser.push_line(line)

            if raw_msg is not None and raw_msg.is_clean:
                msg = Message.from_raw(raw_msg)

                timestamp.append(datetime.fromtimestamp(msg.timestamp))
                instant_power_1.append(msg.instant_power_1)
                instant_power_2.append(msg.instant_power_2)
                instant_power_3.append(msg.instant_power_3)

                if not gas_timestamps or gas_timestamps[-1] != msg.gas_timestamp:
                    gas_timestamps.append(datetime.fromtimestamp(msg.gas_timestamp))
                    gas_volumes.append(msg.gas_volume)

    instant_power = np.array([instant_power_1, instant_power_2, instant_power_3])

    plt.figure()
    plt.plot(timestamp, instant_power.T, label=["P1 [W]", "P2 [W]", "P3 [W]"])
    plt.gca().xaxis.set_major_formatter(DateFormatter("%H:%M"))
    plt.show(block=False)

    plt.figure()
    plt.plot(gas_timestamps, gas_volumes, label="Gas [m3]", marker=".")
    plt.gca().xaxis.set_major_formatter(ConciseDateFormatter(AutoDateLocator()))
    plt.show(block=False)

    plt.figure()
    plt.plot(gas_timestamps[:-1], np.diff(gas_volumes), label="Gas [m3]", marker=".")
    plt.gca().xaxis.set_major_formatter(ConciseDateFormatter(AutoDateLocator()))
    plt.show()



if __name__ == '__main__':
    main()
    # print()
    # print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(parse_timestamp("221216232120W"))))
    # print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(parse_timestamp("221216232006W"))))
