import numpy as np
from matplotlib import pyplot as plt
from matplotlib.dates import DateFormatter

from parse import Parser, Message


def main():
    path = "../log.txt"
    parser = Parser()

    timestamp = []
    instant_power_1 = []
    instant_power_2 = []
    instant_power_3 = []

    with open(path, "r") as f:
        while True:
            line = f.readline()

            if len(line) == 0:
                break

            raw_msg = parser.push_line(line)

            if raw_msg is not None:
                msg = Message.from_raw(raw_msg)

                timestamp.append(msg.timestamp.timestamp.date_time)
                instant_power_1.append(msg.instant_power_1)
                instant_power_2.append(msg.instant_power_2)
                instant_power_3.append(msg.instant_power_3)

    instant_power = np.array([instant_power_1, instant_power_2, instant_power_3])

    plt.plot(timestamp, instant_power.T, label=["P1 [W]", "P2 [W]", "P3 [W]"])
    plt.gca().xaxis.set_major_formatter(DateFormatter("%H:%M"))
    plt.show()


if __name__ == '__main__':
    main()
