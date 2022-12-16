import os.path
import time
from threading import Thread

import serial

from grapher import grapher
from grapher.grapher import Broadcast, GraphPoint
from parse import Parser, Message
from run_server import run_server


def serial_generator(broadcast: Broadcast):
    port = serial.Serial(
        port='/dev/ttyS0',
        baudrate=115200,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.EIGHTBITS,
        timeout=10,
    )
    parser = Parser()

    while True:
        line = port.readline()
        if len(line) == 0:
            print("Timeout")
            parser.reset()
            continue

        try:
            line_str = line.decode()
        except UnicodeDecodeError:
            print("Unicode decode error")
            parser.reset()
            continue

        raw_msg = parser.push_line(line_str)

        if raw_msg is not None:
            msg = Message(raw_msg)
            values = {
                "P1 [W]": msg.instant_power_1,
                "P2 [W]": msg.instant_power_2,
                "P3 [W]": msg.instant_power_3,
            }
            info = f"Peak power: {msg.peak_power} W at {msg.peak_power_timestamp}"
            point = GraphPoint(time.time(), values, info)
            broadcast.send(point)


def main():
    def thread_main():
        assert os.path.exists("grapher/")
        run_server("grapher/")

    thread = Thread(target=thread_main)
    thread.start()

    grapher.run_grapher(serial_generator)


if __name__ == '__main__':
    main()
