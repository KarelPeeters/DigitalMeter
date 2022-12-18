from queue import Queue as QQueue
from threading import Thread

import serial

from parse import Parser, Message
from server.http_server import run_http_server
from server.socket_server import run_socket_server


def run_serial_parser(message_queue: QQueue, log):
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

        if log is not None:
            log.write(line_str)

        raw_msg = parser.push_line(line_str)

        if raw_msg is not None:
            msg = Message.from_raw(raw_msg)
            message_queue.put(msg)


if __name__ == '__main__':
    def generator(queue):
        with open("log.txt", "a") as log:
            run_serial_parser(queue, log)

    Thread(target=run_http_server, args=("resources",)).start()

    run_socket_server(generator, "data.db")
