import time
from queue import Queue as QQueue
from threading import Thread

import serial

from inputs.adc import ArduinoADC, ADCMessage
from inputs.parse import Parser, MeterMessage
from server.main import server_main


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

        if raw_msg is not None and raw_msg.is_clean:
            msg = MeterMessage.from_raw(raw_msg)
            message_queue.put(msg)


def main():
    def main_serial(queue):
        with open("log.txt", "a") as log:
            run_serial_parser(queue, log)

    def main_adc(queue):
        adc = ArduinoADC()
        adc_period = 1
        while True:
            time_start = time.perf_counter()
            msg = adc.readout_message()
            queue.put(msg)

            delta = adc_period - (time.perf_counter() - time_start)
            if delta > 0:
                time.sleep(delta)

    message_queue = QQueue()
    Thread(target=main_serial, args=(message_queue,)).start()
    Thread(target=main_adc, args=(message_queue,)).start()
    server_main("data.db", message_queue)


if __name__ == '__main__':
    main()
