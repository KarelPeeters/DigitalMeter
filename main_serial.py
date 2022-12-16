import serial

from parse import Parser, Message


def main():
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
            print("Timeout...")

        raw_msg = parser.push_line(line.decode())

        if raw_msg is not None:
            msg = Message(raw_msg)
            print(msg)


if __name__ == '__main__':
    main()
