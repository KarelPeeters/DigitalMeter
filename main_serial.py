import serial

from inputs.parse import Parser, MeterMessage


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

        if raw_msg is not None and raw_msg.is_clean:
            msg = MeterMessage.from_raw(raw_msg)
            print(msg)


if __name__ == '__main__':
    main()
