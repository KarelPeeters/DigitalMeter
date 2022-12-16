from parse import Parser, Message


def main():
    path = "../output.txt"
    parser = Parser()

    with open(path, "r") as f:
        while True:
            line = f.readline()

            if len(line) == 0:
                break

            raw_msg = parser.push_line(line)

            if raw_msg is not None:
                msg = Message(raw_msg)
                print(msg)


if __name__ == '__main__':
    main()
