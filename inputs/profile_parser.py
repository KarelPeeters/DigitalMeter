import time

from parse import Parser


def main():
    path = "log.txt"
    parser = Parser()

    count = 0
    start = time.perf_counter()
    prev = start

    with (open(path, "r") as f):
        while True:
            line = f.readline()
            if len(line) == 0:
                break

            raw_msg = parser.push_line(line)
            if raw_msg is not None and raw_msg.is_clean:
                count += 1

                if count % 1000 == 0:
                    now = time.perf_counter()
                    print(f"Messages/s: {1000/(now-prev):.2f}")
                    prev = now

                # if count > 20e3:
                #     break

        byte_count = f.tell()
    delta = time.perf_counter() - start

    print(f"Total messages: {count}")
    print(f"Total bytes: {byte_count}")
    print(f"Bytes/message: {byte_count/count:.2f}")
    print(f"Time: {delta:.2f}")
    print(f"Messages/s: {count/delta:.2f}")
    print(f"Bytes/s: {byte_count/delta:.2f}")


if __name__ == '__main__':
    main()
