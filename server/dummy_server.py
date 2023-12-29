import itertools
import math
import random
import time
from queue import Queue as QQueue

from parse import Message
from server.main import server_main


def run_dummy_parser(message_queue: QQueue):
    t = time.time()
    start = t

    for _ in itertools.count():
        t = t + 1
        time.sleep(max(0.0, t - time.time()))

        # if drop_count > 0:
        #     print(f"Dropping message {drop_count}")
        #     drop_count -= 1
        #     continue
        # if random.random() < 0.05:
        #     drop_count = 5

        ya = math.sin(t * 0.1) + random.random() * 0.1 + 4
        yb = math.sin(t * 0.2) + random.random() * 0.2 + 4
        yc = math.sin(t * 0.5) + random.random() * 0.05 + 4
        g = 100 + 0.1 * (t - start) + random.random() * 0.1

        msg = Message(int(t), "dummy", ya, yb, yc, ya/10, yb/10, yc/10, math.nan, 0, "dummy", g, int(t)//10*10, "dummy")
        message_queue.put(msg)


def main():
    server_main("dummy.db", run_dummy_parser)


if __name__ == '__main__':
    main()
