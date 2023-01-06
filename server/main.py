from queue import Queue as QQueue
from threading import Thread
from typing import Callable

from server.data import DataStore, Database
from server.flask_server import flask_main
from server.socket_server import socket_server_main


def run_message_processor(store: DataStore, message_queue: QQueue):
    while True:
        q_size = message_queue.qsize()
        if q_size > 10:
            print(f"WARNING: backlog of {q_size} messages")

        message = message_queue.get()
        store.process_message(message)


def run_flask(database_path: str):
    database = Database(database_path)
    flask_main(database)


def server_main(database_path: str, generator: Callable[[QQueue], None]):
    message_queue = QQueue()

    Thread(target=generator, args=(message_queue,)).start()

    store = DataStore(Database(database_path))
    Thread(target=socket_server_main, args=(store,)).start()

    Thread(target=run_flask, args=(database_path,)).start()

    run_message_processor(store, message_queue)
