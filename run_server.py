import contextlib
import http.server
import mimetypes
import os
import socket
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from http.server import test as http_server_test


def run_server(directory):
    mimetypes.add_type("application/javascript", ".js")
    mimetypes.add_type("text/html", ".html")

    class DualStackServer(ThreadingHTTPServer):
        def server_bind(self):
            # suppress exception when protocol is IPv4
            with contextlib.suppress(Exception):
                self.socket.setsockopt(
                    socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            return super().server_bind()

    class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
        def end_headers(self):
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            super().end_headers()

    http_server_test(
        HandlerClass=partial(NoCacheHandler, directory=directory),
        ServerClass=DualStackServer,
        port=8000,
        bind=None,
    )


def main():
    run_server(os.getcwd())


if __name__ == '__main__':
    main()
