import contextlib
import http.server
import mimetypes
import os
import socket
import sys
from functools import partial
from http.server import ThreadingHTTPServer
from http.server import test as http_server_test


def run_http_server(directory):
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
    assert len(sys.argv) == 2, "Expected single argument for the path of the directory to server"
    print("Found files", os.listdir(sys.argv[1]))
    run_http_server(sys.argv[1])


if __name__ == '__main__':
    main()
