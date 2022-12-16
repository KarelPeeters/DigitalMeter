import contextlib
import mimetypes
import os
import socket
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from http.server import test as http_server_test

mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/html", ".html")


class DualStackServer(ThreadingHTTPServer):
    def server_bind(self):
        # suppress exception when protocol is IPv4
        with contextlib.suppress(Exception):
            self.socket.setsockopt(
                socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        return super().server_bind()


http_server_test(
    HandlerClass=partial(SimpleHTTPRequestHandler, directory=os.getcwd()),
    ServerClass=DualStackServer,
    port=8000,
    bind=None,
)
