"""Tiny HTTP server for the docker-runtime integration test.

Answers every request with 200 + "hello from docker" on 0.0.0.0:8080. No
third-party deps (stdlib only) so the image builds fast from python:3-slim.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def _respond(self):
        body = b"hello from docker"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 (stdlib casing)
        self._respond()

    def do_POST(self):  # noqa: N802
        self._respond()

    def log_message(self, *_args):  # silence request logging
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
