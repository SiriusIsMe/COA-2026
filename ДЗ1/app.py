"""Notification service: минимальный сервис уведомлений маркетплейса.

GET  /health  -> 200 {"status": "ok"}
POST /notify  -> 202 {"status": "queued"}  (заглушка отправки через Mindbox)
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/notify":
            self._send(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            payload = json.loads(self.rfile.read(length) or "{}")
        except json.JSONDecodeError:
            self._send(400, {"error": "invalid json"})
            return
        print("NOTIFY:", payload, flush=True)
        self._send(202, {"status": "queued"})


if __name__ == "__main__":
    print("notification-service started on :8000", flush=True)
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
