"""HTTP front-end for the duty service (stdlib only).

Endpoints
---------
POST /v1/choices        create / retransmit a choice
POST /v1/executions     consume a choice and execute
GET  /v1/devices/<id>   inspect recovered device state
GET  /healthz           liveness + durable-record integrity

The two execution endpoints accept an optional ``crash`` field
(``before_result`` / ``after_result``) used by the acceptance suite to
inject power cuts; it hard-kills the process with exit code 99.
"""

from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .gateway import DeviceGateway
from .service import DutyService
from .store import RecordStore

DATA_DIR = os.environ.get("DATA_DIR", "/data")
GATEWAY_LOG = os.environ.get("GATEWAY_LOG", os.path.join(DATA_DIR, "gateway.log"))
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8080"))


def build_service() -> DutyService:
    store = RecordStore(DATA_DIR)
    gateway = DeviceGateway(GATEWAY_LOG)
    return DutyService(store, gateway)


class Handler(BaseHTTPRequestHandler):
    @property
    def service(self) -> "DutyService":
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # quiet, structured stderr
        import sys
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def _send(self, resp) -> None:
        body = json.dumps(resp.to_dict(), sort_keys=True).encode("utf-8")
        self.send_response(resp.http_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return {"__invalid_json__": True}
        return data if isinstance(data, dict) else {"__invalid_json__": True}

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/healthz":
            self._send(self.service.health())
            return
        if path.startswith("/v1/devices/"):
            device_id = path[len("/v1/devices/"):]
            self._send(self.service.device_state(device_id))
            return
        self._send_not_found()

    def do_POST(self):
        path = urlparse(self.path).path
        data = self._read_json()
        if data.get("__invalid_json__"):
            self._send_json(400, {"verdict": "BAD_REQUEST",
                                  "detail": "invalid JSON body"})
            return
        if path == "/v1/choices":
            resp = self.service.select(
                device_id=data.get("device_id", ""),
                op_id=data.get("op_id", ""),
                summary=data.get("summary", ""),
                expires_at=data.get("expires_at", 0),
            )
            self._send(resp)
            return
        if path == "/v1/executions":
            resp = self.service.execute(
                device_id=data.get("device_id", ""),
                op_id=data.get("op_id", ""),
                summary=data.get("summary", ""),
                _crash=data.get("crash"),
            )
            self._send(resp)
            return
        self._send_not_found()

    def _send_not_found(self):
        self._send_json(404, {"verdict": "NOT_FOUND", "detail": "unknown path"})

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    service = build_service()
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    httpd.service = service  # type: ignore[attr-defined]
    import sys
    sys.stderr.write(f"duty-service listening on {HOST}:{PORT} data={DATA_DIR}\n")
    sys.stderr.flush()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
