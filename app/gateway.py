"""Simulated on-board device gateway.

The gateway is the (simulated) downstream that actually sends the
high-risk command.  It is idempotent on the stable operation id: a retry
after a crash that occurred after dispatch but before the result was
persisted never causes a second physical command.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time


class DeviceGateway:
    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()
        # (device_id, op_id) -> recorded outcome
        self._delivered: dict[tuple[str, str], dict] = {}
        # physical dispatches performed in *this* process boot
        self.dispatch_count = 0
        self._reload()

    def _reload(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, "rb") as fh:
            for line in fh.read().splitlines():
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = (rec["device_id"], rec["op_id"])
                self._delivered[key] = rec["outcome"]

    def _append(self, rec: dict) -> None:
        with open(self._path, "ab") as fh:
            fh.write((json.dumps(rec, sort_keys=True) + "\n").encode())
            fh.flush()
            os.fsync(fh.fileno())

    def send(self, device_id: str, op_id: str, summary: str,
             delay: float = 0.05) -> dict:
        """Deliver the command; exactly one physical dispatch per op id."""
        key = (device_id, op_id)
        with self._lock:
            prior = self._delivered.get(key)
            if prior is not None:
                # Idempotent replay at the downstream level.
                return dict(prior, replayed_downstream=True)
            outcome = {
                "status": "ok",
                "device_id": device_id,
                "op_id": op_id,
                "summary": summary,
                "executed_at": time.time(),
                "receipt": hashlib.sha256(
                    f"{device_id}|{op_id}|{summary}".encode()
                ).hexdigest()[:16],
            }
            self.dispatch_count += 1
            self._append({"device_id": device_id, "op_id": op_id,
                          "outcome": outcome})
        if delay:
            time.sleep(delay)
        return outcome
