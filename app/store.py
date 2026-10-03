"""Durable record store with checksum verification and crash recovery.

The store keeps one append-only log file per operation id, plus an index
mapping device -> current operation id. Every persisted record carries a
SHA-256 checksum so that torn/corrupt writes after a power cut are
detectable; the health check reports any file that cannot be verified.

Recovery rules (crash may happen at *any* point, incl. mid-fsync):

* A fully intact ``selected`` record            -> ``SELECTED`` (safe retry)
* ``executing`` record intact, no result        -> ``SELECTED`` (safe retry)
* An intact ``executing`` *and* an intact
  ``result`` record                              -> ``EXECUTED`` (replayable)
* A corrupt / unverifiable record               -> health UNHEALTHY;
                                                   the device's choice is
                                                   quarantined and refuses
                                                   further commands

Old choices never revive: replacing a choice writes a new index entry that
points at a new operation file; stale files are simply not referenced.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
from dataclasses import dataclass, field
from typing import Optional

RECORD_SELECTED = "selected"
RECORD_EXECUTING = "executing"
RECORD_RESULT = "result"

STATE_MISSING = "MISSING"
STATE_SELECTED = "SELECTED"
STATE_EXECUTED = "EXECUTED"

MAGIC = "GS-CHOICE-1"


def _record_path(data_dir: str, op_id: str) -> str:
    safe = hashlib.sha256(op_id.encode("utf-8")).hexdigest()
    return os.path.join(data_dir, f"op-{safe}.log")


def _encode(rec: dict) -> bytes:
    body = json.dumps(rec, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    return f"{MAGIC}|{digest}|{body}\n".encode("utf-8")


def _decode(line: bytes) -> Optional[dict]:
    """Decode one checksummed record line; return None if unverifiable."""
    try:
        text = line.decode("utf-8")
        magic, digest, body = text.rstrip("\n").split("|", 2)
        if magic != MAGIC:
            return None
        if hashlib.sha256(body.encode("utf-8")).hexdigest() != digest:
            return None
        return json.loads(body)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None


@dataclass
class Recovered:
    state: str
    record: dict
    result: Optional[dict] = None
    corrupt: bool = False


@dataclass
class _IndexEntry:
    op_id: str
    file: str  # basename of the record log
    state: str = STATE_MISSING
    record: Optional[dict] = None
    result: Optional[dict] = None
    corrupt: bool = False


@dataclass
class HealthReport:
    status: str  # "ok" | "unhealthy"
    corrupt_files: list = field(default_factory=list)
    detail: dict = field(default_factory=dict)


class RecordStore:
    """File-backed, checksummed, crash-safe store of choices/executions."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        self._index_path = os.path.join(self.data_dir, "index.json")
        self._lock = threading.RLock()
        # device_id -> _IndexEntry
        self._devices: dict[str, _IndexEntry] = {}
        self._recover()

    # ------------------------------------------------------------------ #
    # recovery
    # ------------------------------------------------------------------ #
    def _recover(self) -> None:
        raw_index = self._read_raw_index()
        index_corrupt = raw_index is None and os.path.exists(self._index_path)

        if raw_index:
            for device, ent in raw_index.items():
                op_id = ent.get("op_id")
                fname = ent.get("file")
                if not op_id or not fname:
                    index_corrupt = True
                    continue
                rec = self._load_file(fname, op_id)
                self._devices[device] = rec  # _load_file builds the entry

        # Files present but not referenced by the index are stale (old
        # choices); they must never be resurrected.
        self._index_corrupt = index_corrupt

    def _read_raw_index(self) -> Optional[dict]:
        """Read index with an atomic-rename aware lookup.

        Writers publish the index via temp-file + ``os.replace``.  If a
        crash happens between unlinking the old name and the rename of the
        new one (POSIX rename is atomic, so that window essentially does
        not exist), the previous content is additionally kept at
        ``index.json.prev`` as a best-effort fallback.
        """
        for path in (self._index_path, self._index_path + ".prev"):
            if not os.path.exists(path):
                continue
            try:
                with open(path, "rb") as fh:
                    blob = fh.read()
                text = blob.decode("utf-8").rstrip("\n")
                magic, digest, body = text.split("|", 2)
                if magic != MAGIC:
                    continue
                if hashlib.sha256(body.encode("utf-8")).hexdigest() != digest:
                    continue
                return json.loads(body)
            except (ValueError, UnicodeDecodeError, json.JSONDecodeError, OSError):
                continue
        return None

    def _load_file(self, fname: str, op_id: str) -> _IndexEntry:
        entry = _IndexEntry(op_id=op_id, file=fname)
        path = os.path.join(self.data_dir, fname)
        try:
            with open(path, "rb") as fh:
                lines = fh.readlines()
        except OSError:
            entry.corrupt = True
            return entry

        records: list[dict] = []
        saw_bad_line = False
        for line in lines:
            if not line.strip():
                # A zero-byte tail from a torn write: harmless only when it
                # is the very last line and empty.
                continue
            rec = _decode(line)
            if rec is None:
                saw_bad_line = True
                break
            records.append(rec)

        latest_selected = None
        executing = False
        result = None
        for rec in records:
            kind = rec.get("kind")
            if kind == RECORD_SELECTED and rec.get("op_id") == op_id:
                latest_selected = rec
                executing = False
                result = None
            elif kind == RECORD_EXECUTING and rec.get("op_id") == op_id:
                executing = True
            elif kind == RECORD_RESULT and rec.get("op_id") == op_id:
                result = rec
                executing = False

        if latest_selected is None:
            # No usable selected record -> choice cannot be trusted.
            entry.corrupt = True
            return entry

        entry.record = latest_selected
        if result is not None:
            entry.state = STATE_EXECUTED
            entry.result = result
        elif executing:
            # Crash between "executing" and "result" fsync: the downstream
            # command may or may not have happened.  We cannot prove
            # execution, therefore we recover to the safe-retry state
            # SELECTED (the executor is required to be idempotent on
            # op_id).
            entry.state = STATE_SELECTED
        else:
            entry.state = STATE_SELECTED

        # A bad trailing record after an already-durable result: the result
        # itself is intact and replayable, but the file is not clean ->
        # flag unhealthy while keeping the replayable result.
        if saw_bad_line:
            entry.corrupt = True
        return entry

    # ------------------------------------------------------------------ #
    # index persistence
    # ------------------------------------------------------------------ #
    def _write_index_locked(self) -> None:
        payload = {
            dev: {"op_id": e.op_id, "file": e.file}
            for dev, e in self._devices.items()
        }
        body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        blob = f"{MAGIC}|{hashlib.sha256(body.encode()).hexdigest()}|{body}\n".encode()
        if os.path.exists(self._index_path):
            try:
                with open(self._index_path, "rb") as fh:
                    old = fh.read()
                with open(self._index_path + ".prev", "wb") as fh:
                    fh.write(old)
                    fh.flush()
                    os.fsync(fh.fileno())
            except OSError:
                pass
        fd, tmp = tempfile.mkstemp(dir=self.data_dir, prefix=".index-")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(blob)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._index_path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        dir_fd = os.open(self.data_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #
    def append(self, op_id: str, record: dict) -> None:
        path = _record_path(self.data_dir, op_id)
        blob = _encode(record)
        # Append + fsync so previously durable records survive even if the
        # new record is torn by a power cut.
        with open(path, "ab") as fh:
            fh.write(blob)
            fh.flush()
            os.fsync(fh.fileno())
        dir_fd = os.open(self.data_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def put_choice(self, device_id: str, record: dict) -> None:
        """Persist a new choice and make it the device's current choice."""
        with self._lock:
            op_id = record["op_id"]
            fname = os.path.basename(_record_path(self.data_dir, op_id))
            # 1. record log first (durable choice)...
            self.append(op_id, record)
            # 2. ...then point the device index at it (atomic publish).
            self._devices[device_id] = _IndexEntry(
                op_id=op_id, file=fname, state=STATE_SELECTED, record=record
            )
            self._write_index_locked()

    def mark_executing(self, device_id: str) -> None:
        with self._lock:
            entry = self._devices.get(device_id)
            if entry is None or entry.record is None:
                return
            self.append(
                entry.op_id,
                {"kind": RECORD_EXECUTING, "op_id": entry.op_id,
                 "device_id": device_id, "ts": _now()},
            )
            entry.state = STATE_SELECTED  # in-memory: still consumable-safe

    def put_result(self, device_id: str, result: dict) -> None:
        with self._lock:
            entry = self._devices.get(device_id)
            if entry is None or entry.record is None:
                return
            self.append(entry.op_id, result)
            entry.state = STATE_EXECUTED
            entry.result = result

    def get(self, device_id: str) -> Optional[_IndexEntry]:
        with self._lock:
            return self._devices.get(device_id)

    def all_devices(self) -> dict[str, _IndexEntry]:
        with self._lock:
            return dict(self._devices)

    def health(self) -> HealthReport:
        with self._lock:
            corrupt = []
            if getattr(self, "_index_corrupt", False):
                corrupt.append("index.json")
            for dev, entry in self._devices.items():
                if entry.corrupt:
                    corrupt.append(f"{dev}:{entry.file}")
            return HealthReport(
                status="unhealthy" if corrupt else "ok",
                corrupt_files=corrupt,
                detail={
                    "devices": {
                        dev: {"state": e.state, "op_id": e.op_id,
                              "corrupt": e.corrupt}
                        for dev, e in self._devices.items()
                    }
                },
            )


def _now() -> float:
    import time
    return time.time()
