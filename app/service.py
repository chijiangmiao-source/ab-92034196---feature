"""Selection / execution adjudication service.

Protocol (high-risk telecommands require *select-before-execute*):

1. The ground station creates a **choice** keyed by the stable operation
   id, carrying the device id, a command summary and an expiry instant.
2. Only an execution request with the *same* operation id, the *same*
   summary and a still-valid choice may consume it.
3. Retransmissions (identical content) of either the choice or the
   execution replay the original verdict; changed fields are an explicit
   conflict.
4. A device may hold at most one non-expired choice; after a successful
   execution it cannot be selected again.
5. Two concurrent execution requests for one valid choice: exactly one
   first consumption succeeds; the other blocks until the winner commits
   and then receives the *same final result* (never a "processing"
   state, never a second physical dispatch).
6. Choices under a different operation id are rejected; expired choices
   must not execute.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

from .gateway import DeviceGateway
from .store import (
    RecordStore,
    STATE_EXECUTED,
    STATE_SELECTED,
    RECORD_RESULT,
    RECORD_SELECTED,
)

# Verdicts
V_CHOICE_CREATED = "CHOICE_CREATED"
V_CHOICE_REPLAYED = "CHOICE_REPLAYED"          # identical retransmission
V_CHOICE_CONFLICT = "CHOICE_CONFLICT"          # same op_id, fields changed
V_CHOICE_DEVICE_BUSY = "CHOICE_DEVICE_BUSY"    # other live choice exists
V_CHOICE_DEVICE_EXECUTED = "CHOICE_DEVICE_EXECUTED"  # already executed
V_CHOICE_EXPIRED_REQUEST = "CHOICE_EXPIRED_REQUEST"  # expires_at in past
V_CHOICE_BAD_REQUEST = "CHOICE_BAD_REQUEST"

V_EXEC_ACCEPTED = "EXEC_ACCEPTED"
V_EXEC_REPLAYED = "EXEC_REPLAYED"              # retx after completion
V_EXEC_NO_CHOICE = "EXEC_NO_CHOICE"
V_EXEC_WRONG_OP = "EXEC_WRONG_OP"              # different op id requested
V_EXEC_SUMMARY_MISMATCH = "EXEC_SUMMARY_MISMATCH"
V_EXEC_EXPIRED = "EXEC_EXPIRED"
V_EXEC_QUARANTINED = "EXEC_QUARANTINED"        # corrupt durable state

SUCCESS_VERDICTS = {V_CHOICE_CREATED, V_CHOICE_REPLAYED,
                    V_EXEC_ACCEPTED, V_EXEC_REPLAYED}


@dataclass
class Response:
    verdict: str
    http_status: int
    result: Optional[dict] = None
    detail: Optional[str] = None

    def to_dict(self) -> dict:
        d = {"verdict": self.verdict}
        if self.result is not None:
            d["result"] = self.result
        if self.detail:
            d["detail"] = self.detail
        return d


class DutyService:
    def __init__(self, store: RecordStore, gateway: DeviceGateway,
                 clock=time.time):
        self.store = store
        self.gateway = gateway
        self._clock = clock
        # One lock per device serialises select/execute adjudication.
        # A competing execute request blocks on this lock; when it is
        # granted, the winner has already committed a final durable
        # result, so the competitor can only observe EXECUTED and replay
        # it -- never an in-progress state.
        self._device_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _device_lock(self, device_id: str) -> threading.Lock:
        with self._locks_guard:
            lk = self._device_locks.get(device_id)
            if lk is None:
                lk = threading.Lock()
                self._device_locks[device_id] = lk
            return lk

    # ------------------------------------------------------------------ #
    # select
    # ------------------------------------------------------------------ #
    def select(self, device_id: str, op_id: str, summary: str,
               expires_at: float) -> Response:
        err = self._validate(device_id, op_id, summary, expires_at)
        if err:
            return err
        expires_at = float(expires_at)
        if expires_at <= self._clock():
            return Response(V_CHOICE_EXPIRED_REQUEST, 400,
                            detail="expires_at must be in the future")

        with self._device_lock(device_id):
            entry = self.store.get(device_id)

            # Corrupt durable state: refuse to create choices.
            if entry is not None and entry.corrupt:
                return Response(V_EXEC_QUARANTINED, 503,
                                detail="durable record unverifiable")

            # Retransmission / duplicate of the same operation id.
            if entry is not None and entry.op_id == op_id and entry.record:
                rec = entry.record
                if (rec.get("summary") == summary
                        and float(rec.get("expires_at")) == expires_at):
                    if entry.state == STATE_EXECUTED:
                        # Choice retx for an already executed op: replay the
                        # original execution result (idempotent verdict).
                        return Response(V_EXEC_REPLAYED, 200,
                                        result=self._public_result(entry.result))
                    return Response(V_CHOICE_REPLAYED, 200,
                                    result=self._public_choice(rec))
                # Same op id but different payload -> explicit conflict.
                return Response(
                    V_CHOICE_CONFLICT, 409,
                    detail=(f"fields changed for op_id={op_id}: "
                            f"stored summary={rec.get('summary')!r}, "
                            f"expires_at={rec.get('expires_at')}; "
                            f"requested summary={summary!r}, "
                            f"expires_at={expires_at}"))

            # A different op id for the same device.
            if entry is not None:
                if entry.state == STATE_EXECUTED:
                    return Response(V_CHOICE_DEVICE_EXECUTED, 409,
                                    detail="device already executed; "
                                           "no new choice allowed")
                # Existing non-expired live choice blocks a new op id.
                rec = entry.record or {}
                if float(rec.get("expires_at", 0)) > self._clock():
                    return Response(
                        V_CHOICE_DEVICE_BUSY, 409,
                        detail=(f"device already holds live choice "
                                f"op_id={entry.op_id}"))
                # Existing choice expired: the new op id replaces it.
                # The old record file is left behind but unreferenced and
                # never revives (see RecordStore.put_choice).

            record = {
                "kind": RECORD_SELECTED,
                "device_id": device_id,
                "op_id": op_id,
                "summary": summary,
                "expires_at": expires_at,
                "created_at": self._clock(),
            }
            self.store.put_choice(device_id, record)
            return Response(V_CHOICE_CREATED, 201,
                            result=self._public_choice(record))

    # ------------------------------------------------------------------ #
    # execute
    # ------------------------------------------------------------------ #
    def execute(self, device_id: str, op_id: str, summary: str,
                _crash: Optional[str] = None) -> Response:
        """Consume a valid choice and dispatch the command.

        ``_crash`` is a test hook simulating the two power-cut points:

        * ``"before_result"``: hard-exit after dispatch / executing
          record but before the result record is durable;
        * ``"after_result"``: hard-exit right after the result record is
          durable.
        """
        err = self._validate(device_id, op_id, summary, None)
        if err:
            return err

        with self._device_lock(device_id):
            entry = self.store.get(device_id)

            if entry is None or entry.record is None:
                return Response(V_EXEC_NO_CHOICE, 404,
                                detail="no choice for device")
            if entry.corrupt:
                return Response(V_EXEC_QUARANTINED, 503,
                                detail="durable record unverifiable")
            if entry.op_id != op_id:
                return Response(V_EXEC_WRONG_OP, 409,
                                detail=f"live choice belongs to "
                                       f"op_id={entry.op_id}")
            rec = entry.record
            if rec.get("summary") != summary:
                return Response(V_EXEC_SUMMARY_MISMATCH, 409,
                                detail="summary does not match the choice")
            if float(rec.get("expires_at")) <= self._clock():
                return Response(V_EXEC_EXPIRED, 410,
                                detail="choice expired")

            if entry.state == STATE_EXECUTED and entry.result is not None:
                # Duplicate execution request / retx: replay final result.
                return Response(V_EXEC_REPLAYED, 200,
                                result=self._public_result(entry.result))

            # --- first consumption: claim, dispatch, persist result ---
            self.store.mark_executing(device_id)

            outcome = self.gateway.send(device_id, op_id, summary)

            if _crash == "before_result":
                self._hard_exit("simulated power cut BEFORE result durable")

            result = {
                "kind": RECORD_RESULT,
                "device_id": device_id,
                "op_id": op_id,
                "summary": summary,
                "outcome": outcome,
                "finished_at": self._clock(),
            }
            self.store.put_result(device_id, result)

            if _crash == "after_result":
                self._hard_exit("simulated power cut AFTER result durable")

            return Response(V_EXEC_ACCEPTED, 200,
                            result=self._public_result(result))

    # ------------------------------------------------------------------ #
    # health / introspection
    # ------------------------------------------------------------------ #
    def health(self) -> Response:
        report = self.store.health()
        http = 200 if report.status == "ok" else 503
        return Response("HEALTH_OK" if http == 200 else "HEALTH_UNHEALTHY",
                        http, result={
                            "status": report.status,
                            "corrupt_files": report.corrupt_files,
                            **report.detail,
                        })

    def device_state(self, device_id: str) -> Response:
        entry = self.store.get(device_id)
        if entry is None or entry.record is None:
            return Response("DEVICE_NO_CHOICE", 404)
        body = {
            "device_id": device_id,
            "op_id": entry.op_id,
            "state": entry.state,
            "corrupt": entry.corrupt,
        }
        if entry.state == STATE_EXECUTED:
            body["result"] = self._public_result(entry.result)
        return Response("DEVICE_STATE", 200, result=body)

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _validate(device_id, op_id, summary, expires_at) -> Optional[Response]:
        if not device_id or not isinstance(device_id, str):
            return Response(V_CHOICE_BAD_REQUEST, 400, detail="device_id")
        if not op_id or not isinstance(op_id, str):
            return Response(V_CHOICE_BAD_REQUEST, 400, detail="op_id")
        if not summary or not isinstance(summary, str):
            return Response(V_CHOICE_BAD_REQUEST, 400, detail="summary")
        if expires_at is not None and not isinstance(expires_at, (int, float)):
            return Response(V_CHOICE_BAD_REQUEST, 400, detail="expires_at")
        return None

    @staticmethod
    def _public_choice(rec: dict) -> dict:
        return {
            "device_id": rec["device_id"],
            "op_id": rec["op_id"],
            "summary": rec["summary"],
            "expires_at": rec["expires_at"],
            "state": STATE_SELECTED,
        }

    @staticmethod
    def _public_result(rec: Optional[dict]) -> Optional[dict]:
        if rec is None:
            return None
        return {
            "device_id": rec["device_id"],
            "op_id": rec["op_id"],
            "summary": rec["summary"],
            "state": STATE_EXECUTED,
            "outcome": rec.get("outcome"),
            "finished_at": rec.get("finished_at"),
        }

    @staticmethod
    def _hard_exit(msg: str) -> None:
        import sys
        sys.stderr.write(f"CRASH-INJECTION: {msg}\n")
        sys.stderr.flush()
        os._exit(99)
