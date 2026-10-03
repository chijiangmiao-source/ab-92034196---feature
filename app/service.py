"""Selection / execution / withdrawal adjudication service.

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
7. While a choice is still unexecuted, the operator may **withdraw** it
   (device id + operation id + summary).  The withdrawal verdict is
   durable and immutable: identical retransmissions replay the original
   verdict (even after the device moved on to a new choice), a mismatched
   summary or an operation id that does not own the choice is an explicit
   conflict, and a withdrawn choice can never be executed or re-selected
   -- not even after a restart.
8. Withdrawal and execution racing the same choice are serialised into a
   single terminal outcome: if the withdrawal commits first the execution
   is rejected without ever touching the payload; if the execution
   completed first the withdrawal returns the already-executed result.
9. After a successful withdrawal the device immediately accepts a *new*
   choice (new operation id) and follows the normal select-execute flow.
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
    STATE_WITHDRAWN,
    RECORD_RESULT,
    RECORD_SELECTED,
    RECORD_WITHDRAWN,
)

# Verdicts
V_CHOICE_CREATED = "CHOICE_CREATED"
V_CHOICE_REPLAYED = "CHOICE_REPLAYED"          # identical retransmission
V_CHOICE_CONFLICT = "CHOICE_CONFLICT"          # same op_id, fields changed
V_CHOICE_DEVICE_BUSY = "CHOICE_DEVICE_BUSY"    # other live choice exists
V_CHOICE_DEVICE_EXECUTED = "CHOICE_DEVICE_EXECUTED"  # already executed
V_CHOICE_WITHDRAWN = "CHOICE_WITHDRAWN"        # op was withdrawn; terminal
V_CHOICE_EXPIRED_REQUEST = "CHOICE_EXPIRED_REQUEST"  # expires_at in past
V_CHOICE_BAD_REQUEST = "CHOICE_BAD_REQUEST"

V_EXEC_ACCEPTED = "EXEC_ACCEPTED"
V_EXEC_REPLAYED = "EXEC_REPLAYED"              # retx after completion
V_EXEC_NO_CHOICE = "EXEC_NO_CHOICE"
V_EXEC_WRONG_OP = "EXEC_WRONG_OP"              # different op id requested
V_EXEC_SUMMARY_MISMATCH = "EXEC_SUMMARY_MISMATCH"
V_EXEC_EXPIRED = "EXEC_EXPIRED"
V_EXEC_WITHDRAWN = "EXEC_WITHDRAWN"            # choice was withdrawn
V_EXEC_QUARANTINED = "EXEC_QUARANTINED"        # corrupt durable state

V_WITHDRAWN = "WITHDRAWN"                      # withdrawal committed
V_WITHDRAW_REPLAYED = "WITHDRAW_REPLAYED"      # identical withdrawal retx
V_WITHDRAW_CONFLICT = "WITHDRAW_CONFLICT"      # summary/op mismatch
V_WITHDRAW_NO_CHOICE = "WITHDRAW_NO_CHOICE"    # no choice for device/op
V_WITHDRAW_EXECUTED = "WITHDRAW_EXECUTED"      # execution already finished

SUCCESS_VERDICTS = {V_CHOICE_CREATED, V_CHOICE_REPLAYED,
                    V_EXEC_ACCEPTED, V_EXEC_REPLAYED,
                    V_WITHDRAWN, V_WITHDRAW_REPLAYED}


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
                if entry.state == STATE_WITHDRAWN:
                    # The withdrawn choice must never become executable
                    # again: re-selection under the same op id is refused
                    # regardless of the payload.  Use a new op id.
                    return Response(
                        V_CHOICE_WITHDRAWN, 409,
                        detail=(f"op_id={op_id} was withdrawn; "
                                f"create a new operation id instead"))
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
                # A withdrawn choice is dead: a new op id replaces it
                # (same rule as an expired choice).
                if entry.state != STATE_WITHDRAWN:
                    # Existing non-expired live choice blocks a new op id.
                    rec = entry.record or {}
                    if float(rec.get("expires_at", 0)) > self._clock():
                        return Response(
                            V_CHOICE_DEVICE_BUSY, 409,
                            detail=(f"device already holds live choice "
                                    f"op_id={entry.op_id}"))
                # Existing choice expired or withdrawn: the new op id
                # replaces it.  The old record file is left behind but
                # unreferenced and never revives (see RecordStore.put_choice);
                # a withdrawn op stays terminally withdrawn in its own log.

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
            if entry.state == STATE_WITHDRAWN:
                # Terminal: the withdrawal committed first, so this
                # execution must never touch the payload.
                return Response(V_EXEC_WITHDRAWN, 410,
                                detail="choice was withdrawn; "
                                       "command will not be dispatched")
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
    # withdraw
    # ------------------------------------------------------------------ #
    def withdraw(self, device_id: str, op_id: str, summary: str) -> Response:
        """Withdraw a still-unexecuted choice, terminally.

        The verdict is immutable and durable:

        * identical withdrawal retransmissions replay the original
          verdict (same ``withdrawn_at``), even after the device has moved
          on to a new choice -- the per-operation log is consulted;
        * a summary mismatch, or a live choice owned by a different
          operation id, is an explicit conflict;
        * if the execution already completed, the withdrawal returns the
          executed result (never a "processing" state);
        * if the withdrawal commits first, a concurrent execution is
          rejected and the payload is never dispatched.
        """
        err = self._validate(device_id, op_id, summary, None)
        if err:
            return err

        with self._device_lock(device_id):
            entry = self.store.get(device_id)

            if entry is not None and entry.corrupt:
                return Response(V_EXEC_QUARANTINED, 503,
                                detail="durable record unverifiable")

            # The requested operation owns the device's current choice.
            if (entry is not None and entry.op_id == op_id
                    and entry.record is not None):
                rec = entry.record
                if rec.get("summary") != summary:
                    return Response(
                        V_WITHDRAW_CONFLICT, 409,
                        detail=(f"summary does not match the choice for "
                                f"op_id={op_id}: stored "
                                f"summary={rec.get('summary')!r}, "
                                f"requested summary={summary!r}"))
                if entry.state == STATE_EXECUTED:
                    # Execution won the race: return the executed result.
                    return Response(
                        V_WITHDRAW_EXECUTED, 409,
                        result=self._public_result(entry.result),
                        detail="choice already executed; "
                               "withdrawal arrived too late")
                if entry.state == STATE_WITHDRAWN:
                    # Identical retransmission: replay the original verdict.
                    return Response(
                        V_WITHDRAW_REPLAYED, 200,
                        result=self._public_withdrawal(entry.withdrawal))
                # Live choice (executing-marker crashes recover to
                # SELECTED, so this also covers the safe-retry state):
                # commit the withdrawal durably *before* responding.
                record = {
                    "kind": RECORD_WITHDRAWN,
                    "device_id": device_id,
                    "op_id": op_id,
                    "summary": summary,
                    "withdrawn_at": self._clock(),
                }
                self.store.put_withdrawal(device_id, record)
                return Response(V_WITHDRAWN, 200,
                                result=self._public_withdrawal(record))

            # The device's current choice (if any) belongs to another
            # operation id.  The requested op may still own a *durable*
            # choice for this device (e.g. it was withdrawn and the device
            # then selected a new operation): consult the operation log so
            # retransmissions replay the original verdict.
            hist = self.store.load_op(op_id)
            if (hist is not None and hist.record is not None
                    and hist.record.get("device_id") == device_id):
                if hist.corrupt:
                    return Response(V_EXEC_QUARANTINED, 503,
                                    detail="durable record unverifiable")
                if hist.record.get("summary") != summary:
                    return Response(
                        V_WITHDRAW_CONFLICT, 409,
                        detail=(f"summary does not match the choice for "
                                f"op_id={op_id}: stored "
                                f"summary={hist.record.get('summary')!r}, "
                                f"requested summary={summary!r}"))
                if hist.state == STATE_WITHDRAWN:
                    return Response(
                        V_WITHDRAW_REPLAYED, 200,
                        result=self._public_withdrawal(hist.withdrawal))
                if hist.state == STATE_EXECUTED:
                    return Response(
                        V_WITHDRAW_EXECUTED, 409,
                        result=self._public_result(hist.result),
                        detail="choice already executed; "
                               "withdrawal arrived too late")
                # The op's choice was superseded without ever being
                # executed or withdrawn: it no longer belongs to this
                # operation -> explicit conflict.
                return Response(
                    V_WITHDRAW_CONFLICT, 409,
                    detail=(f"choice for op_id={op_id} is no longer the "
                            f"device's active choice"))

            if entry is not None and entry.record is not None:
                # A live choice exists but belongs to another operation.
                return Response(
                    V_WITHDRAW_CONFLICT, 409,
                    detail=(f"device's active choice belongs to "
                            f"op_id={entry.op_id}, not op_id={op_id}"))
            return Response(V_WITHDRAW_NO_CHOICE, 404,
                            detail="no choice for device/op_id")

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
        if entry.state == STATE_WITHDRAWN:
            body["withdrawal"] = self._public_withdrawal(entry.withdrawal)
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
    def _public_withdrawal(rec: Optional[dict]) -> Optional[dict]:
        if rec is None:
            return None
        return {
            "device_id": rec["device_id"],
            "op_id": rec["op_id"],
            "summary": rec["summary"],
            "state": STATE_WITHDRAWN,
            "withdrawn_at": rec.get("withdrawn_at"),
        }

    @staticmethod
    def _hard_exit(msg: str) -> None:
        import sys
        sys.stderr.write(f"CRASH-INJECTION: {msg}\n")
        sys.stderr.flush()
        os._exit(99)
