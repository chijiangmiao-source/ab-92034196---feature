"""Unit tests for select/execute/withdraw adjudication, concurrency and recovery."""

from __future__ import annotations

import glob
import json
import os
import shutil
import tempfile
import threading
import time
import unittest

from app.gateway import DeviceGateway
from app.service import (
    DutyService,
    V_CHOICE_CONFLICT,
    V_CHOICE_CREATED,
    V_CHOICE_DEVICE_BUSY,
    V_CHOICE_DEVICE_EXECUTED,
    V_CHOICE_EXPIRED_REQUEST,
    V_CHOICE_REPLAYED,
    V_CHOICE_WITHDRAWN,
    V_EXEC_ACCEPTED,
    V_EXEC_EXPIRED,
    V_EXEC_NO_CHOICE,
    V_EXEC_QUARANTINED,
    V_EXEC_REPLAYED,
    V_EXEC_SUMMARY_MISMATCH,
    V_EXEC_WITHDRAWN,
    V_EXEC_WRONG_OP,
    V_WITHDRAW_CONFLICT,
    V_WITHDRAW_EXECUTED,
    V_WITHDRAW_NO_CHOICE,
    V_WITHDRAW_REPLAYED,
    V_WITHDRAWN,
)
from app.store import (
    RecordStore,
    STATE_EXECUTED,
    STATE_SELECTED,
    STATE_WITHDRAWN,
    RECORD_EXECUTING,
)


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class ServiceFixture(unittest.TestCase):
    """Shared fixture: fresh store/gateway/service with a fake clock."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.clock = FakeClock()
        self.store = RecordStore(self.dir)
        self.gw = DeviceGateway(os.path.join(self.dir, "gateway.log"))
        self.svc = DutyService(self.store, self.gw, clock=self.clock)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def select(self, device="DEV-1", op="OP-1", summary="CMD:arm", ttl=100):
        return self.svc.select(device, op, summary, self.clock.t + ttl)

    def _fresh_service(self):
        """Simulate a process restart over the same durable directory."""
        store = RecordStore(self.dir)
        gw = DeviceGateway(os.path.join(self.dir, "gateway.log"))
        return DutyService(store, gw, clock=self.clock), store, gw


class ServiceCase(ServiceFixture):

    # ------------------------------------------------------------------ #
    # basic protocol
    # ------------------------------------------------------------------ #
    def test_happy_path_and_retx(self):
        r = self.select()
        self.assertEqual(r.verdict, V_CHOICE_CREATED)
        self.assertEqual(r.http_status, 201)

        # identical choice retransmission -> replay, not a second create
        r2 = self.select()
        self.assertEqual(r2.verdict, V_CHOICE_REPLAYED)
        self.assertEqual(r2.result["op_id"], "OP-1")

        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_ACCEPTED)
        receipt = e.result["outcome"]["receipt"]

        # execution retransmission -> same final result, same receipt
        e2 = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e2.verdict, V_EXEC_REPLAYED)
        self.assertEqual(e2.result["state"], STATE_EXECUTED)
        self.assertEqual(e2.result["outcome"]["receipt"], receipt)

        # exactly one physical downstream dispatch
        self.assertEqual(self.gw.dispatch_count, 1)

        # choice retx for an executed op also replays the result
        r3 = self.select()
        self.assertEqual(r3.verdict, V_EXEC_REPLAYED)
        self.assertEqual(r3.result["outcome"]["receipt"], receipt)

        # executed device cannot be selected again, even with new op id
        r4 = self.svc.select("DEV-1", "OP-OTHER", "CMD:arm",
                             self.clock.t + 100)
        self.assertEqual(r4.verdict, V_CHOICE_DEVICE_EXECUTED)
        self.assertEqual(r4.http_status, 409)

    def test_same_op_changed_fields_is_conflict(self):
        self.select()
        conflict = self.svc.select("DEV-1", "OP-1", "CMD:disarm",
                                   self.clock.t + 100)
        self.assertEqual(conflict.verdict, V_CHOICE_CONFLICT)
        self.assertEqual(conflict.http_status, 409)
        self.assertIn("fields changed", conflict.detail)

        conflict2 = self.svc.select("DEV-1", "OP-1", "CMD:arm",
                                    self.clock.t + 50)
        self.assertEqual(conflict2.verdict, V_CHOICE_CONFLICT)

        # original choice remains usable
        ok = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(ok.verdict, V_EXEC_ACCEPTED)

    def test_other_op_choice_rejected_while_live(self):
        self.select()
        other = self.svc.select("DEV-1", "OP-2", "CMD:arm",
                                self.clock.t + 100)
        self.assertEqual(other.verdict, V_CHOICE_DEVICE_BUSY)

    def test_execute_guards(self):
        self.select()
        self.assertEqual(
            self.svc.execute("DEV-1", "OP-2", "CMD:arm").verdict,
            V_EXEC_WRONG_OP)
        self.assertEqual(
            self.svc.execute("DEV-1", "OP-1", "CMD:disarm").verdict,
            V_EXEC_SUMMARY_MISMATCH)

        self.assertEqual(
            self.svc.execute("DEV-NOPE", "OP-1", "CMD:arm").verdict,
            V_EXEC_NO_CHOICE)

    def test_expired_choice_must_not_execute(self):
        self.select(ttl=10)
        self.clock.advance(11)
        r = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(r.verdict, V_EXEC_EXPIRED)
        self.assertEqual(r.http_status, 410)
        self.assertEqual(self.gw.dispatch_count, 0)

        # expired choice may be replaced by a *new* op id
        new = self.svc.select("DEV-1", "OP-2", "CMD:arm",
                              self.clock.t + 100)
        self.assertEqual(new.verdict, V_CHOICE_CREATED)
        # old op id is dead
        self.assertEqual(
            self.svc.execute("DEV-1", "OP-1", "CMD:arm").verdict,
            V_EXEC_WRONG_OP)
        ok = self.svc.execute("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(ok.verdict, V_EXEC_ACCEPTED)

    def test_cannot_select_with_past_expiry(self):
        r = self.svc.select("DEV-1", "OP-1", "CMD:arm",
                            self.clock.t - 1)
        self.assertEqual(r.verdict, V_CHOICE_EXPIRED_REQUEST)

    # ------------------------------------------------------------------ #
    # concurrency
    # ------------------------------------------------------------------ #
    def test_concurrent_executions_single_consumer(self):
        self.select()
        results = []
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()
            results.append(self.svc.execute("DEV-1", "OP-1", "CMD:arm"))

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start(); t2.start(); t1.join(); t2.join()

        verdicts = sorted(r.verdict for r in results)
        self.assertEqual(verdicts,
                         sorted([V_EXEC_ACCEPTED, V_EXEC_REPLAYED]))
        receipts = {r.result["outcome"]["receipt"] for r in results}
        self.assertEqual(len(receipts), 1)  # same final result
        self.assertEqual(self.gw.dispatch_count, 1)

    def test_concurrent_executions_under_contention(self):
        # Many competing requests: still exactly one first consumption.
        self.select()
        results = []
        start = threading.Event()

        def worker():
            start.wait()
            results.append(self.svc.execute("DEV-1", "OP-1", "CMD:arm"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        start.set()
        for t in threads:
            t.join()

        accepted = [r for r in results if r.verdict == V_EXEC_ACCEPTED]
        replayed = [r for r in results if r.verdict == V_EXEC_REPLAYED]
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(replayed), 7)
        self.assertEqual(self.gw.dispatch_count, 1)
        receipts = {r.result["outcome"]["receipt"] for r in results}
        self.assertEqual(len(receipts), 1)

    # ------------------------------------------------------------------ #
    # crash recovery
    # ------------------------------------------------------------------ #
    def test_recover_after_crash_before_result_durable(self):
        self.select()
        # Downstream dispatch happened, executing marker durable, then
        # power cut before the result record.
        self.gw.send("DEV-1", "OP-1", "CMD:arm", delay=0)
        self.store.append("OP-1", {"kind": RECORD_EXECUTING, "op_id": "OP-1",
                                   "device_id": "DEV-1", "ts": self.clock.t})

        svc2, store2, gw2 = self._fresh_service()
        entry = store2.get("DEV-1")
        self.assertEqual(entry.state, STATE_SELECTED)  # safe to retry

        r = svc2.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(r.verdict, V_EXEC_ACCEPTED)
        self.assertTrue(r.result["outcome"].get("replayed_downstream"))
        # still one physical dispatch across the "reboot"
        with open(os.path.join(self.dir, "gateway.log")) as fh:
            self.assertEqual(len(fh.read().splitlines()), 1)

    def test_recover_after_crash_after_result_durable(self):
        self.select()
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        receipt = e.result["outcome"]["receipt"]

        svc2, store2, gw2 = self._fresh_service()
        entry = store2.get("DEV-1")
        self.assertEqual(entry.state, STATE_EXECUTED)

        r = svc2.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(r.verdict, V_EXEC_REPLAYED)
        self.assertEqual(r.result["outcome"]["receipt"], receipt)
        with open(os.path.join(self.dir, "gateway.log")) as fh:
            self.assertEqual(len(fh.read().splitlines()), 1)

    def test_old_choice_does_not_revive_across_restart(self):
        self.select(ttl=10)
        self.clock.advance(11)
        self.svc.select("DEV-1", "OP-2", "CMD:arm", self.clock.t + 100)

        _, store2, _ = self._fresh_service()
        entry = store2.get("DEV-1")
        self.assertEqual(entry.op_id, "OP-2")
        self.assertEqual(entry.state, STATE_SELECTED)

    # ------------------------------------------------------------------ #
    # integrity
    # ------------------------------------------------------------------ #
    def test_corrupt_record_marks_health_unhealthy(self):
        self.select()
        path = glob.glob(os.path.join(self.dir, "op-*.log"))[0]
        with open(path, "ab") as fh:
            fh.write(b"GS-CHOICE-1|deadbeef|{\"kind\":\"result\"}\n")

        _, store2, _ = (None, RecordStore(self.dir), None)
        report = store2.health()
        self.assertEqual(report.status, "unhealthy")
        self.assertTrue(report.corrupt_files)

        svc3 = DutyService(store2, self.gw, clock=self.clock)
        r = svc3.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(r.verdict, V_EXEC_QUARANTINED)
        self.assertEqual(r.http_status, 503)
        r2 = svc3.select("DEV-1", "OP-9", "CMD:arm", self.clock.t + 10)
        self.assertEqual(r2.verdict, V_EXEC_QUARANTINED)

    def test_torn_trailing_write_detected(self):
        # A torn half-line from mid-write must be detected.
        self.select()
        path = glob.glob(os.path.join(self.dir, "op-*.log"))[0]
        with open(path, "ab") as fh:
            fh.write(b"GS-CHOICE-1|abc|partial")
        store2 = RecordStore(self.dir)
        self.assertEqual(store2.health().status, "unhealthy")

    def test_healthy_initially(self):
        self.select()
        self.assertEqual(RecordStore(self.dir).health().status, "ok")


class WithdrawalCase(ServiceFixture):
    """Withdrawal adjudication: terminal verdicts, replay, races, reboot."""

    # ------------------------------------------------------------------ #
    # basic withdrawal protocol
    # ------------------------------------------------------------------ #
    def test_withdraw_happy_path_and_retx(self):
        self.select()
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAWN)
        self.assertEqual(w.http_status, 200)
        self.assertEqual(w.result["state"], STATE_WITHDRAWN)
        withdrawn_at = w.result["withdrawn_at"]

        # identical withdrawal retransmission replays the original verdict
        w2 = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w2.verdict, V_WITHDRAW_REPLAYED)
        self.assertEqual(w2.result["withdrawn_at"], withdrawn_at)

        # the withdrawn choice must never execute
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_WITHDRAWN)
        self.assertEqual(e.http_status, 410)
        self.assertEqual(self.gw.dispatch_count, 0)

        # the withdrawn op id can never be re-selected
        r = self.select()
        self.assertEqual(r.verdict, V_CHOICE_WITHDRAWN)
        self.assertEqual(r.http_status, 409)

        # device_state exposes the terminal withdrawal
        st = self.svc.device_state("DEV-1")
        self.assertEqual(st.result["state"], STATE_WITHDRAWN)
        self.assertEqual(st.result["withdrawal"]["withdrawn_at"], withdrawn_at)

    def test_reselect_new_op_after_withdrawal(self):
        self.select()
        self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")

        # a new operation id follows the normal select-execute flow
        r = self.svc.select("DEV-1", "OP-2", "CMD:arm", self.clock.t + 100)
        self.assertEqual(r.verdict, V_CHOICE_CREATED)
        e = self.svc.execute("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_ACCEPTED)
        self.assertEqual(self.gw.dispatch_count, 1)

        # the old withdrawal still replays from durable history even
        # though the device has moved on (and been executed)
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAW_REPLAYED)
        self.assertEqual(w.result["op_id"], "OP-1")

    def test_withdraw_conflicts_and_no_choice(self):
        # no choice at all for the device
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAW_NO_CHOICE)
        self.assertEqual(w.http_status, 404)

        self.select()
        # summary mismatch -> explicit conflict
        w2 = self.svc.withdraw("DEV-1", "OP-1", "CMD:disarm")
        self.assertEqual(w2.verdict, V_WITHDRAW_CONFLICT)
        self.assertEqual(w2.http_status, 409)
        # the live choice belongs to another operation id -> conflict
        w3 = self.svc.withdraw("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(w3.verdict, V_WITHDRAW_CONFLICT)
        self.assertEqual(w3.http_status, 409)

        # conflicts did not disturb the original choice
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_ACCEPTED)

    def test_withdraw_after_execute_returns_executed_result(self):
        self.select()
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        receipt = e.result["outcome"]["receipt"]

        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAW_EXECUTED)
        self.assertEqual(w.http_status, 409)
        self.assertEqual(w.result["state"], STATE_EXECUTED)
        self.assertEqual(w.result["outcome"]["receipt"], receipt)

        # existing executed-device semantics are unchanged
        e2 = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e2.verdict, V_EXEC_REPLAYED)
        r = self.svc.select("DEV-1", "OP-9", "CMD:arm", self.clock.t + 100)
        self.assertEqual(r.verdict, V_CHOICE_DEVICE_EXECUTED)

    def test_withdraw_superseded_choice_conflicts(self):
        # OP-1 expires and is replaced by OP-2 without ever being
        # executed or withdrawn: it no longer belongs to OP-1.
        self.select(ttl=10)
        self.clock.advance(11)
        self.svc.select("DEV-1", "OP-2", "CMD:arm", self.clock.t + 100)

        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAW_CONFLICT)
        self.assertEqual(w.http_status, 409)

    def test_withdraw_expired_unexecuted_choice_commits(self):
        # An expired choice can never execute; withdrawing it is still a
        # valid terminal decision (nothing was dispatched).
        self.select(ttl=10)
        self.clock.advance(11)
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAWN)
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_WITHDRAWN)

    # ------------------------------------------------------------------ #
    # concurrency: withdrawal vs execution -> single terminal state
    # ------------------------------------------------------------------ #
    def test_withdraw_execute_race_single_terminal_state(self):
        for trial in range(4):
            device, op = f"DEV-R{trial}", f"OP-R{trial}"
            r = self.svc.select(device, op, "CMD:arm", self.clock.t + 100)
            self.assertEqual(r.verdict, V_CHOICE_CREATED)
            dispatched_before = self.gw.dispatch_count

            results = []
            barrier = threading.Barrier(6)

            def do_withdraw():
                barrier.wait()
                results.append(self.svc.withdraw(device, op, "CMD:arm"))

            def do_execute():
                barrier.wait()
                results.append(self.svc.execute(device, op, "CMD:arm"))

            threads = ([threading.Thread(target=do_withdraw)
                        for _ in range(3)]
                       + [threading.Thread(target=do_execute)
                          for _ in range(3)])
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            withdraws = [x for x in results
                         if x.verdict in (V_WITHDRAWN, V_WITHDRAW_REPLAYED,
                                          V_WITHDRAW_EXECUTED)]
            executes = [x for x in results
                        if x.verdict in (V_EXEC_ACCEPTED, V_EXEC_REPLAYED,
                                         V_EXEC_WITHDRAWN)]
            # every request reached a terminal verdict (never "processing")
            self.assertEqual(len(results), 6)
            self.assertEqual(len(withdraws), 3)
            self.assertEqual(len(executes), 3)

            dispatched = self.gw.dispatch_count - dispatched_before
            if dispatched:
                # execution committed first: exactly one first consumer,
                # withdrawals observe the executed result
                self.assertEqual(dispatched, 1)
                self.assertEqual(
                    [x.verdict for x in executes].count(V_EXEC_ACCEPTED), 1)
                self.assertTrue(all(
                    x.verdict == V_WITHDRAW_EXECUTED for x in withdraws))
                receipts = {x.result["outcome"]["receipt"]
                            for x in results}
                self.assertEqual(len(receipts), 1)
            else:
                # withdrawal committed first: payload never dispatched
                self.assertEqual(
                    [x.verdict for x in withdraws].count(V_WITHDRAWN), 1)
                self.assertEqual(
                    [x.verdict for x in withdraws].count(V_WITHDRAW_REPLAYED),
                    2)
                self.assertTrue(all(
                    x.verdict == V_EXEC_WITHDRAWN for x in executes))

    # ------------------------------------------------------------------ #
    # restart: withdrawn is a durable terminal state
    # ------------------------------------------------------------------ #
    def test_withdrawal_survives_restart(self):
        self.select()
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        withdrawn_at = w.result["withdrawn_at"]

        svc2, store2, gw2 = self._fresh_service()
        entry = store2.get("DEV-1")
        self.assertEqual(entry.state, STATE_WITHDRAWN)

        # terminal verdict replays identically after the reboot
        w2 = svc2.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w2.verdict, V_WITHDRAW_REPLAYED)
        self.assertEqual(w2.result["withdrawn_at"], withdrawn_at)

        # the withdrawn choice did not become executable again
        e = svc2.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_WITHDRAWN)
        self.assertEqual(gw2.dispatch_count, 0)
        r = svc2.select("DEV-1", "OP-1", "CMD:arm", self.clock.t + 100)
        self.assertEqual(r.verdict, V_CHOICE_WITHDRAWN)

        # a new operation id works after the reboot
        r2 = svc2.select("DEV-1", "OP-2", "CMD:arm", self.clock.t + 100)
        self.assertEqual(r2.verdict, V_CHOICE_CREATED)
        e2 = svc2.execute("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(e2.verdict, V_EXEC_ACCEPTED)

        # ... and the old withdrawal still replays after yet another
        # restart, even though the device has moved on to OP-2
        svc3, store3, gw3 = self._fresh_service()
        self.assertEqual(store3.get("DEV-1").op_id, "OP-2")
        w3 = svc3.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w3.verdict, V_WITHDRAW_REPLAYED)
        self.assertEqual(w3.result["withdrawn_at"], withdrawn_at)

    def test_withdraw_on_corrupt_device_quarantined(self):
        self.select()
        path = glob.glob(os.path.join(self.dir, "op-*.log"))[0]
        with open(path, "ab") as fh:
            fh.write(b"GS-CHOICE-1|deadbeef|{\"kind\":\"withdrawn\"}\n")
        store2 = RecordStore(self.dir)
        svc2 = DutyService(store2, self.gw, clock=self.clock)
        w = svc2.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_EXEC_QUARANTINED)
        self.assertEqual(w.http_status, 503)


if __name__ == "__main__":
    unittest.main(verbosity=2)
