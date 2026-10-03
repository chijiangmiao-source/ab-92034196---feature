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
    V_WITHDRAW_ALREADY_EXECUTED,
    V_WITHDRAW_NO_CHOICE,
    V_WITHDRAW_REPLAYED,
    V_WITHDRAW_SUMMARY_MISMATCH,
    V_WITHDRAW_WRONG_OP,
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


class ServiceCase(unittest.TestCase):
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
    # withdrawal
    # ------------------------------------------------------------------ #
    def test_withdraw_happy_path_and_retx(self):
        self.select()
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAWN)
        self.assertEqual(w.http_status, 200)
        self.assertEqual(w.result["state"], STATE_WITHDRAWN)
        self.assertEqual(w.result["withdrawn_at"], self.clock.t)

        # identical retransmission replays the immutable verdict
        w2 = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w2.verdict, V_WITHDRAW_REPLAYED)
        self.assertEqual(w2.result["withdrawn_at"], w.result["withdrawn_at"])

        # the withdrawn choice can never execute; payload untouched
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_WITHDRAWN)
        self.assertEqual(e.http_status, 410)
        self.assertEqual(self.gw.dispatch_count, 0)

        # the withdrawn op id stays closed for re-selection
        r = self.select()
        self.assertEqual(r.verdict, V_CHOICE_WITHDRAWN)
        self.assertEqual(r.http_status, 409)

    def test_withdraw_conflicts_keep_choice_alive(self):
        self.select()
        wrong_op = self.svc.withdraw("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(wrong_op.verdict, V_WITHDRAW_WRONG_OP)
        self.assertEqual(wrong_op.http_status, 409)

        bad_summary = self.svc.withdraw("DEV-1", "OP-1", "CMD:disarm")
        self.assertEqual(bad_summary.verdict, V_WITHDRAW_SUMMARY_MISMATCH)
        self.assertEqual(bad_summary.http_status, 409)

        no_choice = self.svc.withdraw("DEV-NOPE", "OP-1", "CMD:arm")
        self.assertEqual(no_choice.verdict, V_WITHDRAW_NO_CHOICE)
        self.assertEqual(no_choice.http_status, 404)

        # failed withdrawals did not disturb the live choice
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_ACCEPTED)

    def test_withdraw_after_execute_returns_executed_result(self):
        self.select()
        e = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        receipt = e.result["outcome"]["receipt"]

        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAW_ALREADY_EXECUTED)
        self.assertEqual(w.http_status, 409)
        self.assertEqual(w.result["state"], STATE_EXECUTED)
        self.assertEqual(w.result["outcome"]["receipt"], receipt)

    def test_reselect_with_new_op_after_withdraw(self):
        self.select()
        self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")

        # a new op id follows the ordinary select-execute flow
        r = self.svc.select("DEV-1", "OP-2", "CMD:arm", self.clock.t + 100)
        self.assertEqual(r.verdict, V_CHOICE_CREATED)
        e = self.svc.execute("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_ACCEPTED)
        self.assertEqual(self.gw.dispatch_count, 1)

        # the withdrawn op id stays dead
        self.assertEqual(
            self.svc.execute("DEV-1", "OP-1", "CMD:arm").verdict,
            V_EXEC_WRONG_OP)
        # ...and once the device has executed, the executed-device
        # semantics govern any further selection attempt
        self.assertEqual(
            self.svc.select("DEV-1", "OP-1", "CMD:arm",
                            self.clock.t + 100).verdict,
            V_CHOICE_DEVICE_EXECUTED)

    def test_withdraw_expired_choice(self):
        self.select(ttl=10)
        self.clock.advance(11)
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAWN)
        self.assertEqual(
            self.svc.execute("DEV-1", "OP-1", "CMD:arm").verdict,
            V_EXEC_WITHDRAWN)

    # ------------------------------------------------------------------ #
    # withdrawal vs execution races
    # ------------------------------------------------------------------ #
    def test_withdraw_commit_blocks_concurrent_execute(self):
        # Withdrawal commits while an execute request is in flight:
        # the execute must never touch the payload.
        self.select()
        entered = threading.Event()
        orig_put = self.store.put_withdrawal

        def slow_put(device_id, record):
            entered.set()
            time.sleep(0.2)  # withdrawal in flight, device lock held
            return orig_put(device_id, record)

        self.store.put_withdrawal = slow_put
        out = {}
        t = threading.Thread(
            target=lambda: out.__setitem__(
                "w", self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")))
        t.start()
        self.assertTrue(entered.wait(2))
        out["e"] = self.svc.execute("DEV-1", "OP-1", "CMD:arm")
        t.join()

        self.assertEqual(out["w"].verdict, V_WITHDRAWN)
        self.assertEqual(out["e"].verdict, V_EXEC_WITHDRAWN)
        self.assertEqual(self.gw.dispatch_count, 0)  # payload untouched

    def test_execute_in_flight_blocks_concurrent_withdraw(self):
        # Execution commits while a withdraw request is in flight:
        # the withdraw receives the final executed result.
        self.select()
        entered = threading.Event()
        release = threading.Event()
        orig_send = self.gw.send

        def slow_send(device_id, op_id, summary, delay=0.05):
            entered.set()
            release.wait(5)
            return orig_send(device_id, op_id, summary, delay=0)

        self.gw.send = slow_send
        out = {}
        t = threading.Thread(
            target=lambda: out.__setitem__(
                "e", self.svc.execute("DEV-1", "OP-1", "CMD:arm")))
        t.start()
        self.assertTrue(entered.wait(2))  # execution is mid-dispatch

        w_out = {}
        t2 = threading.Thread(
            target=lambda: w_out.__setitem__(
                "w", self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")))
        t2.start()
        time.sleep(0.1)  # withdraw is now blocked on the device lock
        release.set()
        t.join()
        t2.join()

        self.assertEqual(out["e"].verdict, V_EXEC_ACCEPTED)
        w = w_out["w"]
        self.assertEqual(w.verdict, V_WITHDRAW_ALREADY_EXECUTED)
        self.assertEqual(w.result["outcome"]["receipt"],
                         out["e"].result["outcome"]["receipt"])
        self.assertEqual(self.gw.dispatch_count, 1)

    def test_withdraw_execute_race_single_terminal_state(self):
        exec_wins = 0
        for i in range(10):
            dev, op = f"DEV-R{i}", f"OP-R{i}"
            self.svc.select(dev, op, "CMD:arm", self.clock.t + 100)
            out = {}
            barrier = threading.Barrier(2)

            def do_withdraw():
                barrier.wait()
                out["w"] = self.svc.withdraw(dev, op, "CMD:arm")

            def do_execute():
                barrier.wait()
                out["e"] = self.svc.execute(dev, op, "CMD:arm")

            t1 = threading.Thread(target=do_withdraw)
            t2 = threading.Thread(target=do_execute)
            t1.start(); t2.start(); t1.join(); t2.join()

            w, e = out["w"], out["e"]
            if w.verdict == V_WITHDRAWN:
                # withdrawal committed first: payload never touched
                self.assertEqual(e.verdict, V_EXEC_WITHDRAWN)
            elif w.verdict == V_WITHDRAW_ALREADY_EXECUTED:
                # execution completed first: same final result
                self.assertEqual(e.verdict, V_EXEC_ACCEPTED)
                self.assertEqual(w.result["outcome"]["receipt"],
                                 e.result["outcome"]["receipt"])
                exec_wins += 1
            else:
                self.fail(f"unexpected withdraw verdict {w.verdict}")
        # exactly one physical dispatch per round the execution won
        self.assertEqual(self.gw.dispatch_count, exec_wins)

    # ------------------------------------------------------------------ #
    # crash recovery
    # ------------------------------------------------------------------ #
    def _fresh_service(self):
        store = RecordStore(self.dir)
        gw = DeviceGateway(os.path.join(self.dir, "gateway.log"))
        return DutyService(store, gw, clock=self.clock), store, gw

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

    def test_withdraw_final_state_replays_across_restart(self):
        self.select()
        w = self.svc.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w.verdict, V_WITHDRAWN)

        svc2, store2, gw2 = self._fresh_service()
        entry = store2.get("DEV-1")
        self.assertEqual(entry.state, STATE_WITHDRAWN)

        # the immutable verdict replays identically after the reboot
        w2 = svc2.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(w2.verdict, V_WITHDRAW_REPLAYED)
        self.assertEqual(w2.result["withdrawn_at"],
                         w.result["withdrawn_at"])

        # the withdrawn choice never becomes executable again
        e = svc2.execute("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(e.verdict, V_EXEC_WITHDRAWN)
        self.assertEqual(gw2.dispatch_count, 0)

        # the withdrawn op id stays closed; a new op id works normally
        r = svc2.select("DEV-1", "OP-1", "CMD:arm", self.clock.t + 100)
        self.assertEqual(r.verdict, V_CHOICE_WITHDRAWN)
        r2 = svc2.select("DEV-1", "OP-2", "CMD:arm", self.clock.t + 100)
        self.assertEqual(r2.verdict, V_CHOICE_CREATED)
        e2 = svc2.execute("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(e2.verdict, V_EXEC_ACCEPTED)

        # the re-selected flow itself survives another restart
        svc3, store3, _ = self._fresh_service()
        self.assertEqual(store3.get("DEV-1").state, STATE_EXECUTED)
        e3 = svc3.execute("DEV-1", "OP-2", "CMD:arm")
        self.assertEqual(e3.verdict, V_EXEC_REPLAYED)

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
        r3 = svc3.withdraw("DEV-1", "OP-1", "CMD:arm")
        self.assertEqual(r3.verdict, V_EXEC_QUARANTINED)

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
