#!/usr/bin/env python3
"""One-shot acceptance service for the high-risk telecommand duty system.

Phases (exits non-zero on any failure):

  1. Code tests         - unittest suite (adjudication, concurrency,
                          crash recovery, checksum integrity)
  2. Build check        - byte-compile all sources
  3. HTTP/API smoke     - real server subprocesses exercising:
                            * /healthz and the select->execute flow
                            * retransmission replay and explicit conflicts
                            * N concurrent execution requests racing one
                              valid choice (exactly one first consumer)
                            * expired choice rejection
                            * power cut BEFORE the result is durable,
                              restart, safe retry (single physical dispatch)
                            * power cut AFTER the result is durable,
                              restart, replay of the identical final result
                            * corrupt durable record -> /healthz 503
  4. Remote smoke       - if BASE_URL is set (e.g. the compose `duty`
                          service), run health + full flow against it
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PASS = "PASS"
FAIL = "FAIL"
failures: list[str] = []


def report(name: str, ok: bool, detail: str = "") -> None:
    mark = PASS if ok else FAIL
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


# ---------------------------------------------------------------------- #
# HTTP helpers
# ---------------------------------------------------------------------- #
def http(method: str, url: str, body: dict | None = None, timeout: float = 10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, data_dir: str, port: int):
        self.data_dir = data_dir
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.log_path = os.path.join(data_dir, "server.log")

    def start(self) -> None:
        env = dict(os.environ, DATA_DIR=self.data_dir,
                   GATEWAY_LOG=os.path.join(self.data_dir, "gateway.log"),
                   HOST="127.0.0.1", PORT=str(self.port))
        self.log = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app.server"],
            cwd=ROOT, env=env, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"server exited early rc={self.proc.returncode}; "
                    f"see {self.log_path}")
            try:
                status, _ = http("GET", f"http://127.0.0.1:{self.port}/healthz")
                if status in (200, 503):
                    return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("server did not become ready")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log.close()

    def wait_exit(self, timeout: float = 10) -> int | None:
        assert self.proc is not None
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.proc.poll()
            if rc is not None:
                self.log.close()
                return rc
            time.sleep(0.05)
        return None

    def gateway_dispatches(self) -> int:
        path = os.path.join(self.data_dir, "gateway.log")
        if not os.path.exists(path):
            return 0
        with open(path) as fh:
            return len([ln for ln in fh.read().splitlines() if ln.strip()])


def fresh_dir() -> str:
    return tempfile.mkdtemp(prefix="verify-")


# ---------------------------------------------------------------------- #
# phase 1/2: tests + build
# ---------------------------------------------------------------------- #
def phase_code() -> None:
    section("PHASE 1: code tests (unittest)")
    rc = subprocess.call(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests",
         "-t", ".", "-v"], cwd=ROOT)
    report("unittest discovery", rc == 0, f"rc={rc}")

    section("PHASE 2: build check (compileall)")
    rc = subprocess.call(
        [sys.executable, "-m", "compileall", "-q", "app", "tests",
         "scripts"],
        cwd=ROOT)
    report("compileall app/tests/scripts", rc == 0, f"rc={rc}")


# ---------------------------------------------------------------------- #
# phase 3: HTTP smoke
# ---------------------------------------------------------------------- #
def smoke_full_flow(base: str) -> None:
    section("PHASE 3a: health + select/execute protocol over HTTP")
    status, body = http("GET", f"{base}/healthz")
    report("initial healthz 200/ok",
           status == 200 and body["result"]["status"] == "ok",
           f"{status} {body}")

    exp = time.time() + 60
    status, body = http("POST", f"{base}/v1/choices",
                        {"device_id": "DEV-A", "op_id": "OP-A",
                         "summary": "CMD:arm", "expires_at": exp})
    report("create choice", status == 201 and body["verdict"] == "CHOICE_CREATED",
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/choices",
                        {"device_id": "DEV-A", "op_id": "OP-A",
                         "summary": "CMD:arm", "expires_at": exp})
    report("identical choice retx replays",
           status == 200 and body["verdict"] == "CHOICE_REPLAYED",
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/choices",
                        {"device_id": "DEV-A", "op_id": "OP-A",
                         "summary": "CMD:disarm", "expires_at": exp})
    report("same op id, changed summary -> 409 conflict",
           status == 409 and body["verdict"] == "CHOICE_CONFLICT",
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/choices",
                        {"device_id": "DEV-A", "op_id": "OP-B",
                         "summary": "CMD:arm", "expires_at": exp})
    report("other op id while live -> 409 device busy",
           status == 409 and body["verdict"] == "CHOICE_DEVICE_BUSY",
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/executions",
                        {"device_id": "DEV-A", "op_id": "OP-B",
                         "summary": "CMD:arm"})
    report("execute with wrong op id rejected",
           status == 409 and body["verdict"] == "EXEC_WRONG_OP",
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/executions",
                        {"device_id": "DEV-A", "op_id": "OP-A",
                         "summary": "CMD:disarm"})
    report("execute with mismatched summary rejected",
           status == 409 and body["verdict"] == "EXEC_SUMMARY_MISMATCH",
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/executions",
                        {"device_id": "DEV-A", "op_id": "OP-A",
                         "summary": "CMD:arm"})
    ok = status == 200 and body["verdict"] == "EXEC_ACCEPTED"
    receipt = body.get("result", {}).get("outcome", {}).get("receipt")
    report("first execute accepted", ok, f"{status} {body}")

    status, body = http("POST", f"{base}/v1/executions",
                        {"device_id": "DEV-A", "op_id": "OP-A",
                         "summary": "CMD:arm"})
    report("execute retx replays same final result",
           status == 200 and body["verdict"] == "EXEC_REPLAYED"
           and body["result"]["outcome"]["receipt"] == receipt,
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/choices",
                        {"device_id": "DEV-A", "op_id": "OP-A",
                         "summary": "CMD:arm", "expires_at": exp})
    report("choice retx after execution replays result",
           status == 200 and body["verdict"] == "EXEC_REPLAYED"
           and body["result"]["outcome"]["receipt"] == receipt,
           f"{status} {body}")

    status, body = http("POST", f"{base}/v1/choices",
                        {"device_id": "DEV-A", "op_id": "OP-Z",
                         "summary": "CMD:arm", "expires_at": exp})
    report("executed device cannot be selected again",
           status == 409 and body["verdict"] == "CHOICE_DEVICE_EXECUTED",
           f"{status} {body}")


def smoke_concurrency(data_dir: str) -> None:
    section("PHASE 3b: concurrent execution requests race one choice")
    port = free_port()
    srv = Server(data_dir, port)
    srv.start()
    try:
        base = f"http://127.0.0.1:{port}"
        http("POST", f"{base}/v1/choices",
             {"device_id": "DEV-B", "op_id": "OP-B",
              "summary": "CMD:arm", "expires_at": time.time() + 60})

        outcomes = []
        lock = threading.Lock()

        def fire():
            try:
                st, bd = http("POST", f"{base}/v1/executions",
                              {"device_id": "DEV-B", "op_id": "OP-B",
                               "summary": "CMD:arm"}, timeout=30)
            except OSError as exc:  # pragma: no cover - failure path
                with lock:
                    outcomes.append(("CONN_ERROR", str(exc), None))
                return
            rcpt = bd.get("result", {}).get("outcome", {}).get("receipt")
            with lock:
                outcomes.append((bd.get("verdict"), st, rcpt))

        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda _: fire(), range(6)))

        verdicts = sorted(o[0] for o in outcomes)
        accepted = [o for o in outcomes if o[0] == "EXEC_ACCEPTED"]
        replayed = [o for o in outcomes if o[0] == "EXEC_REPLAYED"]
        receipts = {o[2] for o in outcomes}
        n_dispatched = srv.gateway_dispatches()

        print(f"    verdicts={verdicts}")
        print(f"    physical downstream dispatches={n_dispatched}")

        report("exactly one first consumer", len(accepted) == 1, str(verdicts))
        report("all competitors got the same final result",
               len(replayed) == 5 and receipts == {accepted[0][2]},
               f"receipts={receipts}")
        report("no competitor saw a processing/intermediate state",
               set(verdicts) <= {"EXEC_ACCEPTED", "EXEC_REPLAYED"},
               str(verdicts))
        report("exactly one physical dispatch", n_dispatched == 1,
               f"{n_dispatched}")
    finally:
        srv.stop()


def smoke_expiry(data_dir: str) -> None:
    section("PHASE 3c: expired choice must not execute")
    port = free_port()
    srv = Server(data_dir, port)
    srv.start()
    try:
        base = f"http://127.0.0.1:{port}"
        http("POST", f"{base}/v1/choices",
             {"device_id": "DEV-C", "op_id": "OP-C",
              "summary": "CMD:arm", "expires_at": time.time() + 1.5})
        print("    waiting for choice to expire ...")
        time.sleep(2)
        status, body = http("POST", f"{base}/v1/executions",
                            {"device_id": "DEV-C", "op_id": "OP-C",
                             "summary": "CMD:arm"})
        report("expired execute -> 410 EXEC_EXPIRED",
               status == 410 and body["verdict"] == "EXEC_EXPIRED",
               f"{status} {body}")

        status, body = http("POST", f"{base}/v1/choices",
                            {"device_id": "DEV-C", "op_id": "OP-C2",
                             "summary": "CMD:arm",
                             "expires_at": time.time() + 60})
        report("new op id may replace the expired choice",
               status == 201 and body["verdict"] == "CHOICE_CREATED",
               f"{status} {body}")
        status, body = http("POST", f"{base}/v1/executions",
                            {"device_id": "DEV-C", "op_id": "OP-C",
                             "summary": "CMD:arm"})
        report("old expired choice cannot be executed",
               status == 409 and body["verdict"] == "EXEC_WRONG_OP",
               f"{status} {body}")
    finally:
        srv.stop()


def _crash_execute(data_dir: str, device: str, op: str, when: str):
    """Start server, crash it at the requested point, return receipt seen."""
    port = free_port()
    srv = Server(data_dir, port)
    srv.start()
    base = f"http://127.0.0.1:{port}"
    status, body = http("POST", f"{base}/v1/choices",
                        {"device_id": device, "op_id": op,
                         "summary": "CMD:arm",
                         "expires_at": time.time() + 300})
    assert status == 201, (status, body)

    try:
        # The crash hook hard-kills the server process, so the HTTP
        # connection is expected to fail.
        http("POST", f"{base}/v1/executions",
             {"device_id": device, "op_id": op, "summary": "CMD:arm",
              "crash": when}, timeout=5)
        rc = srv.wait_exit()
    except OSError:
        rc = srv.wait_exit()
    return srv, rc


def smoke_crash_before_result(data_dir: str) -> None:
    section("PHASE 3d: power cut BEFORE result durable -> safe retry")
    srv, rc = _crash_execute(data_dir, "DEV-D", "OP-D", "before_result")
    report("server process died from injected crash", rc == 99, f"rc={rc}")
    try:
        report("dispatch reached the (idempotent) downstream once",
               srv.gateway_dispatches() == 1,
               f"{srv.gateway_dispatches()}")

        # ---- restart with the same durable directory ----
        srv2 = Server(data_dir, srv.port)
        srv2.start()
        try:
            base = f"http://127.0.0.1:{srv.port}"
            status, body = http("GET", f"{base}/v1/devices/DEV-D")
            report("recovered to safe-retry SELECTED state",
                   status == 200 and body["result"]["state"] == "SELECTED",
                   f"{status} {body}")

            status, body = http("POST", f"{base}/v1/executions",
                                {"device_id": "DEV-D", "op_id": "OP-D",
                                 "summary": "CMD:arm"})
            ok = (status == 200
                  and body["verdict"] == "EXEC_ACCEPTED"
                  and body["result"]["outcome"].get("replayed_downstream")
                  is True)
            receipt = body.get("result", {}).get("outcome", {}).get("receipt")
            report("retry succeeds via idempotent downstream replay",
                   ok, f"{status} {body}")

            status, body = http("POST", f"{base}/v1/executions",
                                {"device_id": "DEV-D", "op_id": "OP-D",
                                 "summary": "CMD:arm"})
            report("post-recovery retx replays same result",
                   status == 200 and body["verdict"] == "EXEC_REPLAYED"
                   and body["result"]["outcome"]["receipt"] == receipt,
                   f"{status} {body}")
            report("exactly one physical dispatch across the reboot",
                   srv2.gateway_dispatches() == 1,
                   f"{srv2.gateway_dispatches()}")
        finally:
            srv2.stop()
    finally:
        if srv.proc and srv.proc.poll() is None:
            srv.stop()


def smoke_crash_after_result(data_dir: str) -> None:
    section("PHASE 3e: power cut AFTER result durable -> replay executed")
    srv, rc = _crash_execute(data_dir, "DEV-E", "OP-E", "after_result")
    report("server process died from injected crash", rc == 99, f"rc={rc}")
    try:
        report("exactly one physical dispatch",
               srv.gateway_dispatches() == 1, f"{srv.gateway_dispatches()}")

        srv2 = Server(data_dir, srv.port)
        srv2.start()
        try:
            base = f"http://127.0.0.1:{srv.port}"
            status, body = http("GET", f"{base}/v1/devices/DEV-E")
            report("recovered straight to EXECUTED",
                   status == 200 and body["result"]["state"] == "EXECUTED",
                   f"{status} {body}")
            executed_receipt = (body["result"]
                                .get("result", {}).get("outcome", {})
                                .get("receipt"))

            status, body = http("POST", f"{base}/v1/executions",
                                {"device_id": "DEV-E", "op_id": "OP-E",
                                 "summary": "CMD:arm"})
            report("retx after reboot replays identical final result",
                   status == 200 and body["verdict"] == "EXEC_REPLAYED"
                   and body["result"]["outcome"]["receipt"] == executed_receipt,
                   f"{status} {body}")
            report("no second physical dispatch after reboot",
                   srv2.gateway_dispatches() == 1,
                   f"{srv2.gateway_dispatches()}")

            status, body = http("POST", f"{base}/v1/choices",
                                {"device_id": "DEV-E", "op_id": "OP-E2",
                                 "summary": "CMD:arm",
                                 "expires_at": time.time() + 60})
            report("old choice did not revive; executed device stays closed",
                   status == 409
                   and body["verdict"] == "CHOICE_DEVICE_EXECUTED",
                   f"{status} {body}")
        finally:
            srv2.stop()
    finally:
        if srv.proc and srv.proc.poll() is None:
            srv.stop()


def smoke_corruption(data_dir: str) -> None:
    section("PHASE 3f: unverifiable durable record -> health abnormal")
    # Corrupt the executed device's record log, then restart so the store
    # re-verifies every persisted record.
    logs = glob.glob(os.path.join(data_dir, "op-*.log"))
    report("at least one durable record file exists", len(logs) >= 1, str(logs))
    with open(logs[0], "ab") as fh:
        fh.write(b"GS-CHOICE-1|0000000000000000|{\"kind\":\"result\"}\n")

    port = free_port()
    srv = Server(data_dir, port)
    srv.start()
    try:
        base = f"http://127.0.0.1:{port}"
        status, body = http("GET", f"{base}/healthz")
        report("healthz reports 503/unhealthy with corrupt file",
               status == 503 and body["verdict"] == "HEALTH_UNHEALTHY"
               and body["result"]["status"] == "unhealthy"
               and len(body["result"]["corrupt_files"]) >= 1,
               f"{status} {body}")
        status, body = http("POST", f"{base}/v1/executions",
                            {"device_id": "DEV-E", "op_id": "OP-E",
                             "summary": "CMD:arm"})
        report("execution on quarantined device rejected",
               status == 503 and body["verdict"] == "EXEC_QUARANTINED",
               f"{status} {body}")
    finally:
        srv.stop()


def phase_http() -> list[str]:
    dirs = [fresh_dir() for _ in range(6)]
    try:
        # 3a: basic flow
        port = free_port()
        srv = Server(dirs[0], port)
        srv.start()
        try:
            smoke_full_flow(f"http://127.0.0.1:{port}")
        finally:
            srv.stop()

        smoke_concurrency(dirs[1])
        smoke_expiry(dirs[2])
        smoke_crash_before_result(dirs[3])
        smoke_crash_after_result(dirs[4])
        smoke_corruption(dirs[4])  # corrupt the durable executed record
        return dirs
    finally:
        pass  # cleaned by caller


# ---------------------------------------------------------------------- #
# phase 4: remote (compose) smoke
# ---------------------------------------------------------------------- #
def phase_remote(base_url: str) -> None:
    section(f"PHASE 4: remote smoke against {base_url}")
    status, body = http("GET", f"{base_url}/healthz", timeout=15)
    report("remote healthz ok",
           status == 200 and body["result"]["status"] == "ok",
           f"{status} {body}")

    token = f"RM-{int(time.time()*1000)}"
    exp = time.time() + 60
    status, body = http("POST", f"{base_url}/v1/choices",
                        {"device_id": f"DEV-{token}", "op_id": f"OP-{token}",
                         "summary": "CMD:arm", "expires_at": exp})
    report("remote create choice", status == 201, f"{status} {body}")
    status, body = http("POST", f"{base_url}/v1/executions",
                        {"device_id": f"DEV-{token}", "op_id": f"OP-{token}",
                         "summary": "CMD:arm"})
    report("remote execute accepted", status == 200
           and body["verdict"] == "EXEC_ACCEPTED", f"{status} {body}")
    receipt = body["result"]["outcome"]["receipt"]
    status, body = http("POST", f"{base_url}/v1/executions",
                        {"device_id": f"DEV-{token}", "op_id": f"OP-{token}",
                         "summary": "CMD:arm"})
    report("remote execute retx replays",
           status == 200 and body["result"]["outcome"]["receipt"] == receipt,
           f"{status} {body}")


def main() -> int:
    print(f"acceptance root={ROOT} python={sys.version.split()[0]}")
    phase_code()

    dirs: list[str] = []
    try:
        dirs = phase_http()
        base_url = os.environ.get("BASE_URL")
        if base_url:
            phase_remote(base_url.rstrip("/"))
        else:
            section("PHASE 4: BASE_URL not set, skipping remote smoke")
    finally:
        for d in dirs:
            shutil.rmtree(d, ignore_errors=True)

    section("RESULT")
    if failures:
        print(f"{len(failures)} CHECK(S) FAILED:")
        for name in failures:
            print(f"  - {name}")
        return 1
    print("ALL ACCEPTANCE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
