"""Verify an embedded Go package against the isolated MySQL/Doris databases.

Use D:\\PythonVenv\\Scripts\\python.exe. The Go driver runs outside the checkout;
its Rust engine lives in the same Go process. Python only prepares/checks tables
and holds or drops real database receipts. No Dunnelean HTTP service is used.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import queue
import subprocess
import tempfile
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE = "github.com/casperfrome/Dunnelean/sdk/go"
REPORT: list[dict] = []

DRIVER = r'''
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "sync"
    "sync/atomic"
    "time"

    dunnelean "github.com/casperfrome/Dunnelean/sdk/go"
)

type command struct {
    ID int `json:"id"`
    Operation string `json:"operation"`
    Spec json.RawMessage `json:"spec"`
    RunID string `json:"run_id"`
    RequestID string `json:"request_id"`
    Target int `json:"target"`
    TimeoutMS int `json:"timeout_ms"`
}

func main() {
    engine, err := dunnelean.Open(context.Background(), os.Args[1],
        dunnelean.WithNativeCacheDir(os.Args[2]))
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    defer engine.Close(context.Background())
    var ticks atomic.Uint64
    ticker := time.NewTicker(time.Millisecond)
    defer ticker.Stop()
    go func() { for range ticker.C { ticks.Add(1) } }()
    var output sync.Mutex
    encoder := json.NewEncoder(os.Stdout)
    emit := func(id int, value any, err error) {
        response := map[string]any{"id": id, "value": value, "ticks": ticks.Load()}
        if err != nil { response["error"] = err.Error() }
        output.Lock()
        defer output.Unlock()
        if err := encoder.Encode(response); err != nil { fmt.Fprintln(os.Stderr, err) }
    }
    emit(0, map[string]any{"version": dunnelean.Version, "state_store_id": engine.StateStoreID(), "pid": os.Getpid()}, nil)
    var calls sync.Map
    var workers sync.WaitGroup
    decoder := json.NewDecoder(os.Stdin)
    for {
        var input command
        if err := decoder.Decode(&input); err != nil { break }
        if input.Operation == "cancel_call" {
            if cancel, ok := calls.Load(input.Target); ok { cancel.(context.CancelFunc)() }
            emit(input.ID, true, nil)
            continue
        }
        duration := 180 * time.Second
        if input.TimeoutMS > 0 { duration = time.Duration(input.TimeoutMS) * time.Millisecond }
        ctx, cancel := context.WithTimeout(context.Background(), duration)
        calls.Store(input.ID, context.CancelFunc(cancel))
        workers.Add(1)
        go func(input command) {
            defer workers.Done()
            defer cancel()
            defer calls.Delete(input.ID)
            var value any
            var err error
            switch input.Operation {
            case "validate": value, err = engine.Validate(ctx, input.Spec)
            case "submit": value, err = engine.Submit(ctx, input.Spec)
            case "run": value, err = engine.Run(ctx, input.Spec, dunnelean.WithPollInterval(20*time.Millisecond))
            case "get": value, err = engine.GetRun(ctx, input.RunID)
            case "wait": value, err = engine.WaitForCompletion(ctx, input.RunID, dunnelean.WithPollInterval(20*time.Millisecond))
            case "cancel": value, err = engine.CancelRun(ctx, input.RunID)
            case "request": value, err = engine.GetRequest(ctx, input.RequestID)
            case "cancel_request": value, err = engine.CancelRequest(ctx, input.RequestID)
            case "batches": value, err = engine.ListBatches(ctx, input.RunID)
            case "ready": err = engine.Ready(ctx); value = true
            case "close": err = engine.Close(ctx); value = true
            default: err = fmt.Errorf("unknown operation %q", input.Operation)
            }
            emit(input.ID, value, err)
        }(input)
    }
    workers.Wait()
}
'''


def fixtures():
    spec = importlib.util.spec_from_file_location("dunnelean_database_fixtures", ROOT / "scripts/integration-python.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class GoClient:
    def __init__(self, binary: Path, state_path: Path, cache_path: Path, environment: dict):
        self.process = subprocess.Popen(
            [str(binary), str(state_path), str(cache_path)], cwd=binary.parent,
            env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, encoding="utf-8", bufsize=1,
        )
        self.responses: dict[int, queue.Queue] = {0: queue.Queue()}
        self.next_id = 0
        self.lock = threading.Lock()
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()
        self.opened = self.receive(0, 60)
        assert self.opened["value"]["version"] == "0.1.0", self.opened

    def read(self):
        for line in self.process.stdout:
            response = json.loads(line)
            with self.lock:
                destination = self.responses.get(response["id"])
            if destination is not None:
                destination.put(response)
        with self.lock:
            destinations = list(self.responses.values())
        for destination in destinations:
            destination.put({"error": "Go driver exited before responding"})

    def send(self, operation: str, **values) -> int:
        with self.lock:
            self.next_id += 1
            identity = self.next_id
            self.responses[identity] = queue.Queue()
            self.process.stdin.write(json.dumps({"id": identity, "operation": operation, **values}, ensure_ascii=False) + "\n")
            self.process.stdin.flush()
        return identity

    def receive(self, identity: int, timeout=200, *, allow_error=False):
        try:
            response = self.responses[identity].get(timeout=timeout)
        except queue.Empty as error:
            raise AssertionError(f"Go request {identity} timed out") from error
        if "error" in response and not allow_error:
            raise AssertionError(response)
        return response

    def call(self, operation: str, *, allow_error=False, **values):
        return self.receive(self.send(operation, **values), allow_error=allow_error)

    def stop(self):
        if self.process.poll() is None:
            try:
                self.call("close")
            finally:
                self.process.stdin.close()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
        diagnostic = self.process.stderr.read()
        if self.process.returncode:
            raise AssertionError(f"Go driver failed ({self.process.returncode}): {diagnostic}")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop()


def record(case: str, run: dict, started: float, **details):
    result = {
        "case": case, "state": run["state"], "rows_committed": run["rows_committed"],
        "commit_unknown": run["commit_unknown"], "run_id": run["run_id"],
        "seconds": round(time.monotonic() - started, 3), **details,
    }
    REPORT.append(result)
    print(json.dumps(result, ensure_ascii=False), flush=True)


def acceptance(client: GoClient, database, helpers):
    forward = database.spec("mysql-to-doris")
    assert client.call("validate", spec=forward)["value"]["valid"]
    started = time.monotonic()
    run = client.call("run", spec=forward)["value"]
    assert run["state"] == "SUCCEEDED" and run["rows_committed"] == 47, run
    assert run["batches_committed"] > 1 and database.password not in json.dumps(run), run
    assert client.call("submit", spec=forward)["value"]["run_id"] == run["run_id"]
    record("go_sync_mysql_to_doris_exact_types", run, started)

    reverse = database.spec("doris-to-mysql")
    reverse["writer"]["options"]["pre_sql"] = ["DO SLEEP(0.3)"]
    assert client.call("validate", spec=reverse)["value"]["valid"]
    started = time.monotonic()
    initial_ticks = client.call("ready")["ticks"]
    request = client.send("run", spec=reverse)
    time.sleep(0.08)
    heartbeat = client.call("ready")
    assert heartbeat["ticks"] > initial_ticks, heartbeat
    run = client.receive(request)["value"]
    assert run["state"] == "SUCCEEDED" and run["rows_committed"] == 47, run
    assert database.sql(f"SELECT * FROM `{database.source}` ORDER BY id") == database.sql(f"SELECT * FROM `{database.returned}` ORDER BY id"), "Exact embedded-Go roundtrip mismatch"
    assert client.call("submit", spec=reverse)["value"]["run_id"] == run["run_id"]
    record("go_goroutine_doris_to_mysql_exact_roundtrip", run, started, heartbeat_ticks=heartbeat["ticks"] - initial_ticks)

    with helpers.CommitResponseLossProxy() as proxy:
        unknown = database.spec("doris-to-mysql")
        unknown["reader"]["source"]["where"] = "id < 2"
        unknown["writer"]["table"] = database.unknown
        unknown["writer"]["connection"]["port"] = proxy.port
        started = time.monotonic()
        run = client.call("run", spec=unknown)["value"]
        assert run["state"] == "FAILED" and run["commit_unknown"], run
        assert run["rows_committed"] == 0 and run["batches_committed"] == 0, run
        assert proxy.commits == 1 and database.sql(f"SELECT COUNT(*) FROM `{database.unknown}`") == ((2,),)
        assert client.call("submit", spec=unknown)["value"]["run_id"] == run["run_id"]
        batches = client.call("batches", run_id=run["run_id"])["value"]
        assert len(batches) == 1 and batches[0]["state"] in {"INTENT", "UNKNOWN"}, batches
        assert proxy.commits == 1
        record("go_mysql_commit_response_loss_is_not_replayed", run, started, commits=proxy.commits)

    slow = database.spec("mysql-to-doris")
    slow["reader"]["source"]["where"] = "id < 2"
    slow["writer"]["table"] = database.cancel_target
    slow["execution"] = {"rows_per_second": 1}
    started = time.monotonic()
    run = client.call("submit", spec=slow)["value"]
    waiter = client.send("wait", run_id=run["run_id"])
    time.sleep(0.05)
    client.call("cancel_call", target=waiter)
    response = client.receive(waiter, allow_error=True)
    assert "context canceled" in response.get("error", ""), response
    assert client.call("get", run_id=run["run_id"])["value"]["state"] in {"QUEUED", "RUNNING"}
    client.call("cancel", run_id=run["run_id"])
    run = client.call("wait", run_id=run["run_id"])["value"]
    assert run["state"] == "CANCELLED" and not run["commit_unknown"], run
    record("go_context_cancellation_requires_explicit_run_cancel", run, started)


def close_receipt_case(client: GoClient, database, helpers):
    with helpers.DelayedReceipt() as proxy:
        racing = database.spec("mysql-to-doris")
        racing["reader"]["source"]["where"] = "id < 2"
        racing["writer"]["table"] = database.cancel_target
        racing["writer"]["be_http_urls"] = [proxy.url]
        started = time.monotonic()
        previous = database.sql(f"SELECT COUNT(*) FROM `{database.cancel_target}`", doris=True)[0][0]
        run = client.call("submit", spec=racing)["value"]
        assert proxy.committed.wait(30), proxy.errors
        assert client.call("cancel", run_id=run["run_id"])["value"]["state"] == "CANCELLING"
        close_result = client.call("close", timeout_ms=10, allow_error=True)
        assert "error" in close_result, close_result
        assert client.call("get", run_id=run["run_id"])["value"]["state"] == "CANCELLING"
        client.call("cancel", run_id=run["run_id"])
        proxy.release.set()
        run = client.call("wait", run_id=run["run_id"])["value"]
        assert run["state"] == "CANCELLED" and run["rows_committed"] == 2, run
        assert run["partial_write"] and not run["commit_unknown"], run
        assert proxy.requests == 1 and not proxy.errors, proxy.errors
        assert database.sql(f"SELECT COUNT(*) FROM `{database.cancel_target}`", doris=True) == ((previous + 2,),)
        client.call("close")
        record("go_close_timeout_retains_real_commit_receipt_and_retries", run, started, puts=proxy.requests)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", type=Path, required=True, help="Absolute path to a 64-bit Go executable")
    parser.add_argument("--sdk", type=Path, default=ROOT / "sdk/go")
    parser.add_argument("--module-version", help="Fetch this published version instead of using the local SDK")
    parser.add_argument("--report", type=Path, default=ROOT / "var/acceptance-go.json")
    args = parser.parse_args()
    helpers = fixtures()
    database = helpers.TestDatabase(helpers.load_password())
    for attribute in ("source", "returned", "unknown", "target", "cancel_target"):
        setattr(database, attribute, getattr(database, attribute).replace("py_native_", "go_native_", 1))
    database.mysql_tables = (database.source, database.returned, database.unknown)
    database.doris_tables = (database.target, database.cancel_target)
    environment = dict(os.environ, CGO_ENABLED="0", GOTOOLCHAIN="local")
    environment["GOROOT"] = str(args.go.resolve().parent.parent)
    version = subprocess.check_output([str(args.go), "version"], env=environment, text=True, encoding="utf-8").strip()
    assert "windows/amd64" in version or "linux/amd64" in version or "darwin/" in version, version
    with tempfile.TemporaryDirectory(prefix="dunnelean-go-acceptance-") as temporary:
        working = Path(temporary)
        go_mod = f"module dunnelean-go-acceptance\n\ngo 1.25.0\n\nrequire {MODULE} {args.module_version or 'v0.1.0'}\n"
        if args.module_version:
            environment["GOMODCACHE"] = str(working / "module-cache")
        else:
            go_mod += f"\nreplace {MODULE} => {args.sdk.resolve().as_posix()}\n"
        (working / "go.mod").write_text(go_mod, encoding="utf-8")
        (working / "main.go").write_text(DRIVER, encoding="utf-8")
        subprocess.run([str(args.go), "mod", "tidy"], cwd=working, env=environment, check=True)
        module_info = json.loads(subprocess.check_output([str(args.go), "list", "-m", "-json", MODULE], cwd=working, env=environment, text=True, encoding="utf-8"))
        environment["GOPROXY"] = "off"
        environment["GOSUMDB"] = "off"
        binary = working / ("go-acceptance.exe" if os.name == "nt" else "go-acceptance")
        subprocess.run([str(args.go), "build", "-o", str(binary), "."], cwd=working, env=environment, check=True)
        cache = working / "native-cache"
        state = working / "state.sqlite"
        try:
            database.prepare()
            with GoClient(binary, state, cache, environment) as client:
                identity = client.opened["value"]["state_store_id"]
                acceptance(client, database, helpers)
                close_receipt_case(client, database, helpers)
            with GoClient(binary, state, cache, environment) as reopened:
                assert reopened.opened["value"]["state_store_id"] == identity
                assert reopened.call("ready")["value"] is True
        finally:
            database.cleanup()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps({
        "package_version": "0.1.0", "go": version, "cgo_enabled": False,
        "module": module_info, "cases": REPORT,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Embedded Go database acceptance passed: {args.report}", flush=True)


if __name__ == "__main__":
    main()
