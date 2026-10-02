"""Consume an installed native wheel against the isolated MySQL/Doris databases.

Run with D:\\PythonVenv\\Scripts\\python.exe after installing the release wheel.
Only uniquely named py_native_* tables in dunnelean_test are changed. This script
does not start an HTTP service, initialize accounts, or manage containers.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import importlib.metadata
import json
import os
import socket
import tempfile
import threading
import time
import uuid
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import dunnelean
import pymysql
import requests
from dunnelean import AsyncEngine, Engine


ROOT = Path(__file__).resolve().parents[1]
REPORT = []


class TestDatabase:
    def __init__(self, password):
        self.password = password
        prefix = "py_native_" + uuid.uuid4().hex[:10]
        self.source = prefix + "_source"
        self.returned = prefix + "_returned"
        self.unknown = prefix + "_unknown"
        self.target = prefix + "_target"
        self.cancel_target = prefix + "_cancel"
        self.mysql_tables = (self.source, self.returned, self.unknown)
        self.doris_tables = (self.target, self.cancel_target)
        self.created = []

    def connect(self, doris=False):
        connection = pymysql.connect(
            host="127.0.0.1", port=9030 if doris else 3308,
            user="dunnelean", password=self.password, database="dunnelean_test",
            charset="utf8mb4", autocommit=True, connect_timeout=10, read_timeout=30,
        )
        with connection.cursor() as cursor:
            cursor.execute("SET time_zone='+00:00'")
            if doris:
                cursor.execute("SET enable_decimal256=true")
        return connection

    def sql(self, statement, args=None, doris=False):
        with self.connect(doris) as connection, connection.cursor() as cursor:
            cursor.execute(statement, args)
            return cursor.fetchall() if cursor.description else cursor.rowcount

    def prepare(self):
        # Preflight both business accounts before creating any fixtures.
        assert self.sql("SELECT 1") == ((1,),)
        assert self.sql("SELECT 1", doris=True) == ((1,),)
        self.sql(f"""CREATE TABLE `{self.source}` (
            id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            amount DECIMAL(30,8), precise_value DECIMAL(65,5),
            Name VARCHAR(512), event_time DATETIME(6), event_instant TIMESTAMP(6) NULL,
            enabled TINYINT, notes TEXT
        ) ENGINE=InnoDB""")
        self.created.append((False, self.source))
        for table in (self.returned, self.unknown):
            self.sql(f"CREATE TABLE `{table}` LIKE `{self.source}`")
            self.created.append((False, table))
        for table in self.doris_tables:
            self.sql(f"""CREATE TABLE `{table}` (
                id DECIMAL(20,0) NOT NULL, amount DECIMAL(30,10), precise_value DECIMAL(65,5),
                Name VARCHAR(512), event_time DATETIME(6), event_instant DATETIME(6),
                enabled TINYINT, notes STRING
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")""", doris=True)
            self.created.append((True, table))
        rows = [
            (
                2**64 - 1 if index == 46 else index,
                Decimal("-12345678901234567890.12345678") if index % 3 else None,
                Decimal("123456789012345678901234567890123456789012345678901234567890.12345"),
                None if index % 4 == 0 else '中文🚀\n"quote"\\' + str(index),
                "2026-09-29 12:34:56.123456", "2026-09-29 04:34:56.654321", index % 2,
                "" if index % 2 else "line1\nline2\tNULL\\N",
            )
            for index in range(47)
        ]
        with self.connect() as connection, connection.cursor() as cursor:
            cursor.executemany(f"INSERT INTO `{self.source}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", rows)

    def cleanup(self):
        errors = []
        for doris, table in reversed(self.created):
            try:
                self.sql(f"DROP TABLE IF EXISTS `{table}`", doris=doris)
            except Exception as error:
                errors.append(f"{table}: {type(error).__name__}: {error}")
        if errors:
            raise RuntimeError("Could not remove isolated Python fixtures: " + "; ".join(errors))

    def spec(self, direction):
        spec = json.loads((ROOT / "examples" / f"{direction}.json").read_text(encoding="utf-8"))
        spec["request_id"] = str(uuid.uuid4())
        spec["reader"]["batch"] = {"rows": 7, "bytes": 65536}
        spec["writer"]["options"] = {"batch": {"rows": 5, "bytes": 65536}}
        if direction == "mysql-to-doris":
            spec["reader"]["source"]["table"] = self.source
            spec["writer"]["table"] = self.target
        else:
            spec["reader"]["source"]["table"] = self.target
            spec["writer"]["table"] = self.returned
        return spec


class DelayedReceipt:
    """Forward a load to the real BE, then hold or discard its commit receipt."""

    def __init__(self, drop=False):
        self.drop = drop
        self.requests = 0
        self.errors = []
        self.committed = threading.Event()
        self.release = threading.Event()

    def __enter__(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def do_GET(self):
                self.send_body(200, b'{"status":"OK"}')

            def send_body(self, status, body):
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

            def do_PUT(self):
                owner.requests += 1
                try:
                    body = self.rfile.read(int(self.headers["Content-Length"]))
                    headers = {
                        key: value for key, value in self.headers.items()
                        if key.lower() not in {"host", "connection", "expect", "content-length"}
                    }
                    with requests.Session() as session:
                        session.trust_env = False
                        response = session.put(
                            "http://127.0.0.1:8040" + self.path,
                            headers=headers, data=body, timeout=30,
                        )
                    assert response.status_code == 200, response.status_code
                    assert response.json()["Status"] == "Success", response.text
                    owner.committed.set()
                    if owner.drop:
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                    assert owner.release.wait(30), "Commit receipt was not released"
                    self.send_body(response.status_code, response.content)
                except Exception as error:
                    owner.errors.append(str(error))
                    try:
                        self.send_body(502, b"{}")
                    except OSError:
                        pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class CommitResponseLossProxy:
    """Drop exactly the real MySQL server reply to COM_QUERY COMMIT."""

    def __enter__(self):
        self.commits = 0
        self.closed = threading.Event()
        self.lock = threading.Lock()
        self.connections = []
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()
        return self

    @staticmethod
    def packet(connection):
        def exact(size):
            result = b""
            while len(result) < size:
                part = connection.recv(size - len(result))
                if not part:
                    raise EOFError
                result += part
            return result
        header = exact(4)
        return header + exact(int.from_bytes(header[:3], "little"))

    def relay(self, source, destination, client_side, drop, client, upstream):
        try:
            while not self.closed.is_set():
                packet = self.packet(source)
                if client_side and packet[4:5] == b"\x03" and packet[5:].strip().upper() == b"COMMIT":
                    with self.lock:
                        self.commits += 1
                    drop.set()
                if not client_side and drop.is_set():
                    break
                destination.sendall(packet)
        except (OSError, EOFError):
            pass
        finally:
            self.close_connections((client, upstream))

    @staticmethod
    def close_connections(connections):
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()

    def accept(self):
        while not self.closed.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                upstream = socket.create_connection(("127.0.0.1", 3308), timeout=10)
                upstream.settimeout(None)
            except OSError:
                client.close()
                continue
            with self.lock:
                self.connections.extend((client, upstream))
            drop = threading.Event()
            for source, destination, client_side in ((client, upstream, True), (upstream, client, False)):
                threading.Thread(
                    target=self.relay,
                    args=(source, destination, client_side, drop, client, upstream), daemon=True,
                ).start()

    def __exit__(self, *_):
        self.closed.set()
        self.listener.close()
        self.thread.join(timeout=5)
        with self.lock:
            self.close_connections(self.connections)


def record(case, run, started, **detail):
    result = {
        "case": case, "state": run["state"], "rows_committed": run["rows_committed"],
        "commit_unknown": run["commit_unknown"], "run_id": run["run_id"],
        "seconds": round(time.monotonic() - started, 3), **detail,
    }
    REPORT.append(result)
    print(json.dumps(result, ensure_ascii=False), flush=True)


def synchronous_cases(database, state_path):
    with Engine(state_path) as engine:
        forward = database.spec("mysql-to-doris")
        assert engine.validate(forward)["valid"] is True
        started = time.monotonic()
        run = engine.run(forward, timeout=120, poll_interval=0.02)
        assert run["state"] == "SUCCEEDED", run
        assert run["rows_committed"] == 47 and run["batches_committed"] > 1, run
        assert database.password not in json.dumps(run)
        assert engine.submit(json.dumps(forward))["run_id"] == run["run_id"]
        record("installed_wheel_sync_mysql_to_doris_exact_types", run, started)

        with DelayedReceipt() as proxy:
            racing = database.spec("mysql-to-doris")
            racing["reader"]["source"]["where"] = "id < 2"
            racing["writer"]["table"] = database.cancel_target
            racing["writer"]["be_http_urls"] = [proxy.url]
            started = time.monotonic()
            run = engine.submit(racing)
            assert proxy.committed.wait(30), proxy.errors
            assert engine.cancel_run(run["run_id"])["state"] == "CANCELLING"
            assert engine.get_run(run["run_id"])["state"] == "CANCELLING"
            proxy.release.set()
            run = engine.wait_for_completion(run["run_id"], timeout=60, poll_interval=0.02)
            assert run["state"] == "CANCELLED", run
            assert run["rows_committed"] == 2 and run["partial_write"] and not run["commit_unknown"], run
            assert proxy.requests == 1 and not proxy.errors, proxy.errors
            assert database.sql(f"SELECT COUNT(*) FROM `{database.cancel_target}`", doris=True) == ((2,),)
            record("cancel_waits_for_real_doris_commit_receipt", run, started, puts=proxy.requests)

        with CommitResponseLossProxy() as proxy:
            unknown = database.spec("doris-to-mysql")
            unknown["reader"]["source"]["where"] = "id < 2"
            unknown["writer"]["table"] = database.unknown
            unknown["writer"]["connection"]["port"] = proxy.port
            started = time.monotonic()
            run = engine.run(unknown, timeout=120, poll_interval=0.02)
            assert run["state"] == "FAILED" and run["commit_unknown"], run
            assert run["rows_committed"] == 0 and run["batches_committed"] == 0, run
            assert proxy.commits == 1, proxy.commits
            assert database.sql(f"SELECT COUNT(*) FROM `{database.unknown}`") == ((2,),)
            assert engine.submit(unknown)["run_id"] == run["run_id"]
            assert proxy.commits == 1
            batches = engine.list_batches(run["run_id"])
            assert len(batches) == 1 and batches[0]["state"] in {"INTENT", "UNKNOWN"}, batches
            record("mysql_commit_response_loss_is_not_replayed", run, started, commits=proxy.commits)


async def asynchronous_cases(database, state_path):
    async with AsyncEngine(state_path) as engine:
        reverse = database.spec("doris-to-mysql")
        assert (await engine.validate(reverse))["valid"] is True
        started = time.monotonic()
        run = await engine.run(reverse, timeout=120, poll_interval=0.02)
        assert run["state"] == "SUCCEEDED" and run["rows_committed"] == 47, run
        assert database.sql(f"SELECT * FROM `{database.source}` ORDER BY id") == database.sql(f"SELECT * FROM `{database.returned}` ORDER BY id"), "Exact native-wheel roundtrip mismatch"
        assert (await engine.submit(reverse))["run_id"] == run["run_id"]
        record("installed_wheel_async_doris_to_mysql_exact_roundtrip", run, started)

        slow = database.spec("mysql-to-doris")
        slow["reader"]["source"]["where"] = "id < 2"
        slow["writer"]["table"] = database.cancel_target
        slow["execution"] = {"rows_per_second": 1}
        started = time.monotonic()
        run = await engine.submit(slow)
        waiter = asyncio.create_task(engine.wait_for_completion(run["run_id"], timeout=60, poll_interval=0.02))
        await asyncio.sleep(0.05)
        waiter.cancel()
        try:
            await waiter
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Cancelling a Python waiter must raise CancelledError")
        assert (await engine.get_run(run["run_id"]))["state"] in {"QUEUED", "RUNNING"}
        await engine.cancel_run(run["run_id"])
        run = await engine.wait_for_completion(run["run_id"], timeout=60, poll_interval=0.02)
        assert run["state"] == "CANCELLED" and not run["commit_unknown"], run
        record("async_waiter_cancellation_requires_explicit_run_cancel", run, started)


def load_password():
    password = os.environ.get("DUNNELEAN_TEST_PASSWORD")
    if password is None:
        env_path = ROOT / "deploy/.env"
        if env_path.is_file():
            settings = dict(
                line.split("=", 1) for line in env_path.read_text(encoding="utf-8").splitlines()
                if line and not line.startswith("#") and "=" in line
            )
            password = settings.get("DUNNELEAN_TEST_PASSWORD")
    if password is None:
        raise RuntimeError("Set DUNNELEAN_TEST_PASSWORD or prepare the isolated deploy/.env first")
    os.environ["DUNNELEAN_TEST_PASSWORD"] = password
    return password


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=ROOT / "var/acceptance-python.json")
    args = parser.parse_args()
    package_path = Path(dunnelean.__file__).resolve()
    assert ROOT / "sdk/python/python" not in package_path.parents, "Install the release wheel first"
    assert importlib.metadata.version("dunnelean") == dunnelean.__version__
    database = TestDatabase(load_password())
    try:
        database.prepare()
        with tempfile.TemporaryDirectory(prefix="dunnelean-python-acceptance-") as temporary:
            state_path = Path(temporary) / "state.sqlite"
            synchronous_cases(database, state_path)
            asyncio.run(asynchronous_cases(database, state_path))
    finally:
        database.cleanup()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps({
        "package_version": dunnelean.__version__, "installed_package": str(package_path),
        "python": os.sys.version, "cases": REPORT,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Installed native-wheel database acceptance passed: {args.report}", flush=True)


if __name__ == "__main__":
    main()
