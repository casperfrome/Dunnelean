"""Fixtures for testing an installed wheel; never add Python sources to sys.path."""
from __future__ import annotations

import json
import os
import socket
import threading
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def repo_root() -> Path:
    root = Path(os.environ.get("DUNNELEAN_REPO_ROOT", Path(__file__).resolve().parents[3]))
    assert (root / "examples/mysql-to-doris.json").is_file(), root
    return root.resolve()


@pytest.fixture
def job(repo_root):
    spec = json.loads((repo_root / "examples/mysql-to-doris.json").read_text(encoding="utf-8"))
    # Unit tests never connect to the real database ports or depend on deploy/.env.
    for connection in (spec["reader"]["connection"], spec["writer"]["sql"]):
        connection["credentials"] = {"username": "unit-test", "password": "unit-secret"}
        connection["timeouts"] = {"connect_ms": 5000, "read_ms": 5000, "write_ms": 5000}
    spec["request_id"] = "python-unit-request"
    return spec


class PendingHandshake:
    """Accept MySQL TCP connections without sending a greeting until released."""

    def __enter__(self):
        self.accepted = threading.Event()
        self.stopped = threading.Event()
        self.connections = []
        self.lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._accept, daemon=True)
        self.thread.start()
        return self

    def _accept(self):
        while not self.stopped.is_set():
            try:
                connection, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self.lock:
                self.connections.append(connection)
            self.accepted.set()

    def release(self):
        self.stopped.set()
        self.listener.close()
        self.thread.join(timeout=2)
        with self.lock:
            for connection in self.connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()
            self.connections.clear()

    def __exit__(self, *_):
        self.release()


@pytest.fixture
def stalled_job(job):
    with PendingHandshake() as peer:
        job["reader"]["connection"]["port"] = peer.port
        job["writer"]["sql"]["port"] = peer.port
        yield job, peer
