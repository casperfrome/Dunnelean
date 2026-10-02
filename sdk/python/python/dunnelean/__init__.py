"""Embedded Arrow-native MySQL ↔ Doris batch synchronization."""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypeAlias

from . import _native

__version__: str = _native.__version__
Spec: TypeAlias = Mapping[str, Any] | str
Result: TypeAlias = dict[str, Any]
_TERMINAL = frozenset({"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"})

__all__ = [
    "Engine", "AsyncEngine", "DunneleanError", "WaitTimeoutError",
    "CloseTimeoutError", "connectors", "request_schema", "__version__",
]


class DunneleanError(Exception):
    """A core error; retryable does not authorize replaying a whole job."""

    def __init__(self, code: str, message: str, *, commit_unknown: bool = False,
                 retryable: bool = False) -> None:
        self.code = code
        self.message = message
        self.commit_unknown = commit_unknown
        self.retryable = retryable
        super().__init__(f"{code}: {message}")


class WaitTimeoutError(DunneleanError, TimeoutError):
    """Only the local wait timed out; the run may still be executing."""

    def __init__(self, run_id: str, timeout: float) -> None:
        self.run_id = run_id
        self.timeout = timeout
        super().__init__("WAIT_TIMEOUT", f"Run {run_id} did not finish within {timeout:g} seconds")


class CloseTimeoutError(DunneleanError, TimeoutError):
    """Resources remain owned; query/cancel and retry close to finish cleanup."""

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        super().__init__("CLOSE_TIMEOUT", f"Engine did not finish closing within {timeout:g} seconds")


def _invoke(call: Any, *args: Any) -> Any:
    try:
        return call(*args)
    except _native.NativeError as exc:
        error = json.loads(exc.args[0])
        raise DunneleanError(
            error["code"], error["message"],
            commit_unknown=error.get("commit_unknown", False),
            retryable=error.get("retryable", False),
        ) from exc


def _seconds(value: float, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a finite number of seconds")
    number = float(value)
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        raise ValueError(f"{name} must be {'positive' if positive else 'nonnegative'} and finite")
    # All deadlines and native millisecond timeouts must be representable.
    if number > (2**63 - 1) / 1000:
        raise ValueError(f"{name} is too large")
    return number


def _spec_json(spec: Spec) -> str:
    if isinstance(spec, str):
        return spec
    if not isinstance(spec, Mapping):
        raise TypeError("spec must be a mapping or a JSON string")
    return json.dumps(dict(spec), ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def connectors() -> Result:
    """Return connector capabilities, limits and the core's request schema."""
    return json.loads(_invoke(_native.connectors))


def request_schema() -> Result:
    """Return the JSON Schema generated from Rust RunSpec."""
    return connectors()["request_schema"]


class Engine:
    """A persistent local engine. Use a unique SQLite path and close explicitly."""

    def __init__(self, state_path: str | os.PathLike[str], *, max_running: int = 2,
                 max_queued: int = 16, shutdown_timeout: float = 60.0) -> None:
        self._shutdown_timeout = _seconds(shutdown_timeout, "shutdown_timeout")
        for name, value in (("max_running", max_running), ("max_queued", max_queued)):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an integer")
        if not 1 <= max_running <= 64 or not 0 <= max_queued <= 4096:
            raise DunneleanError("INVALID_CONFIG", "max_running must be 1..64 and max_queued must be 0..4096")
        path = os.fspath(state_path)
        if not isinstance(path, str) or not path or path == ":memory:":
            raise ValueError("state_path must name a persistent SQLite file")
        self._native = _invoke(_native.NativeEngine, str(Path(path).resolve()), max_running, max_queued)
        try:
            self._state_store_id = _invoke(self._native.state_store_id)
        except BaseException:
            self._abandon()
            raise

    def _abandon(self) -> None:
        try:
            _invoke(self._native.abandon)
        except DunneleanError:
            # Keep the original exception. Rust retains safe ownership if it
            # cannot start its cleanup thread; finalization remains a fallback.
            pass

    @property
    def state_store_id(self) -> str:
        """The identity of this state database, stable across reopening."""
        return self._state_store_id

    def __enter__(self) -> Engine:
        try:
            self.ready()
        except BaseException:
            self._abandon()
            raise
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def ready(self) -> None:
        """Check engine admission and the state database; does not probe databases."""
        _invoke(self._native.ready)

    def validate(self, spec: Spec) -> Result:
        """Validate configuration, live database connections and schema compatibility."""
        return json.loads(_invoke(self._native.validate, _spec_json(spec)))

    def submit(self, spec: Spec) -> Result:
        """Submit without waiting. Stable request_id values prevent duplicate runs."""
        return json.loads(_invoke(self._native.submit, _spec_json(spec)))

    def get_run(self, run_id: str) -> Result:
        return json.loads(_invoke(self._native.get_run, run_id))

    def cancel_run(self, run_id: str) -> Result:
        """Request cancellation; in-flight commits still need confirmation."""
        return json.loads(_invoke(self._native.cancel_run, run_id))

    def list_runs(self, limit: int = 50, offset: int = 0) -> list[Result]:
        if (isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500
                or isinstance(offset, bool) or not isinstance(offset, int) or offset < 0):
            raise ValueError("limit must be an integer in 1..500 and offset a nonnegative integer")
        return json.loads(_invoke(self._native.list_runs, limit, offset))

    def list_batches(self, run_id: str) -> list[Result]:
        return json.loads(_invoke(self._native.list_batches, run_id))

    def get_request(self, request_id: str) -> Result:
        return json.loads(_invoke(self._native.get_request, request_id))

    def cancel_request(self, request_id: str) -> Result:
        """Persist cancellation even if the request has not been submitted yet."""
        return json.loads(_invoke(self._native.cancel_request, request_id))

    def wait_for_completion(self, run_id: str, *, timeout: float = 300.0,
                            poll_interval: float = 0.5) -> Result:
        timeout = _seconds(timeout, "timeout")
        poll_interval = _seconds(poll_interval, "poll_interval", positive=True)
        deadline = time.monotonic() + timeout
        while True:
            run = self.get_run(run_id)
            if run["state"] in _TERMINAL:
                return run
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WaitTimeoutError(run_id, timeout)
            time.sleep(min(poll_interval, remaining))

    def run(self, spec: Spec, *, timeout: float = 300.0,
            poll_interval: float = 0.5) -> Result:
        _seconds(timeout, "timeout")
        _seconds(poll_interval, "poll_interval", positive=True)
        return self.wait_for_completion(self.submit(spec)["run_id"], timeout=timeout,
                                        poll_interval=poll_interval)

    def close(self, *, timeout: float | None = None) -> None:
        """Cancel, drain and release resources; retry after CloseTimeoutError."""
        seconds = self._shutdown_timeout if timeout is None else _seconds(timeout, "timeout")
        if not _invoke(self._native.close, math.ceil(seconds * 1000)):
            raise CloseTimeoutError(seconds)


class AsyncEngine:
    """asyncio facade over the same native engine; opening also runs off the loop."""

    def __init__(self, state_path: str | os.PathLike[str], *, max_running: int = 2,
                 max_queued: int = 16, shutdown_timeout: float = 60.0) -> None:
        self._options = (state_path, max_running, max_queued, shutdown_timeout)
        self._engine: Engine | None = None
        self._opening: asyncio.Task[Engine] | None = None
        self._abandoned = False

    @classmethod
    async def open(cls, state_path: str | os.PathLike[str], *, max_running: int = 2,
                   max_queued: int = 16, shutdown_timeout: float = 60.0) -> AsyncEngine:
        instance = cls(state_path, max_running=max_running, max_queued=max_queued,
                       shutdown_timeout=shutdown_timeout)
        await instance._start()
        return instance

    async def _start(self) -> None:
        if self._abandoned:
            raise DunneleanError("ENGINE_CLOSED", "Async engine opening was cancelled or engine was closed")
        if self._engine is not None:
            return
        if self._opening is None:
            path, running, queued, shutdown = self._options
            self._opening = asyncio.create_task(asyncio.to_thread(
                Engine, path, max_running=running, max_queued=queued, shutdown_timeout=shutdown))
        task = self._opening
        try:
            engine = await asyncio.shield(task)
        except asyncio.CancelledError:
            self._abandoned = True
            # A retained cancellation traceback can keep Task.result alive.
            # Close that result explicitly through Rust background cleanup.
            def release(done: asyncio.Task[Engine]) -> None:
                self._opening = None
                if done.cancelled():
                    return
                try:
                    opened = done.result()
                except BaseException:
                    return
                self._engine = opened
                opened._abandon()
            task.add_done_callback(release)
            raise
        except BaseException:
            self._opening = None
            raise
        if self._abandoned:
            self._opening = None
            engine._abandon()
            raise DunneleanError("ENGINE_CLOSED", "Async engine opening was cancelled")
        self._engine = engine
        self._opening = None

    def _get_engine(self) -> Engine:
        if self._engine is None:
            raise DunneleanError("ENGINE_NOT_OPEN", "Use async with AsyncEngine(...) or await AsyncEngine.open(...)")
        return self._engine

    @property
    def state_store_id(self) -> str:
        return self._get_engine().state_store_id

    async def __aenter__(self) -> AsyncEngine:
        try:
            await self._start()
            await self.ready()
        except BaseException:
            self._abandoned = True
            if self._engine is not None:
                self._engine._abandon()
            raise
        return self

    async def __aexit__(self, *exc: Any) -> None:
        try:
            await self.close()
        except asyncio.CancelledError:
            self._get_engine()._abandon()
            raise

    async def ready(self) -> None:
        await asyncio.to_thread(self._get_engine().ready)

    async def validate(self, spec: Spec) -> Result:
        # Freeze mutable input before scheduling onto a worker thread.
        return await asyncio.to_thread(self._get_engine().validate, _spec_json(spec))

    async def submit(self, spec: Spec) -> Result:
        return await asyncio.to_thread(self._get_engine().submit, _spec_json(spec))

    async def get_run(self, run_id: str) -> Result:
        return await asyncio.to_thread(self._get_engine().get_run, run_id)

    async def cancel_run(self, run_id: str) -> Result:
        return await asyncio.to_thread(self._get_engine().cancel_run, run_id)

    async def list_runs(self, limit: int = 50, offset: int = 0) -> list[Result]:
        return await asyncio.to_thread(self._get_engine().list_runs, limit, offset)

    async def list_batches(self, run_id: str) -> list[Result]:
        return await asyncio.to_thread(self._get_engine().list_batches, run_id)

    async def get_request(self, request_id: str) -> Result:
        return await asyncio.to_thread(self._get_engine().get_request, request_id)

    async def cancel_request(self, request_id: str) -> Result:
        return await asyncio.to_thread(self._get_engine().cancel_request, request_id)

    async def wait_for_completion(self, run_id: str, *, timeout: float = 300.0,
                                  poll_interval: float = 0.5) -> Result:
        timeout = _seconds(timeout, "timeout")
        poll_interval = _seconds(poll_interval, "poll_interval", positive=True)
        deadline = time.monotonic() + timeout
        while True:
            run = await self.get_run(run_id)
            if run["state"] in _TERMINAL:
                return run
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WaitTimeoutError(run_id, timeout)
            await asyncio.sleep(min(poll_interval, remaining))

    async def run(self, spec: Spec, *, timeout: float = 300.0,
                  poll_interval: float = 0.5) -> Result:
        _seconds(timeout, "timeout")
        _seconds(poll_interval, "poll_interval", positive=True)
        return await self.wait_for_completion((await self.submit(spec))["run_id"],
                                              timeout=timeout, poll_interval=poll_interval)

    async def close(self, *, timeout: float | None = None) -> None:
        if self._engine is None:
            if self._opening is None:
                self._abandoned = True
                return
            self._abandoned = True
            self._engine = await asyncio.shield(self._opening)
            self._opening = None
        await asyncio.to_thread(self._get_engine().close, timeout=timeout)
