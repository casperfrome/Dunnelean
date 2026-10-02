from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import subprocess
import sys
import threading
import time
from collections import UserDict
from pathlib import Path

import dunnelean
import pytest
from dunnelean import AsyncEngine, CloseTimeoutError, DunneleanError, Engine, WaitTimeoutError


def capture_failure(engine, spec, failures):
    try:
        engine.validate(spec)
    except DunneleanError as error:
        failures.append(error)


def test_context_lifecycle_store_lock_and_identity(tmp_path):
    path = tmp_path / "state.sqlite"
    with Engine(path) as engine:
        identity = engine.state_store_id
        assert identity
        assert engine.ready() is None
        assert engine.list_runs() == []
        with pytest.raises(DunneleanError) as error:
            Engine(path)
        assert error.value.code == "STATE_STORE"
        # A second interpreter must observe the same OS-level exclusive lock.
        installed_root = Path(dunnelean.__file__).resolve().parent.parent
        consumer = (
            "import sys; sys.path.insert(0, sys.argv[1]); "
            "from dunnelean import Engine,DunneleanError; "
            "\ntry: Engine(sys.argv[2])"
            "\nexcept DunneleanError as e: assert e.code == 'STATE_STORE'"
            "\nelse: raise AssertionError('state lock not held')"
        )
        subprocess.run([sys.executable, "-I", "-c", consumer, str(installed_root), str(path)], check=True, timeout=20)
    engine.close()
    with pytest.raises(DunneleanError) as error:
        engine.list_runs()
    assert error.value.code == "ENGINE_CLOSED"
    with Engine(path) as reopened:
        assert reopened.state_store_id == identity


@pytest.mark.parametrize("fixture", ["all-fields-mysql-to-doris.json", "all-fields-doris-to-mysql.json"])
@pytest.mark.parametrize("as_json", [False, True])
def test_full_rust_contract_fixtures_accept_mapping_and_json(repo_root, tmp_path, monkeypatch, fixture, as_json):
    monkeypatch.setenv("JAVA_SDK_TEST_PASSWORD", "python-contract-secret")
    spec = json.loads((repo_root / "sdk/java/src/test/resources" / fixture).read_text(encoding="utf-8"))
    with Engine(tmp_path / "state.sqlite") as engine:
        engine.cancel_request(spec["request_id"])
        with pytest.raises(DunneleanError) as error:
            engine.submit(json.dumps(spec) if as_json else UserDict(spec))
        # A persisted cancellation avoids network traffic but still exercises all Rust config fields.
        assert error.value.code == "REQUEST_CANCELLED"


@pytest.mark.parametrize("change", [
    lambda spec: spec["reader"].update(fetch_size=5),
    lambda spec: spec["reader"]["source"].update(query="DELETE FROM source_orders"),
    lambda spec: spec["writer"].update(headers={"FORMAT": "csv"}),
    lambda spec: spec.update(execution={"rows_per_second": 0}),
])
def test_invalid_configuration_fails_before_database_access(tmp_path, job, change):
    change(job)
    with Engine(tmp_path / "state.sqlite") as engine:
        with pytest.raises(DunneleanError) as error:
            engine.submit(job)
        assert error.value.code in {"INVALID_CONFIG", "JSON"}
        assert error.value.message
        assert error.value.commit_unknown is False
        assert error.value.retryable is False
        assert engine.list_runs() == []


@pytest.mark.parametrize("text", ['{"reader":', "[]", "null"])
def test_invalid_json_is_a_structured_error(tmp_path, text):
    with Engine(tmp_path / "state.sqlite") as engine:
        with pytest.raises(DunneleanError) as error:
            engine.submit(text)
        assert error.value.code in {"INVALID_CONFIG", "JSON"}


def test_cancellation_tombstone_survives_reopen(tmp_path, job):
    path = tmp_path / "state.sqlite"
    with Engine(path) as engine:
        assert engine.cancel_request(job["request_id"]) == {
            "request_id": job["request_id"], "cancel_requested": True, "run": None,
        }
    with Engine(path) as engine:
        assert engine.get_request(job["request_id"])["cancel_requested"] is True
        with pytest.raises(DunneleanError) as error:
            engine.submit(job)
        assert error.value.code == "REQUEST_CANCELLED"


def test_idempotency_integer_precision_redaction_and_explicit_cancel(tmp_path, stalled_job):
    job, peer = stalled_job
    job["execution"] = {"rows_per_second": 2**64 - 1}
    job["reader"]["split"] = {
        "column": "id", "lower_bound": "0", "upper_bound": str(2**64 - 1),
        "partitions": 2, "parallelism": 1,
    }
    with Engine(tmp_path / "state.sqlite") as engine:
        run = engine.submit(UserDict(job))
        assert engine.submit(json.dumps(job))["run_id"] == run["run_id"]
        assert run["config"]["execution"]["rows_per_second"] == 2**64 - 1
        assert run["config"]["reader"]["split"]["upper_bound"] == str(2**64 - 1)
        assert "unit-secret" not in json.dumps(run)
        conflict = copy.deepcopy(job)
        conflict["writer"]["table"] = "different_target"
        with pytest.raises(DunneleanError) as error:
            engine.submit(conflict)
        assert error.value.code == "CONFLICT"
        engine.cancel_run(run["run_id"])
        terminal = engine.wait_for_completion(run["run_id"], timeout=5, poll_interval=0.01)
        assert terminal["state"] == "CANCELLED"
        assert terminal["rows_committed"] == 0
        assert terminal["commit_unknown"] is False
        assert engine.list_batches(run["run_id"]) == []
        assert engine.get_request(job["request_id"])["run"]["run_id"] == run["run_id"]
        assert [item["run_id"] for item in engine.list_runs(limit=1)] == [run["run_id"]]
        assert engine.list_runs(limit=1, offset=1) == []
        assert engine.submit(job)["state"] == "CANCELLED"


def test_wait_timeout_preserves_run_for_explicit_cancellation(tmp_path, stalled_job):
    job, peer = stalled_job
    with Engine(tmp_path / "state.sqlite") as engine:
        run = engine.submit(job)
        assert peer.accepted.wait(2)
        with pytest.raises(WaitTimeoutError) as error:
            engine.wait_for_completion(run["run_id"], timeout=0.03, poll_interval=0.01)
        assert error.value.code == "WAIT_TIMEOUT"
        assert error.value.run_id == run["run_id"]
        assert error.value.timeout == 0.03
        assert engine.get_run(run["run_id"])["state"] in {"RUNNING", "QUEUED"}
        engine.cancel_run(run["run_id"])
        assert engine.wait_for_completion(run["run_id"], timeout=5, poll_interval=0.01)["state"] == "CANCELLED"


def test_run_returns_failed_terminal_state_and_structured_error(tmp_path, stalled_job):
    job, peer = stalled_job
    peer.release()
    with Engine(tmp_path / "state.sqlite") as engine:
        run = engine.run(job, timeout=5, poll_interval=0.01)
        assert run["state"] == "FAILED"
        assert run["rows_committed"] == 0
        assert run["error"]["code"]
        assert run["error"]["message"]
        assert not run["commit_unknown"]
        assert engine.wait_for_completion(run["run_id"], timeout=0)["run_id"] == run["run_id"]


def test_native_validate_releases_gil(tmp_path, stalled_job):
    job, peer = stalled_job
    failures = []
    with Engine(tmp_path / "state.sqlite") as engine:
        worker = threading.Thread(target=capture_failure, args=(engine, job, failures))
        worker.start()
        try:
            assert peer.accepted.wait(2), "The native call held the GIL while awaiting TCP"
            assert worker.is_alive()
            engine.cancel_request("gil-test-tombstone")
            assert engine.get_request("gil-test-tombstone")["cancel_requested"] is True
        finally:
            peer.release()
            worker.join(timeout=5)
        assert not worker.is_alive()
        assert len(failures) == 1


def test_close_timeout_retains_lock_and_allows_query_cancel_and_retry(tmp_path, stalled_job):
    job, peer = stalled_job
    failures = []
    path = tmp_path / "state.sqlite"
    engine = Engine(path)
    worker = threading.Thread(target=capture_failure, args=(engine, job, failures))
    worker.start()
    try:
        assert peer.accepted.wait(2)
        with pytest.raises(CloseTimeoutError) as error:
            engine.close(timeout=0.03)
        assert error.value.code == "CLOSE_TIMEOUT"
        assert error.value.timeout == 0.03
        assert engine.list_runs() == []
        assert engine.cancel_request("closing-tombstone")["cancel_requested"] is True
        with pytest.raises(DunneleanError) as error:
            engine.submit(job)
        assert error.value.code == "ENGINE_CLOSING"
        with pytest.raises(DunneleanError) as error:
            Engine(path)
        assert error.value.code == "STATE_STORE"
    finally:
        peer.release()
        worker.join(timeout=5)
        engine.close(timeout=5)
    with Engine(path) as reopened:
        assert reopened.get_request("closing-tombstone")["cancel_requested"] is True


def test_concurrent_close_waits_for_active_validation(tmp_path, stalled_job):
    job, peer = stalled_job
    failures = []
    engine = Engine(tmp_path / "state.sqlite")
    worker = threading.Thread(target=capture_failure, args=(engine, job, failures))
    worker.start()
    assert peer.accepted.wait(2)
    close_errors = []
    def close():
        try:
            engine.close(timeout=3)
        except Exception as error:
            close_errors.append(error)
    closers = [threading.Thread(target=close) for _ in range(2)]
    for closer in closers:
        closer.start()
    try:
        time.sleep(0.05)
        assert all(closer.is_alive() for closer in closers)
    finally:
        peer.release()
        worker.join(timeout=5)
        for closer in closers:
            closer.join(timeout=5)
        engine.close(timeout=5)
    assert not close_errors
    assert all(not closer.is_alive() for closer in closers)


@pytest.mark.asyncio
async def test_async_factory_context_and_store_identity(tmp_path):
    path = tmp_path / "state.sqlite"
    engine = await AsyncEngine.open(path)
    identity = engine.state_store_id
    await engine.ready()
    assert await engine.list_runs() == []
    await engine.close()
    await engine.close()
    async with AsyncEngine(path) as reopened:
        assert reopened.state_store_id == identity
        status = await reopened.cancel_request("async-tombstone")
        assert status["run"] is None


@pytest.mark.asyncio
async def test_async_validation_keeps_event_loop_responsive(tmp_path, stalled_job):
    job, peer = stalled_job
    async with AsyncEngine(tmp_path / "state.sqlite") as engine:
        task = asyncio.create_task(engine.validate(job))
        try:
            assert await asyncio.wait_for(asyncio.to_thread(peer.accepted.wait, 2), 3)
            assert not task.done()
            status = await asyncio.wait_for(engine.cancel_request("responsive-loop"), 1)
            assert status["cancel_requested"] is True
        finally:
            peer.release()
            with pytest.raises(DunneleanError):
                await asyncio.wait_for(task, 5)


@pytest.mark.asyncio
async def test_async_run_returns_failed_terminal_state(tmp_path, stalled_job):
    job, peer = stalled_job
    peer.release()
    async with AsyncEngine(tmp_path / "state.sqlite") as engine:
        run = await engine.run(json.dumps(job), timeout=5, poll_interval=0.01)
        assert run["state"] == "FAILED" and run["rows_committed"] == 0
        assert run["error"]["code"]


@pytest.mark.asyncio
async def test_cancelling_async_waiter_does_not_cancel_job(tmp_path, stalled_job):
    job, peer = stalled_job
    async with AsyncEngine(tmp_path / "state.sqlite") as engine:
        run = await engine.submit(job)
        assert await asyncio.to_thread(peer.accepted.wait, 2)
        waiter = asyncio.create_task(engine.wait_for_completion(run["run_id"], timeout=5, poll_interval=0.01))
        await asyncio.sleep(0.03)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        current = await engine.get_run(run["run_id"])
        assert current["state"] in {"QUEUED", "RUNNING"}
        await engine.cancel_run(run["run_id"])
        terminal = await engine.wait_for_completion(run["run_id"], timeout=5, poll_interval=0.01)
        assert terminal["state"] == "CANCELLED"


@pytest.mark.asyncio
async def test_cancelled_validation_keeps_native_lease_until_completion(tmp_path, stalled_job):
    job, peer = stalled_job
    engine = await AsyncEngine.open(tmp_path / "state.sqlite")
    task = asyncio.create_task(engine.validate(job))
    try:
        assert await asyncio.to_thread(peer.accepted.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(CloseTimeoutError):
            await engine.close(timeout=0.03)
        assert await engine.list_runs() == []
    finally:
        peer.release()
        await engine.close(timeout=5)


async def reopen_when_unlocked(path, timeout=5):
    """Observe resource release without dropping the original Python references."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return await asyncio.to_thread(Engine, path)
        except DunneleanError as error:
            assert error.code == "STATE_STORE"
            if time.monotonic() >= deadline:
                raise AssertionError("Cancelled operation retained ownership of the state store") from error
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_cancelled_open_releases_store_with_task_and_traceback_retained(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite"
    constructed = []
    started = threading.Event()
    release = threading.Event()
    retained_errors = []

    def slow_constructor(*args, **kwargs):
        engine = Engine(*args, **kwargs)
        # Keep the actual Engine alive too: cleanup must not depend on its Drop/GC.
        constructed.append(engine)
        started.set()
        assert release.wait(5), "Construction barrier was not released"
        return engine

    monkeypatch.setattr(dunnelean, "Engine", slow_constructor)
    opening = asyncio.create_task(AsyncEngine.open(path))
    reopened = None
    try:
        assert await asyncio.to_thread(started.wait, 2)
        opening.cancel()
        try:
            await opening
        except asyncio.CancelledError as error:
            retained_errors.append(error)
        else:
            raise AssertionError("Opening waiter did not propagate cancellation")
        assert retained_errors[0].__traceback__ is not None
        release.set()
        reopened = await reopen_when_unlocked(path)
        assert opening.cancelled() and retained_errors[0].__traceback__ is not None
        assert constructed, "The real native engine must have acquired the store"
        with pytest.raises(DunneleanError) as error:
            constructed[0].ready()
        assert error.value.code == "ENGINE_CLOSED"
    finally:
        release.set()
        if reopened is not None:
            await asyncio.to_thread(reopened.close)
        for engine in constructed:
            await asyncio.to_thread(engine.close, timeout=5)


@pytest.mark.asyncio
async def test_cancelled_close_continues_native_cleanup_with_traceback_retained(tmp_path, stalled_job):
    job, peer = stalled_job
    path = tmp_path / "state.sqlite"
    engine = await AsyncEngine.open(path)
    validation = asyncio.create_task(engine.validate(job))
    retained_errors = []
    closing = None
    reopened = None
    try:
        assert await asyncio.to_thread(peer.accepted.wait, 2)
        closing = asyncio.create_task(engine.close(timeout=5))
        deadline = time.monotonic() + 2
        while True:
            try:
                await engine.ready()
            except DunneleanError as error:
                assert error.code in {"BUSY", "ENGINE_CLOSING"}
                break
            assert time.monotonic() < deadline, "Native close did not begin"
            await asyncio.sleep(0.01)
        closing.cancel()
        try:
            await closing
        except asyncio.CancelledError as error:
            retained_errors.append(error)
        else:
            raise AssertionError("Closing waiter did not propagate cancellation")
        assert retained_errors[0].__traceback__ is not None
        with pytest.raises(DunneleanError) as error:
            Engine(path)
        assert error.value.code == "STATE_STORE", "A live validation must retain the store"
        peer.release()
        with pytest.raises(DunneleanError):
            await asyncio.wait_for(validation, 5)
        reopened = await reopen_when_unlocked(path)
        assert closing.cancelled() and retained_errors[0].__traceback__ is not None
        with pytest.raises(DunneleanError) as error:
            await engine.list_runs()
        assert error.value.code == "ENGINE_CLOSED"
    finally:
        peer.release()
        if not validation.done():
            try:
                await asyncio.wait_for(validation, 5)
            except DunneleanError:
                pass
        if reopened is not None:
            await asyncio.to_thread(reopened.close)
        await engine.close(timeout=5)


@pytest.mark.asyncio
async def test_cancelled_submit_is_recoverable_by_request_id(tmp_path, stalled_job, monkeypatch):
    job, peer = stalled_job
    path = tmp_path / "state.sqlite"
    engine = await AsyncEngine.open(path)
    blocker = sqlite3.connect(path, isolation_level=None)
    started = threading.Event()
    retained_errors = []
    original_submit = engine._engine.submit

    def signal_submit_started(spec):
        started.set()
        return original_submit(spec)

    # Observe the worker entering the real call; do not replace its behavior.
    monkeypatch.setattr(engine._engine, "submit", signal_submit_started)
    blocker.execute("BEGIN IMMEDIATE")
    submitting = asyncio.create_task(engine.submit(job))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        assert not submitting.done()
        assert blocker.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
        submitting.cancel()
        try:
            await submitting
        except asyncio.CancelledError as error:
            retained_errors.append(error)
        else:
            raise AssertionError("Submitting waiter did not propagate cancellation")
        blocker.rollback()
        deadline = time.monotonic() + 3
        while True:
            try:
                request = await engine.get_request(job["request_id"])
                break
            except DunneleanError as error:
                assert error.code == "NOT_FOUND"
                assert time.monotonic() < deadline, "Cancelled waiter lost the submitted request"
                await asyncio.sleep(0.01)
        assert request["cancel_requested"] is False
        run = request["run"]
        assert run["request_id"] == job["request_id"]
        assert run["state"] in {"RUNNING", "QUEUED"}
        assert await asyncio.to_thread(peer.accepted.wait, 2)
        assert submitting.cancelled() and retained_errors[0].__traceback__ is not None
        # Recover the run rather than submitting another job or inferring cancellation.
        await engine.cancel_run(run["run_id"])
        terminal = await engine.wait_for_completion(run["run_id"], timeout=5, poll_interval=0.01)
        assert terminal["state"] == "CANCELLED"
        assert len(await engine.list_runs()) == 1
    finally:
        blocker.rollback()
        blocker.close()
        peer.release()
        await engine.close(timeout=5)


@pytest.mark.asyncio
async def test_cancelled_context_entry_releases_store_with_engine_and_traceback_retained(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite"
    started = threading.Event()
    release = threading.Event()
    retained_errors = []
    original_ready = Engine.ready

    def slow_ready(self):
        original_ready(self)
        started.set()
        assert release.wait(5), "Context-entry readiness barrier was not released"

    monkeypatch.setattr(Engine, "ready", slow_ready)
    engine = AsyncEngine(path)

    async def enter_context():
        async with engine:
            raise AssertionError("Context body ran before readiness completed")

    entering = asyncio.create_task(enter_context())
    reopened = None
    try:
        assert await asyncio.to_thread(started.wait, 2)
        assert engine.state_store_id, "The actual engine must exist before cancelling readiness"
        entering.cancel()
        try:
            await entering
        except asyncio.CancelledError as error:
            retained_errors.append(error)
        else:
            raise AssertionError("Context-entry waiter did not propagate cancellation")
        assert retained_errors[0].__traceback__ is not None
        release.set()
        reopened = await reopen_when_unlocked(path)
        assert entering.cancelled() and retained_errors[0].__traceback__ is not None
        with pytest.raises(DunneleanError) as error:
            await engine.list_runs()
        assert error.value.code == "ENGINE_CLOSED"
    finally:
        release.set()
        if reopened is not None:
            await asyncio.to_thread(reopened.close)
        await engine.close(timeout=5)
