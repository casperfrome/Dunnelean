//! Shared process-local runtime and lifecycle for Python and C ABI bindings.
use crate::{
    config::{RunSpec, ServerConfig},
    engine::{self, Engine},
    error::{Error, Result},
    store::Store,
};
use serde::Serialize;
use std::{
    sync::{Arc, Condvar, Mutex, TryLockError},
    time::{Duration, Instant},
};
use tokio::runtime::{Builder, Runtime};

fn json(value: &impl Serialize) -> Result<String> {
    serde_json::to_string(value).map_err(Into::into)
}

fn lock_error() -> Error {
    Error::new("ENGINE_INTERNAL", "Engine lifecycle lock is poisoned")
}

struct Session {
    // The runtime is explicitly destroyed before the engine releases its store.
    runtime: Option<Runtime>,
    engine: Engine,
}

impl Session {
    fn runtime(&self) -> &Runtime {
        // Only dispose takes the runtime, after the session has become exclusive.
        self.runtime.as_ref().expect("live session owns a runtime")
    }

    fn dispose(mut self, budget: Duration) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(budget);
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Even construction failures must not run Tokio's unbounded Drop wait.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Open,
    Closing,
    Finalizing,
    Closed,
}

struct Lifecycle {
    phase: Phase,
    session: Option<Arc<Session>>,
    leases: usize,
}

struct Shared {
    lifecycle: Mutex<Lifecycle>,
    changed: Condvar,
    closer: Mutex<()>,
}

#[derive(Clone, Copy)]
enum Access {
    Open,
    Observe,
}

struct Lease {
    shared: Arc<Shared>,
    session: Option<Arc<Session>>,
}

impl Lease {
    fn session(&self) -> &Session {
        self.session.as_deref().expect("live lease owns a session")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Drop the borrowed session before publishing leases == 0, so close can
        // acquire exclusive runtime ownership without an Arc reference race.
        drop(self.session.take());
        let mut lifecycle = self
            .shared
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        lifecycle.leases -= 1;
        self.shared.changed.notify_all();
    }
}

impl Shared {
    fn acquire(self: &Arc<Self>, access: Access) -> Result<Lease> {
        let mut lifecycle = self.lifecycle.lock().map_err(|_| lock_error())?;
        match lifecycle.phase {
            Phase::Closed => {
                return Err(Error::new("ENGINE_CLOSED", "Engine is closed"));
            }
            Phase::Closing if matches!(access, Access::Open) => {
                return Err(Error::new("ENGINE_CLOSING", "Engine is closing"));
            }
            Phase::Finalizing => {
                return Err(Error::new("ENGINE_CLOSING", "Engine is closing"));
            }
            _ => {}
        }
        let session = lifecycle.session.as_ref().ok_or_else(lock_error)?.clone();
        lifecycle.leases += 1;
        Ok(Lease {
            shared: self.clone(),
            session: Some(session),
        })
    }

    fn close(&self, timeout_ms: u64) -> Result<bool> {
        let started = Instant::now();
        let budget = Duration::from_millis(timeout_ms);
        // Only close calls serialize here. Regular methods retain access to
        // receipts/cancellation while a different thread waits for shutdown.
        let _closer = loop {
            match self.closer.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::Poisoned(_)) => return Err(lock_error()),
                Err(TryLockError::WouldBlock) => {
                    let remaining = budget.saturating_sub(started.elapsed());
                    if remaining.is_zero() {
                        return Ok(false);
                    }
                    std::thread::sleep(remaining.min(Duration::from_millis(5)));
                }
            }
        };
        let session = {
            let mut lifecycle = self.lifecycle.lock().map_err(|_| lock_error())?;
            if lifecycle.phase == Phase::Closed {
                return Ok(true);
            }
            lifecycle.phase = Phase::Closing;
            lifecycle.session.as_ref().ok_or_else(lock_error)?.clone()
        };
        let remaining_ms = budget
            .saturating_sub(started.elapsed())
            .as_millis()
            .min(u64::MAX as u128) as u64;
        if !session
            .runtime()
            .block_on(session.engine.shutdown_and_wait(remaining_ms))
        {
            return Ok(false);
        }
        let mut lifecycle = self.lifecycle.lock().map_err(|_| lock_error())?;
        while lifecycle.leases > 0 || !session.engine.store.is_exclusively_owned() {
            let remaining = budget.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Ok(false);
            }
            let (next, _) = self
                .changed
                // Store clones held by aborted Rust child tasks do not notify
                // this condition variable when dropped, so also poll briefly.
                .wait_timeout(lifecycle, remaining.min(Duration::from_millis(10)))
                .map_err(|_| lock_error())?;
            lifecycle = next;
        }
        let owner = lifecycle.session.take().ok_or_else(lock_error)?;
        lifecycle.phase = Phase::Finalizing;
        drop(lifecycle);
        drop(owner);
        match Arc::try_unwrap(session) {
            Ok(session) => {
                session.dispose(budget.saturating_sub(started.elapsed()));
                self.lifecycle.lock().map_err(|_| lock_error())?.phase = Phase::Closed;
                Ok(true)
            }
            Err(session) => {
                // Keep resources owned if an unexpected outstanding reference
                // exists, rather than reporting a closure that did not happen.
                let mut lifecycle = self.lifecycle.lock().map_err(|_| lock_error())?;
                lifecycle.session = Some(session);
                lifecycle.phase = Phase::Closing;
                Ok(false)
            }
        }
    }
}

fn spawn_cleanup(shared: &Arc<Shared>) -> Result<()> {
    let cleanup = shared.clone();
    std::thread::Builder::new()
        .name("dunnelean-cleanup".into())
        .spawn(move || {
            loop {
                match cleanup.close(60_000) {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(_) => {
                        // A poisoned lifecycle cannot safely be drained.
                        // Leaking is safer than blocking Python finalization.
                        std::mem::forget(cleanup);
                        return;
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(Into::into)
}

/// One persistent engine, store and Tokio runtime. Close explicitly to confirm
/// cleanup; dropping an abandoned wrapper schedules cleanup in the background.
pub struct NativeHost {
    creator_pid: u32,
    shared: Option<Arc<Shared>>,
}

impl NativeHost {
    pub fn open(state_path: String, max_running: usize, max_queued: usize) -> Result<Self> {
        let config = ServerConfig {
            state_path: state_path.clone(),
            max_running,
            max_queued,
            ..ServerConfig::default()
        };
        if state_path.is_empty() || state_path == ":memory:" {
            return Err(Error::config(
                "state_path must identify a persistent SQLite file",
            ));
        }
        if max_running == 0 || max_running > 64 || max_queued > 4096 {
            return Err(Error::config(
                "max_running must be 1..64 and max_queued must be 0..4096",
            ));
        }
        let store = Store::open(&state_path)?;
        let engine = Engine::new(store, &config)?;
        let worker_threads = std::thread::available_parallelism()
            .map(|available| available.get().min(4))
            .unwrap_or(1);
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("dunnelean-worker")
            .enable_all()
            .build()?;
        Ok(Self {
            creator_pid: std::process::id(),
            shared: Some(Arc::new(Shared {
                lifecycle: Mutex::new(Lifecycle {
                    phase: Phase::Open,
                    session: Some(Arc::new(Session {
                        runtime: Some(runtime),
                        engine,
                    })),
                    leases: 0,
                }),
                changed: Condvar::new(),
                closer: Mutex::new(()),
            })),
        })
    }

    fn shared(&self) -> Result<&Arc<Shared>> {
        // Check before any lock after fork, where worker threads no longer exist.
        if self.creator_pid != std::process::id() {
            return Err(Error::new(
                "ENGINE_FORKED",
                "An engine cannot be used in a forked process; create a new engine there",
            ));
        }
        self.shared.as_ref().ok_or_else(lock_error)
    }

    fn operate<T>(
        &self,
        access: Access,
        operation: impl FnOnce(&Session) -> Result<T>,
    ) -> Result<T> {
        let lease = self.shared()?.acquire(access)?;
        operation(lease.session())
    }

    pub fn validate(&self, spec_json: &str) -> Result<String> {
        self.operate(Access::Open, |session| {
            let spec: RunSpec = serde_json::from_str(spec_json)?;
            let validated = session.runtime().block_on(engine::validate(&spec))?;
            json(&engine::validation_json(&validated))
        })
    }

    pub fn submit(&self, spec_json: &str) -> Result<String> {
        self.operate(Access::Open, |session| {
            let spec: RunSpec = serde_json::from_str(spec_json)?;
            json(&session.runtime().block_on(session.engine.submit(spec))?)
        })
    }

    pub fn get_run(&self, id: &str) -> Result<String> {
        self.operate(Access::Observe, |session| {
            json(&session.engine.store.get(id)?)
        })
    }

    pub fn cancel_run(&self, id: &str) -> Result<String> {
        self.operate(Access::Observe, |session| {
            json(&session.runtime().block_on(session.engine.cancel(id))?)
        })
    }

    pub fn get_request(&self, id: &str) -> Result<String> {
        self.operate(Access::Observe, |session| {
            json(&session.engine.store.request(id)?)
        })
    }

    pub fn cancel_request(&self, id: &str) -> Result<String> {
        self.operate(Access::Observe, |session| {
            json(
                &session
                    .runtime()
                    .block_on(session.engine.cancel_request(id))?,
            )
        })
    }

    pub fn list_runs(&self, limit: usize, offset: usize) -> Result<String> {
        if !(1..=500).contains(&limit) {
            return Err(Error::config("limit must be 1..500"));
        }
        self.operate(Access::Observe, |session| {
            json(&session.engine.store.list(limit, offset)?)
        })
    }

    pub fn list_batches(&self, id: &str) -> Result<String> {
        self.operate(Access::Observe, |session| {
            session.engine.store.get(id)?;
            json(&session.engine.store.batches(id)?)
        })
    }

    pub fn state_store_id(&self) -> Result<String> {
        self.operate(Access::Observe, |session| {
            session.engine.store.state_store_id()
        })
    }

    pub fn ready(&self) -> Result<()> {
        self.operate(Access::Observe, |session| session.engine.ready())
    }

    pub fn close(&self, timeout_ms: u64) -> Result<bool> {
        self.shared()?.close(timeout_ms)
    }

    pub fn abandon(&self) -> Result<()> {
        spawn_cleanup(self.shared()?)
    }
}

impl Drop for NativeHost {
    fn drop(&mut self) {
        let Some(shared) = self.shared.take() else {
            return;
        };
        if self.creator_pid != std::process::id() {
            std::mem::forget(shared);
            return;
        }
        if spawn_cleanup(&shared).is_err() {
            std::mem::forget(shared);
        }
    }
}

pub fn connectors() -> Result<String> {
    json(&crate::api::connector_info())
}
#[cfg(test)]
mod tests {
    use super::*;

    fn shared(path: &std::path::Path) -> Arc<Shared> {
        let engine = Engine::new(Store::open(path).unwrap(), &ServerConfig::default()).unwrap();
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        Arc::new(Shared {
            lifecycle: Mutex::new(Lifecycle {
                phase: Phase::Open,
                session: Some(Arc::new(Session {
                    runtime: Some(runtime),
                    engine,
                })),
                leases: 0,
            }),
            changed: Condvar::new(),
            closer: Mutex::new(()),
        })
    }

    #[test]
    fn close_timeout_keeps_store_owned_and_allows_a_retry() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let shared = shared(&path);
        let lease = shared.acquire(Access::Open).unwrap();
        assert!(!shared.close(0).unwrap());
        assert!(Store::open(&path).is_err());
        assert!(matches!(
            shared.acquire(Access::Open),
            Err(error) if error.code == "ENGINE_CLOSING"
        ));
        let receipt_access = shared.acquire(Access::Observe).unwrap();
        assert!(
            !receipt_access
                .session()
                .engine
                .store
                .state_store_id()
                .unwrap()
                .is_empty()
        );
        drop(receipt_access);
        drop(lease);
        assert!(shared.close(1000).unwrap());
        assert!(shared.close(0).unwrap());
        assert!(matches!(
            shared.acquire(Access::Observe),
            Err(error) if error.code == "ENGINE_CLOSED"
        ));
        assert!(Store::open(&path).is_ok());
    }

    #[test]
    fn close_waits_for_a_cancelled_worker_to_release_its_store_clone() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let shared = shared(&path);
        // An aborted child future can retain this owner until its next poll,
        // even after the supervising run and every Python lease have drained.
        let worker_store = {
            let lease = shared.acquire(Access::Observe).unwrap();
            lease.session().engine.store.clone()
        };
        assert!(!shared.close(0).unwrap());
        assert!(Store::open(&path).is_err());
        assert!(matches!(
            shared.acquire(Access::Open),
            Err(error) if error.code == "ENGINE_CLOSING"
        ));
        drop(worker_store);
        assert!(shared.close(1000).unwrap());
        assert!(Store::open(&path).is_ok());
    }

    #[test]
    fn close_polls_for_store_owners_that_do_not_notify_the_lifecycle() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let shared = shared(&path);
        let worker_store = {
            let lease = shared.acquire(Access::Observe).unwrap();
            lease.session().engine.store.clone()
        };
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            drop(worker_store);
        });
        assert!(shared.close(1000).unwrap());
        worker.join().unwrap();
        assert!(Store::open(&path).is_ok());
    }

    #[test]
    fn concurrent_close_waits_for_leases_and_releases_the_store() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let shared = shared(&path);
        let lease = shared.acquire(Access::Observe).unwrap();
        let closer = {
            let shared = shared.clone();
            std::thread::spawn(move || shared.close(1000).unwrap())
        };
        let started = Instant::now();
        while shared.lifecycle.lock().unwrap().phase != Phase::Closing {
            assert!(started.elapsed() < Duration::from_secs(1));
            std::thread::yield_now();
        }
        assert!(!shared.close(0).unwrap());
        drop(lease);
        assert!(closer.join().unwrap());
        assert!(Store::open(&path).is_ok());
    }

    #[test]
    fn destructor_drains_on_a_background_thread() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let shared = shared(&path);
        let engine = NativeHost {
            creator_pid: std::process::id(),
            shared: Some(shared.clone()),
        };
        let lease = shared.acquire(Access::Observe).unwrap();
        drop(engine);
        assert!(Store::open(&path).is_err());
        drop(lease);
        let started = Instant::now();
        loop {
            if Store::open(&path).is_ok() {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(2));
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn abandonment_drains_while_the_native_object_is_still_retained() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let shared = shared(&path);
        let engine = NativeHost {
            creator_pid: std::process::id(),
            shared: Some(shared.clone()),
        };
        let lease = shared.acquire(Access::Observe).unwrap();
        spawn_cleanup(engine.shared().unwrap()).unwrap();
        assert!(Store::open(&path).is_err());
        drop(lease);
        let started = Instant::now();
        loop {
            if Store::open(&path).is_ok() {
                break;
            }
            assert!(started.elapsed() < Duration::from_secs(2));
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(engine.shared().unwrap().close(0).unwrap());
        assert!(matches!(
            engine.shared().unwrap().acquire(Access::Observe),
            Err(error) if error.code == "ENGINE_CLOSED"
        ));
    }

    #[test]
    fn pid_guard_rejects_access_before_touching_locks() {
        let temp = tempfile::tempdir().unwrap();
        let shared = shared(&temp.path().join("state.sqlite"));
        let mut engine = NativeHost {
            creator_pid: std::process::id().wrapping_add(1),
            shared: Some(shared.clone()),
        };
        let lifecycle = shared.lifecycle.lock().unwrap();
        assert!(matches!(engine.shared(), Err(error) if error.code == "ENGINE_FORKED"));
        drop(lifecycle);
        engine.creator_pid = std::process::id();
        assert!(shared.close(1000).unwrap());
    }
}
