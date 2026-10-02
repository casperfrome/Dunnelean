//! Stable, pointer-and-integer-only C ABI for process-local language bindings.
use dunnelean::{
    error::{Error, Result},
    native_host::{self, NativeHost},
};
use serde::Deserialize;
use std::{
    collections::HashMap,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};

pub const ABI_VERSION: u32 = 1;
pub const OP_VALIDATE: u32 = 1;
pub const OP_SUBMIT: u32 = 2;
pub const OP_GET_RUN: u32 = 3;
pub const OP_CANCEL_RUN: u32 = 4;
pub const OP_LIST_RUNS: u32 = 5;
pub const OP_GET_REQUEST: u32 = 6;
pub const OP_CANCEL_REQUEST: u32 = 7;
pub const OP_LIST_BATCHES: u32 = 8;
pub const OP_STATE_STORE_ID: u32 = 9;
pub const OP_READY: u32 = 10;
pub const OP_CONNECTORS: u32 = 11;

static CREATOR_PID: AtomicU32 = AtomicU32::new(0);
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);
static HOSTS: OnceLock<Mutex<HashMap<u64, Arc<NativeHost>>>> = OnceLock::new();

fn check_pid() -> Result<()> {
    let pid = std::process::id();
    let owner = CREATOR_PID.load(Ordering::Acquire);
    if owner == 0 {
        match CREATOR_PID.compare_exchange(0, pid, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(()),
            Err(owner) if owner == pid => return Ok(()),
            Err(_) => {}
        }
    } else if owner == pid {
        return Ok(());
    }
    Err(Error::new(
        "ENGINE_FORKED",
        "The native library cannot be used after fork; exec a new process",
    ))
}

fn registry() -> Result<&'static Mutex<HashMap<u64, Arc<NativeHost>>>> {
    // Before touching OnceLock or Mutex: inherited locks can be held forever.
    check_pid()?;
    Ok(HOSTS.get_or_init(|| Mutex::new(HashMap::new())))
}

fn host(handle: u64) -> Result<Arc<NativeHost>> {
    registry()?
        .lock()
        .map_err(|_| internal())?
        .get(&handle)
        .cloned()
        .ok_or_else(|| Error::new("ENGINE_CLOSED", "Engine handle was released or is invalid"))
}

fn internal() -> Error {
    Error::new("ENGINE_INTERNAL", "Native engine internal failure")
}

fn encode(value: &impl serde::Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(Into::into)
}

// C callers guarantee that a non-null pointer names len readable bytes.
unsafe fn input<'a>(data: *const u8, len: usize) -> Result<&'a str> {
    if len > isize::MAX as usize || (len > 0 && data.is_null()) {
        return Err(Error::config("Invalid input pointer or length"));
    }
    if len == 0 {
        return Ok("");
    }
    let bytes = unsafe { std::slice::from_raw_parts(data, len) };
    std::str::from_utf8(bytes).map_err(|_| Error::config("Input must be UTF-8"))
}

// Always initialize outputs, including errors and caught panics.
unsafe fn boundary(
    output: *mut *mut u8,
    output_len: *mut usize,
    operation: impl FnOnce() -> Result<Vec<u8>>,
) -> i32 {
    if output.is_null() || output_len.is_null() {
        return 1;
    }
    unsafe {
        output.write(ptr::null_mut());
        output_len.write(0);
    }
    let (status, bytes) = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(bytes)) => (0, bytes),
        Ok(Err(error)) => (1, encode(&error).unwrap_or_else(|_| fallback_error())),
        Err(_) => (2, encode(&internal()).unwrap_or_else(|_| fallback_error())),
    };
    let allocation = bytes.into_boxed_slice();
    let len = allocation.len();
    let data = Box::into_raw(allocation) as *mut u8;
    unsafe {
        output.write(data);
        output_len.write(len);
    }
    status
}

fn fallback_error() -> Vec<u8> {
    br#"{"code":"ENGINE_INTERNAL","message":"Native engine internal failure","commit_unknown":false,"retryable":false}"#.to_vec()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenOptions {
    state_path: String,
    #[serde(default = "default_running")]
    max_running: usize,
    #[serde(default = "default_queued")]
    max_queued: usize,
}

fn default_running() -> usize {
    2
}
fn default_queued() -> usize {
    16
}
fn default_limit() -> usize {
    50
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdInput {
    id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListInput {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

fn invoke(handle: u64, op: u32, value: &str) -> Result<Vec<u8>> {
    if op == OP_CONNECTORS {
        check_pid()?;
        return native_host::connectors().map(String::into_bytes);
    }
    let host = host(handle)?;
    let id = || {
        serde_json::from_str::<IdInput>(value)
            .map(|input| input.id)
            .map_err(Error::from)
    };
    let result = match op {
        OP_VALIDATE => host.validate(value)?,
        OP_SUBMIT => host.submit(value)?,
        OP_GET_RUN => host.get_run(&id()?)?,
        OP_CANCEL_RUN => host.cancel_run(&id()?)?,
        OP_LIST_RUNS => {
            let pagination: ListInput = serde_json::from_str(value)?;
            host.list_runs(pagination.limit, pagination.offset)?
        }
        OP_GET_REQUEST => host.get_request(&id()?)?,
        OP_CANCEL_REQUEST => host.cancel_request(&id()?)?,
        OP_LIST_BATCHES => host.list_batches(&id()?)?,
        OP_STATE_STORE_ID => serde_json::to_string(&host.state_store_id()?)?,
        OP_READY => {
            host.ready()?;
            r#"{"status":"ready"}"#.into()
        }
        _ => return Err(Error::config("Unknown native operation")),
    };
    Ok(result.into_bytes())
}

#[unsafe(no_mangle)]
pub extern "C" fn dunnelean_abi_version() -> u32 {
    catch_unwind(|| ABI_VERSION).unwrap_or(0)
}

/// Return JSON containing version and ABI version.
///
/// # Safety
/// Both output pointers must be writable and non-overlapping. Free the returned
/// buffer exactly once using dunnelean_buffer_free with its original length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dunnelean_native_version(
    output: *mut *mut u8,
    output_len: *mut usize,
) -> i32 {
    unsafe {
        boundary(output, output_len, || {
            encode(&serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"), "abi_version": ABI_VERSION
            }))
        })
    }
}

/// Create an engine and return its integer handle and state-store identity.
///
/// # Safety
/// Input must name input_len readable bytes for the duration of this call.
/// out_handle and both output pointers must be writable and non-overlapping.
/// Free the returned buffer exactly once using dunnelean_buffer_free.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dunnelean_engine_open(
    data: *const u8,
    len: usize,
    out_handle: *mut u64,
    output: *mut *mut u8,
    output_len: *mut usize,
) -> i32 {
    if !out_handle.is_null() {
        unsafe {
            out_handle.write(0);
        }
    }
    unsafe {
        boundary(output, output_len, || {
            if out_handle.is_null() {
                return Err(Error::config("Missing engine handle output"));
            }
            check_pid()?;
            let options: OpenOptions = serde_json::from_str(input(data, len)?)?;
            let engine = Arc::new(NativeHost::open(
                options.state_path,
                options.max_running,
                options.max_queued,
            )?);
            let response =
                encode(&serde_json::json!({"state_store_id": engine.state_store_id()?}))?;
            let handle = NEXT_HANDLE
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_add(1)
                })
                .map_err(|_| internal())?;
            registry()?
                .lock()
                .map_err(|_| internal())?
                .insert(handle, engine);
            out_handle.write(handle);
            Ok(response)
        })
    }
}

/// Invoke an operation on an engine; OP_CONNECTORS also permits handle zero.
///
/// # Safety
/// Input must name len readable bytes for this call. Output pointers must be
/// writable and non-overlapping; free the returned buffer exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dunnelean_engine_invoke(
    handle: u64,
    op: u32,
    data: *const u8,
    len: usize,
    output: *mut *mut u8,
    output_len: *mut usize,
) -> i32 {
    unsafe { boundary(output, output_len, || invoke(handle, op, input(data, len)?)) }
}

/// Request cancellation and wait up to timeout_ms; JSON closed=false means retry.
///
/// # Safety
/// Output pointers must be writable and non-overlapping. Free the returned
/// buffer exactly once. The handle stays valid after either close outcome.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dunnelean_engine_close(
    handle: u64,
    timeout_ms: u64,
    output: *mut *mut u8,
    output_len: *mut usize,
) -> i32 {
    unsafe {
        boundary(output, output_len, || {
            encode(&serde_json::json!({"closed": host(handle)?.close(timeout_ms)?}))
        })
    }
}

/// Release a handle without waiting; repeated release is safe.
#[unsafe(no_mangle)]
pub extern "C" fn dunnelean_engine_release(handle: u64) -> i32 {
    match catch_unwind(AssertUnwindSafe(|| -> Result<()> {
        let engine = registry()?.lock().map_err(|_| internal())?.remove(&handle);
        if let Some(engine) = engine {
            engine.abandon()?;
        }
        Ok(())
    })) {
        Ok(Ok(())) => 0,
        Ok(Err(_)) => 1,
        Err(_) => 2,
    }
}

/// Free an output allocation from this exact library.
///
/// # Safety
/// data and len must be the unchanged output pair returned by this library.
/// The pair may be freed only once. Null with zero length is accepted.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn dunnelean_buffer_free(data: *mut u8, len: usize) {
    // No user code or destructors run here, but keep every exported boundary
    // guarded against an ordinary Rust unwind.
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !data.is_null() {
            unsafe {
                drop(Box::from_raw(ptr::slice_from_raw_parts_mut(data, len)));
            }
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    unsafe fn consume(data: *mut u8, len: usize) -> Value {
        let value =
            serde_json::from_slice(unsafe { std::slice::from_raw_parts(data, len) }).unwrap();
        unsafe {
            dunnelean_buffer_free(data, len);
        }
        value
    }

    fn call(handle: u64, op: u32, input: &str) -> (i32, Value) {
        let mut output = ptr::null_mut();
        let mut output_len = 0;
        let status = unsafe {
            dunnelean_engine_invoke(
                handle,
                op,
                input.as_ptr(),
                input.len(),
                &mut output,
                &mut output_len,
            )
        };
        (status, unsafe { consume(output, output_len) })
    }

    fn open(path: &std::path::Path) -> u64 {
        let input = serde_json::json!({"state_path":path}).to_string();
        let mut handle = 0;
        let mut output = ptr::null_mut();
        let mut len = 0;
        let status = unsafe {
            dunnelean_engine_open(
                input.as_ptr(),
                input.len(),
                &mut handle,
                &mut output,
                &mut len,
            )
        };
        let result = unsafe { consume(output, len) };
        assert_eq!(status, 0, "{result}");
        assert_ne!(handle, 0);
        assert!(!result["state_store_id"].as_str().unwrap().is_empty());
        handle
    }

    #[test]
    fn abi_returns_owned_json_and_complete_errors() {
        assert_eq!(dunnelean_abi_version(), 1);
        let temp = tempfile::tempdir().unwrap();
        let handle = open(&temp.path().join("state.sqlite"));
        assert_eq!(call(handle, OP_LIST_RUNS, "{}"), (0, serde_json::json!([])));
        let (status, error) = call(handle, OP_GET_RUN, r#"{"id":"missing"}"#);
        assert_eq!(status, 1);
        assert_eq!(error["code"], "NOT_FOUND");
        assert_eq!(error["commit_unknown"], false);
        assert_eq!(error["retryable"], false);
        assert!(error["message"].is_string());
        assert_eq!(dunnelean_engine_release(handle), 0);
        assert_eq!(dunnelean_engine_release(handle), 0);
        assert_eq!(call(handle, OP_READY, "").1["code"], "ENGINE_CLOSED");
    }

    #[test]
    fn close_timeout_keeps_handle_and_store_owned_until_retry() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("state.sqlite");
        let handle = open(&path);
        // A listening socket which never sends a MySQL greeting makes the
        // run deterministically live until shutdown requests cancellation.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut input: Value =
            serde_json::from_str(include_str!("../../../examples/mysql-to-doris.json")).unwrap();
        for connection in ["/reader/connection", "/writer/sql"] {
            let value = input.pointer_mut(connection).unwrap();
            value["port"] = listener.local_addr().unwrap().port().into();
            value["credentials"] = serde_json::json!({"username":"test","password":""});
        }
        let run = call(handle, OP_SUBMIT, &input.to_string());
        assert_eq!(run.0, 0, "{}", run.1);
        let mut output = ptr::null_mut();
        let mut len = 0;
        assert_eq!(
            unsafe { dunnelean_engine_close(handle, 0, &mut output, &mut len) },
            0
        );
        assert_eq!(unsafe { consume(output, len) }["closed"], false);
        assert!(dunnelean::store::Store::open(&path).is_err());
        assert_eq!(call(handle, OP_LIST_RUNS, "{}").0, 0);
        assert_eq!(
            unsafe { dunnelean_engine_close(handle, 5000, &mut output, &mut len) },
            0
        );
        assert_eq!(unsafe { consume(output, len) }["closed"], true);
        assert!(dunnelean::store::Store::open(&path).is_ok());
        assert_eq!(dunnelean_engine_release(handle), 0);
    }

    #[test]
    fn panic_is_captured_and_outputs_are_initialized() {
        let mut output = ptr::dangling_mut();
        let mut len = usize::MAX;
        let status = unsafe { boundary(&mut output, &mut len, || panic!("FFI test panic")) };
        assert_eq!(status, 2);
        assert_eq!(unsafe { consume(output, len) }["code"], "ENGINE_INTERNAL");
    }

    #[test]
    fn input_rejection_and_connectors_need_no_engine() {
        let mut output = ptr::null_mut();
        let mut len = 0;
        let status = unsafe {
            dunnelean_engine_invoke(0, OP_CONNECTORS, ptr::null(), 1, &mut output, &mut len)
        };
        assert_eq!(status, 1);
        assert_eq!(unsafe { consume(output, len) }["code"], "INVALID_CONFIG");
        let (status, connectors) = call(0, OP_CONNECTORS, "");
        assert_eq!(status, 0);
        assert!(connectors["request_schema"].is_object());
    }

    #[test]
    fn release_cannot_invalidate_an_already_acquired_owner() {
        let temp = tempfile::tempdir().unwrap();
        let handle = open(&temp.path().join("state.sqlite"));
        let acquired = host(handle).unwrap();
        assert_eq!(dunnelean_engine_release(handle), 0);
        assert!(acquired.close(1000).unwrap());
        assert_eq!(call(handle, OP_READY, "").1["code"], "ENGINE_CLOSED");
    }
}
