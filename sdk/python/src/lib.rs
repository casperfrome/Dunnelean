use dunnelean::{
    error::{Error, Result},
    native_host::{self, NativeHost},
};
use pyo3::{create_exception, exceptions::PyException, prelude::*};

create_exception!(_native, NativeError, PyException);

fn native_error(error: Error) -> PyErr {
    NativeError::new_err(
        serde_json::json!({
            "code": error.code,
            "message": error.message,
            "commit_unknown": error.commit_unknown,
            "retryable": error.retryable,
        })
        .to_string(),
    )
}

#[pyclass(frozen, module = "dunnelean._native")]
struct NativeEngine {
    host: NativeHost,
}

impl NativeEngine {
    fn operate<T: Send>(
        &self,
        py: Python<'_>,
        operation: impl FnOnce(&NativeHost) -> Result<T> + Send,
    ) -> PyResult<T> {
        py.detach(|| operation(&self.host)).map_err(native_error)
    }
}

#[pymethods]
impl NativeEngine {
    #[new]
    #[pyo3(signature = (state_path, max_running=2, max_queued=16))]
    fn new(
        py: Python<'_>,
        state_path: String,
        max_running: usize,
        max_queued: usize,
    ) -> PyResult<Self> {
        py.detach(|| NativeHost::open(state_path, max_running, max_queued))
            .map(|host| Self { host })
            .map_err(native_error)
    }

    fn validate(&self, py: Python<'_>, spec_json: String) -> PyResult<String> {
        self.operate(py, |host| host.validate(&spec_json))
    }
    fn submit(&self, py: Python<'_>, spec_json: String) -> PyResult<String> {
        self.operate(py, |host| host.submit(&spec_json))
    }
    fn get_run(&self, py: Python<'_>, id: String) -> PyResult<String> {
        self.operate(py, |host| host.get_run(&id))
    }
    fn cancel_run(&self, py: Python<'_>, id: String) -> PyResult<String> {
        self.operate(py, |host| host.cancel_run(&id))
    }
    fn get_request(&self, py: Python<'_>, id: String) -> PyResult<String> {
        self.operate(py, |host| host.get_request(&id))
    }
    fn cancel_request(&self, py: Python<'_>, id: String) -> PyResult<String> {
        self.operate(py, |host| host.cancel_request(&id))
    }
    fn list_runs(&self, py: Python<'_>, limit: usize, offset: usize) -> PyResult<String> {
        self.operate(py, |host| host.list_runs(limit, offset))
    }
    fn list_batches(&self, py: Python<'_>, id: String) -> PyResult<String> {
        self.operate(py, |host| host.list_batches(&id))
    }
    fn state_store_id(&self, py: Python<'_>) -> PyResult<String> {
        self.operate(py, NativeHost::state_store_id)
    }
    fn ready(&self, py: Python<'_>) -> PyResult<()> {
        self.operate(py, NativeHost::ready)
    }
    fn close(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<bool> {
        self.operate(py, |host| host.close(timeout_ms))
    }
    fn abandon(&self, py: Python<'_>) -> PyResult<()> {
        self.operate(py, NativeHost::abandon)
    }
}

#[pyfunction]
fn connectors(py: Python<'_>) -> PyResult<String> {
    py.detach(native_host::connectors).map_err(native_error)
}

#[pymodule(gil_used = true)]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeEngine>()?;
    module.add("NativeError", module.py().get_type::<NativeError>())?;
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_function(wrap_pyfunction!(connectors, module)?)?;
    Ok(())
}
