use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, put},
};
use dunnelean::{
    config::{RunSpec, WriterConfig},
    connectors::{Writer, doris::DorisSink},
    store::Store,
    types::TargetColumn,
};
use serde_json::{Value, json};
use std::{io::Cursor, sync::Arc};
use tokio::sync::Mutex;

fn spec() -> RunSpec {
    let mut v: Value =
        serde_json::from_str(include_str!("../examples/mysql-to-doris.json")).unwrap();
    for pointer in ["/reader/connection/credentials", "/writer/sql/credentials"] {
        let c = v.pointer_mut(pointer).unwrap().as_object_mut().unwrap();
        c.remove("password_env");
        c.insert("password".into(), json!("unit-secret"));
    }
    v["request_id"] = json!("idempotent-test");
    serde_json::from_value(v).unwrap()
}
#[test]
fn idempotent_submission_and_interrupted_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("state.sqlite");
    let spec = spec();
    let store = Store::open(&path).unwrap();
    assert!(
        Store::open(&path).is_err(),
        "a second process must not interrupt a live owner's runs"
    );
    let (run, new) = store.submit(&spec).unwrap();
    assert!(new);
    let (same, new) = store.submit(&spec).unwrap();
    assert!(!new);
    assert_eq!(same.run_id, run.run_id);
    let mut different = spec.clone();
    different.execution.queue_capacity += 1;
    assert_eq!(store.submit(&different).unwrap_err().code, "CONFLICT");
    store
        .intent(&run.run_id, 0, 3, 100, Some("label0"))
        .unwrap();
    store
        .confirmed(&run.run_id, 0, 3, 0, 6, &json!({"ok":true}))
        .unwrap();
    assert!(
        store
            .confirmed(&run.run_id, 0, 3, 0, 6, &json!({}))
            .is_err()
    );
    store
        .intent(&run.run_id, 1, 5, 200, Some("label1"))
        .unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let recovered = store.get(&run.run_id).unwrap();
    assert_eq!(recovered.state, "INTERRUPTED");
    assert_eq!(recovered.rows_committed, 3);
    assert_eq!(recovered.server_affected_rows, 6);
    assert!(recovered.partial_write && recovered.commit_unknown);
    assert!(
        !serde_json::to_string(&recovered)
            .unwrap()
            .contains("unit-secret")
    );
    assert_eq!(store.submit(&spec).unwrap().0.state, "INTERRUPTED");
}
#[test]
fn schema_and_config_reject_silent_misconfiguration() {
    let s = spec();
    assert!(s.validate().is_ok());
    let mut v = serde_json::to_value(&s).unwrap();
    v["reader"]["fetch_size"] = json!(5);
    assert!(serde_json::from_value::<RunSpec>(v).is_err());
    let mut s = spec();
    if let WriterConfig::Doris(w) = &mut s.writer {
        w.headers.insert("FORMAT".into(), "csv".into());
    }
    assert!(s.validate().is_err());
    let openapi = dunnelean::api::openapi();
    assert_eq!(openapi["openapi"], "3.1.0");
    assert!(openapi.pointer("/components/schemas/RunSpec").is_some());
}

#[test]
fn sqlite_receipt_failure_leaves_intent_and_counters_atomic() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("state.sqlite");
    let store = Store::open(&path).unwrap();
    let (run, _) = store.submit(&spec()).unwrap();
    store.intent(&run.run_id, 0, 2, 32, Some("label")).unwrap();
    let fault = rusqlite::Connection::open(&path).unwrap();
    fault.execute_batch("CREATE TRIGGER fail_receipt BEFORE UPDATE ON runs BEGIN SELECT RAISE(FAIL, 'injected SQLite write failure'); END;").unwrap();
    assert!(
        store
            .confirmed(&run.run_id, 0, 2, 0, 2, &json!({}))
            .is_err()
    );
    assert_eq!(store.get(&run.run_id).unwrap().rows_committed, 0);
    assert!(store.has_pending(&run.run_id).unwrap());
    assert_eq!(store.batches(&run.run_id).unwrap()[0]["state"], "INTENT");
    fault.execute_batch("DROP TRIGGER fail_receipt").unwrap();
    drop(store);
    assert!(
        Store::open(&path)
            .unwrap()
            .get(&run.run_id)
            .unwrap()
            .commit_unknown
    );
}

#[derive(Clone)]
struct Fake {
    mode: &'static str,
    requests: Arc<Mutex<Requests>>,
}
type Requests = Vec<(String, Vec<u8>)>;
async fn load(
    State(state): State<Fake>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, Json<Value>) {
    let mut requests = state.requests.lock().await;
    let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(&body), None).unwrap();
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 2);
    requests.push((headers["label"].to_str().unwrap().into(), body.to_vec()));
    let n = requests.len();
    let success = json!({"Status":"Success","NumberLoadedRows":2,"NumberFilteredRows":0,"NumberUnselectedRows":0});
    match state.mode {
        "publish" => (
            StatusCode::OK,
            Json(
                json!({"Status":"Publish Timeout","NumberLoadedRows":2,"NumberFilteredRows":0,"NumberUnselectedRows":0}),
            ),
        ),
        "duplicate" => (
            StatusCode::OK,
            Json(json!({"Status":"Label Already Exists","ExistingJobStatus":"FINISHED"})),
        ),
        "lost" | "unknown" => (StatusCode::BAD_GATEWAY, Json(json!({}))),
        "abort_then_retry" if n == 1 => (StatusCode::BAD_GATEWAY, Json(json!({}))),
        _ => (StatusCode::OK, Json(success)),
    }
}
async fn load_state(State(state): State<Fake>) -> Json<Value> {
    Json(
        json!({"code":0,"data":match state.mode{"unknown"=>"UNKNOWN","abort_then_retry"=>"ABORTED",_=>"VISIBLE"}}),
    )
}
async fn fake_writer(
    mode: &'static str,
) -> (DorisSink, RecordBatch, Fake, tokio::task::JoinHandle<()>) {
    let state = Fake {
        mode,
        requests: Arc::new(Mutex::new(vec![])),
    };
    let app = Router::new()
        .route("/api/dunnelean_test/target_orders/_stream_load", put(load))
        .route("/api/dunnelean_test/get_load_state", get(load_state))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let WriterConfig::Doris(mut config) = spec().writer else {
        unreachable!()
    };
    config.fe_http_urls = vec![base.clone()];
    config.be_http_urls = vec![base];
    config.options.retry_backoff_ms = 1;
    let field = Field::new("id", DataType::Int64, false);
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let columns = vec![TargetColumn {
        field,
        sql_type: "bigint".into(),
        has_default: false,
        generated: false,
        key: true,
        datetime_precision: None,
    }];
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    (
        DorisSink::new(*config, columns).unwrap(),
        batch,
        state,
        task,
    )
}
#[tokio::test]
async fn publish_timeout_duplicate_and_lost_response_are_reconciled_without_replay() {
    for mode in ["publish", "duplicate", "lost"] {
        let (mut writer, batch, state, task) = fake_writer(mode).await;
        let receipt = writer.write(&batch, "stable_label").await.unwrap();
        assert_eq!(receipt.loaded, 2);
        assert_eq!(state.requests.lock().await.len(), 1);
        task.abort();
    }
}
#[tokio::test]
async fn only_confirmed_abort_retries_identical_arrow_payload_and_label() {
    let (mut writer, batch, state, task) = fake_writer("abort_then_retry").await;
    assert_eq!(
        writer.write(&batch, "stable_label").await.unwrap().loaded,
        2
    );
    let requests = state.requests.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
    task.abort();
}
#[tokio::test]
async fn ambiguous_commit_is_not_retried() {
    let (mut writer, batch, state, task) = fake_writer("unknown").await;
    assert!(
        writer
            .write(&batch, "stable_label")
            .await
            .unwrap_err()
            .commit_unknown
    );
    assert_eq!(state.requests.lock().await.len(), 1);
    task.abort();
}

#[test]
fn cancellation_tombstone_and_store_identity_survive_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("state.sqlite");
    let store = Store::open(&path).unwrap();
    let identity = store.state_store_id().unwrap();
    store.cancel_request("idempotent-test").unwrap();
    assert_eq!(store.submit(&spec()).unwrap_err().code, "REQUEST_CANCELLED");
    assert_eq!(
        store.request("idempotent-test").unwrap()["cancel_requested"],
        true
    );
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.state_store_id().unwrap(), identity);
    assert_eq!(store.submit(&spec()).unwrap_err().code, "REQUEST_CANCELLED");
}
#[tokio::test]
async fn control_api_enforces_identity_and_cancellation() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use dunnelean::{config::ServerConfig, engine::Engine};
    use tower::ServiceExt;
    let temp = tempfile::tempdir().unwrap();
    let store = Store::open(temp.path().join("state.sqlite")).unwrap();
    let config: ServerConfig = toml::from_str("listen='127.0.0.1:0'\nstate_path='unused'").unwrap();
    let app = dunnelean::api::router(Engine::new(store, &config).unwrap());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .header("x-dunnelean-state-store-id", "wrong-store")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/requests/idempotent-test/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let value: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert!(value["run"].is_null());
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/runs")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&spec()).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
}
