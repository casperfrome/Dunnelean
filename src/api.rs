use crate::{
    config::RunSpec,
    engine::{self, Engine},
    error::{Error, Result},
    store::Run,
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};

pub fn router(engine: Engine) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/v1/connectors", get(connectors))
        .route("/v1/validate", post(validate))
        .route("/v1/runs", post(submit).get(list))
        .route("/v1/runs/{id}", get(run))
        .route("/v1/runs/{id}/cancel", post(cancel))
        .route("/v1/runs/{id}/batches", get(batches))
        .route("/openapi.json", get(|| async { Json(openapi()) }))
        .route("/v1/requests/{id}", get(request_status))
        .route("/v1/requests/{id}/cancel", post(cancel_request))
        .layer(axum::middleware::from_fn_with_state(
            engine.clone(),
            check_store,
        ))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(engine)
}
async fn check_store(
    State(engine): State<Engine>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<axum::response::Response> {
    if let Some(expected) = request.headers().get("x-dunnelean-state-store-id")
        && expected.to_str().ok() != Some(engine.store.state_store_id()?.as_str())
    {
        return Err(Error::new(
            "STATE_STORE_CHANGED",
            "Dunnelean state store identity changed",
        ));
    }
    Ok(next.run(request).await)
}
async fn health(State(engine): State<Engine>) -> Result<Json<Value>> {
    Ok(Json(
        json!({"status":"ok","service":"Dunnelean","version":env!("CARGO_PKG_VERSION"),"state_store_id":engine.store.state_store_id()?}),
    ))
}
async fn request_status(
    State(engine): State<Engine>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    Ok(Json(engine.store.request(&id)?))
}
async fn cancel_request(
    State(engine): State<Engine>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    Ok(Json(engine.cancel_request(&id).await?))
}
async fn ready(State(engine): State<Engine>) -> Result<Json<Value>> {
    engine.ready()?;
    Ok(Json(json!({"status":"ready"})))
}
pub fn connector_info() -> Value {
    json!({
        "connectors":[
            {"type":"mysql","reader":{"protocol":"binary prepared streaming","consistency":["snapshot","statement"],"range_split":"integer only; includes NULL and both tails"},"writer":{"modes":["insert","upsert"],"transaction":"one InnoDB transaction per batch"}},
            {"type":"doris","version":"4.1.4","reader":{"protocol":"Arrow Flight SQL","prepared_parameters":false,"experimental":true},"writer":{"protocol":"Arrow IPC Stream Load","modes":["append","upsert"],"models":["DUPLICATE KEY","UNIQUE KEY merge-on-write"]}}
        ],"request_schema":schemars::schema_for!(RunSpec),
        "limits":{"unknown_fields":"rejected","time_zone":"fixed ±HH:MM, default +00:00","memory":"split between reader buffers and queued conversion/encoding buffers; not an OS RSS cap","ordering":"parallel readers/writers do not guarantee row order","resume":false,"schema_conversion":"only exact scalar conversions; unsupported types require explicit source SQL"}
    })
}
async fn connectors() -> Json<Value> {
    Json(connector_info())
}
async fn validate(
    payload: std::result::Result<Json<RunSpec>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>> {
    let Json(spec) = payload.map_err(|e| Error::config(e.body_text()))?;
    Ok(Json(engine::validation_json(
        &engine::validate(&spec).await?,
    )))
}
async fn submit(
    State(engine): State<Engine>,
    payload: std::result::Result<Json<RunSpec>, axum::extract::rejection::JsonRejection>,
) -> Result<(StatusCode, Json<Run>)> {
    let Json(spec) = payload.map_err(|e| Error::config(e.body_text()))?;
    Ok((StatusCode::ACCEPTED, Json(engine.submit(spec).await?)))
}
#[derive(Deserialize)]
struct Pagination {
    limit: Option<usize>,
    offset: Option<usize>,
}
async fn list(State(engine): State<Engine>, Query(p): Query<Pagination>) -> Result<Json<Value>> {
    Ok(Json(
        json!({"runs":engine.store.list(p.limit.unwrap_or(50).min(500),p.offset.unwrap_or(0))?}),
    ))
}
async fn run(State(engine): State<Engine>, Path(id): Path<String>) -> Result<Json<Run>> {
    Ok(Json(engine.store.get(&id)?))
}
async fn cancel(State(engine): State<Engine>, Path(id): Path<String>) -> Result<Json<Run>> {
    Ok(Json(engine.cancel(&id).await?))
}
async fn batches(State(engine): State<Engine>, Path(id): Path<String>) -> Result<Json<Value>> {
    engine.store.get(&id)?;
    Ok(Json(json!({"batches":engine.store.batches(&id)?})))
}
pub fn openapi() -> Value {
    fn relocate(v: &mut Value) {
        match v {
            Value::Object(m) => {
                for (k, v) in m {
                    if k == "$ref" {
                        if let Some(s) = v.as_str() {
                            *v = Value::String(s.replace("#/$defs/", "#/components/schemas/"));
                        }
                    } else {
                        relocate(v)
                    }
                }
            }
            Value::Array(a) => {
                for v in a {
                    relocate(v)
                }
            }
            _ => {}
        }
    }
    let mut schemas = serde_json::Map::new();
    for (name, mut schema) in [
        (
            "RunSpec",
            serde_json::to_value(schemars::schema_for!(RunSpec)).unwrap(),
        ),
        (
            "Run",
            serde_json::to_value(schemars::schema_for!(Run)).unwrap(),
        ),
    ] {
        relocate(&mut schema);
        if let Some(defs) = schema.as_object_mut().unwrap().remove("$defs") {
            schemas.extend(defs.as_object().unwrap().clone());
        }
        schema.as_object_mut().unwrap().remove("$schema");
        schemas.insert(name.into(), schema);
    }
    schemas.insert("Health".into(), json!({"type":"object","required":["status","service","version"],"properties":{"status":{"const":"ok"},"service":{"const":"Dunnelean"},"version":{"type":"string"},"state_store_id":{"type":"string"}}}));
    schemas.insert(
        "Ready".into(),
        json!({"type":"object","required":["status"],"properties":{"status":{"const":"ready"}}}),
    );
    schemas.insert("Field".into(), json!({"type":"object","required":["name","type","nullable"],"properties":{"name":{"type":"string"},"type":{"type":"string"},"nullable":{"type":"boolean"}}}));
    schemas.insert("Validation".into(), json!({"type":"object","required":["valid","source_schema","target_schema","semantics"],"properties":{"valid":{"const":true},"source_schema":{"type":"array","items":{"$ref":"#/components/schemas/Field"}},"target_schema":{"type":"array","items":{"$ref":"#/components/schemas/Field"}},"semantics":{"type":"string"}}}));
    schemas.insert("Connectors".into(), json!({"type":"object","required":["connectors","request_schema","limits"],"properties":{"connectors":{"type":"array","items":{"type":"object"}},"request_schema":{"type":"object","description":"JSON Schema 2020-12 with defaults and connector-specific fields"},"limits":{"type":"object"}}}));
    schemas.insert("BatchList".into(), json!({"type":"object","required":["batches"],"properties":{"batches":{"type":"array","items":{"type":"object","required":["batch_id","state","rows","bytes","label","detail"],"properties":{"batch_id":{"type":"integer"},"state":{"enum":["INTENT","CONFIRMED","NOT_COMMITTED","UNKNOWN"]},"rows":{"type":"integer"},"bytes":{"type":"integer"},"label":{"type":["string","null"]},"detail":{"type":["string","null"],"description":"Serialized database receipt or error; no batch data"}}}}}}));
    schemas.insert("RequestStatus".into(),json!({"type":"object","required":["request_id","cancel_requested","run"],"properties":{"request_id":{"type":"string"},"cancel_requested":{"type":"boolean"},"run":{"anyOf":[{"$ref":"#/components/schemas/Run"},{"type":"null"}]}}}));
    let response = |name: &str| json!({"description":"Response","content":{"application/json":{"schema":{"$ref":format!("#/components/schemas/{name}")}}}});
    let errors = json!({"description":"Error","content":{"application/json":{"schema":{"type":"object","required":["error"],"properties":{"error":{"$ref":"#/components/schemas/Error"}}}}}});
    let request = json!({"required":true,"content":{"application/json":{"schema":{"$ref":"#/components/schemas/RunSpec"}}}});
    let id = json!([{"name":"id","in":"path","required":true,"schema":{"type":"string"}}]);
    json!({"openapi":"3.1.0","info":{"title":"Dunnelean Local API","version":env!("CARGO_PKG_VERSION"),"description":"Arrow-native offline MySQL ↔ Doris. Batch commits; cancellation does not undo committed batches."},"servers":[{"url":"http://127.0.0.1:9876"}],"components":{"schemas":schemas},"paths":{
        "/openapi.json":{"get":{"responses":{"200":{"description":"This OpenAPI 3.1 document","content":{"application/json":{"schema":{"type":"object"}}}}}}},
        "/healthz":{"get":{"responses":{"200":response("Health")}}},
        "/readyz":{"get":{"responses":{"200":response("Ready"),"500":errors}}},
        "/v1/connectors":{"get":{"responses":{"200":response("Connectors")}}},
        "/v1/validate":{"post":{"requestBody":request,"responses":{"200":response("Validation"),"400":errors,"500":errors}}},
        "/v1/runs":{"post":{"requestBody":request,"responses":{"202":response("Run"),"400":errors,"409":errors,"429":errors,"500":errors}},"get":{"parameters":[{"name":"limit","in":"query","schema":{"type":"integer","default":50,"maximum":500}},{"name":"offset","in":"query","schema":{"type":"integer","default":0}}],"responses":{"200":{"description":"Run list","content":{"application/json":{"schema":{"type":"object","properties":{"runs":{"type":"array","items":{"$ref":"#/components/schemas/Run"}}}}}}}}}},
        "/v1/requests/{id}":{"get":{"parameters":id,"responses":{"200":response("RequestStatus"),"404":errors}}},
        "/v1/requests/{id}/cancel":{"post":{"parameters":id,"responses":{"200":response("RequestStatus"),"409":errors}}},
        "/v1/runs/{id}":{"get":{"parameters":id,"responses":{"200":response("Run"),"404":errors}}},
        "/v1/runs/{id}/cancel":{"post":{"parameters":id,"responses":{"200":response("Run"),"404":errors}}},
        "/v1/runs/{id}/batches":{"get":{"parameters":id,"responses":{"200":response("BatchList"),"404":errors}}}
    }})
}
