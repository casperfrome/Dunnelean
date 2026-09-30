use super::{Reader, Receipt, Writer};
use crate::{
    config::*,
    engine::Emitter,
    error::{Error, Result, timeout},
    types::{self, TargetColumn},
};
use arrow::{datatypes::SchemaRef, ipc::writer::StreamWriter, record_batch::RecordBatch};
use arrow_flight::{
    FlightEndpoint, FlightInfo, flight_service_client::FlightServiceClient,
    sql::client::FlightSqlServiceClient,
};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

type FlightClient = FlightSqlServiceClient<Channel>;
// Doris transports LARGEINT as a tagged Arrow string. Normalize it to a precise
// numeric Arrow column at the reader boundary, and restore its wire type at the sink.
fn normalize_schema(schema: &arrow::datatypes::Schema) -> SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| {
                if f.metadata()
                    .get("doris_type")
                    .is_some_and(|t| t.eq_ignore_ascii_case("LARGEINT"))
                {
                    f.as_ref()
                        .clone()
                        .with_data_type(arrow::datatypes::DataType::Decimal256(39, 0))
                } else {
                    f.as_ref().clone()
                }
            })
            .collect::<Vec<_>>(),
    ))
}
fn normalize_batch(batch: RecordBatch) -> Result<RecordBatch> {
    use arrow::array::{Array, ArrayRef, Decimal256Array, StringArray};
    let schema = normalize_schema(&batch.schema());
    let arrays = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(a, f)| {
            if a.data_type() == f.data_type() {
                return Ok(a.clone());
            }
            let strings = a
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| types::type_error("Unexpected Doris LARGEINT wire type"))?;
            let values = strings
                .iter()
                .map(|v| v.map(|v| types::decimal_parse(v, 39, 0)).transpose())
                .collect::<Result<Vec<_>>>()?;
            Ok(
                Arc::new(Decimal256Array::from(values).with_precision_and_scale(39, 0)?)
                    as ArrayRef,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, arrays)?)
}
fn physical_batch(batch: &RecordBatch, columns: &[TargetColumn]) -> Result<RecordBatch> {
    use arrow::{
        array::{ArrayRef, Decimal256Array, StringBuilder},
        datatypes::{DataType, Schema},
    };
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for (a, col) in batch.columns().iter().zip(columns) {
        if col.sql_type.eq_ignore_ascii_case("largeint") {
            let values = a
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| types::type_error("LARGEINT requires normalized Decimal256"))?;
            let mut b = StringBuilder::new();
            for value in values.iter() {
                match value {
                    None => b.append_null(),
                    Some(v) => b.append_value(
                        v.to_i128()
                            .ok_or_else(|| types::type_error("LARGEINT i128 overflow"))?
                            .to_string(),
                    ),
                }
            }
            fields.push(col.field.clone().with_data_type(DataType::Utf8));
            arrays.push(Arc::new(b.finish()) as ArrayRef);
        } else {
            fields.push(col.field.clone());
            arrays.push(a.clone());
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}
pub struct DorisSource(pub DorisReader);
impl DorisSource {
    async fn client(&self, uri: &str, token: Option<String>) -> Result<FlightClient> {
        let uri = uri
            .replace("grpc+tcp://", "http://")
            .replace("grpc+tls://", "https://")
            .replace("grpc://", "http://");
        if self.0.tls.enabled != uri.starts_with("https://") {
            return Err(Error::config(
                "Flight URI scheme must agree with tls.enabled",
            ));
        }
        let mut endpoint = Endpoint::from_shared(uri.clone())
            .map_err(|e| Error::config(e.to_string()))?
            .connect_timeout(Duration::from_millis(self.0.timeouts.connect_ms));
        if self.0.tls.enabled {
            let mut tls = ClientTlsConfig::new().with_native_roots();
            if let Some(ca) = &self.0.tls.ca_certificate {
                tls = tls.ca_certificate(Certificate::from_pem(std::fs::read(ca)?));
            }
            if let (Some(cert), Some(key)) =
                (&self.0.tls.client_certificate, &self.0.tls.client_key)
            {
                tls = tls.identity(Identity::from_pem(
                    std::fs::read(cert)?,
                    std::fs::read(key)?,
                ));
            }
            if let Some(name) = &self.0.tls.server_name {
                tls = tls.domain_name(name);
            }
            endpoint = endpoint.tls_config(tls)?;
        }
        let channel = endpoint.connect().await.map_err(|e| {
            Error::new(
                "FLIGHT_TRANSPORT",
                format!("Cannot connect to {uri}: {e:?}"),
            )
        })?;
        let inner = FlightServiceClient::new(channel)
            .max_decoding_message_size(self.0.max_grpc_message_bytes);
        let mut client = FlightClient::new_from_inner(inner);
        if let Some(token) = token {
            client.set_token(token);
        } else {
            timeout(self.0.timeouts.connect_ms, async {
                client
                    .handshake(
                        &self.0.credentials.username,
                        &self.0.credentials.password()?,
                    )
                    .await?;
                Ok(())
            })
            .await?;
        }
        Ok(client)
    }
    async fn endpoint(
        &self,
        endpoint: &FlightEndpoint,
        token: Option<String>,
    ) -> Result<FlightClient> {
        let locations = if endpoint.location.is_empty() {
            vec![self.0.flight_uri.clone()]
        } else {
            endpoint
                .location
                .iter()
                .map(|l| {
                    self.0
                        .endpoint_map
                        .get(&l.uri)
                        .cloned()
                        .unwrap_or_else(|| l.uri.clone())
                })
                .collect()
        };
        let mut error = Error::new("FLIGHT", "No reachable endpoint location");
        for location in locations {
            match self.client(&location, token.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => error = e,
            }
        }
        Err(error)
    }
    async fn drain(&self, info: FlightInfo, token: Option<String>) -> Result<()> {
        for e in &info.endpoint {
            let mut client = self.endpoint(e, token.clone()).await?;
            let mut stream = timeout(self.0.timeouts.read_ms, async {
                Ok(client
                    .do_get(
                        e.ticket
                            .clone()
                            .ok_or_else(|| Error::new("FLIGHT", "Missing endpoint ticket"))?,
                    )
                    .await?)
            })
            .await?;
            while timeout(self.0.timeouts.read_ms, async {
                Ok(stream.try_next().await?)
            })
            .await?
            .is_some()
            {}
        }
        Ok(())
    }
    async fn session(&self) -> Result<FlightClient> {
        let mut client = self.client(&self.0.flight_uri, None).await?;
        let mut queries = vec![format!("USE {}", ident(&self.0.database)?)];
        for (key, value) in &self.0.session_variables {
            queries.push(format!("SET {}={}", ident(key)?, literal(value)));
        }
        for sql in queries {
            let info = timeout(self.0.timeouts.write_ms, async {
                Ok(client.execute(sql, None).await?)
            })
            .await?;
            self.drain(info, client.token().cloned()).await?;
        }
        Ok(client)
    }
}
#[async_trait]
impl Reader for DorisSource {
    async fn schema(&self) -> Result<SchemaRef> {
        let mut client = self.session().await?;
        let sql = self.0.source.sql(&self.0.database, None)?;
        let info = timeout(self.0.timeouts.read_ms, async {
            Ok(client
                .execute(
                    format!("SELECT * FROM ({sql}) AS dunnelean_schema LIMIT 0"),
                    None,
                )
                .await?)
        })
        .await?;
        let schema = normalize_schema(&info.clone().try_decode_schema()?);
        self.drain(info, client.token().cloned()).await?;
        types::check_declared(&schema, &self.0.source.column_types)?;
        Ok(schema)
    }
    async fn read(&self, emitter: Emitter) -> Result<()> {
        let mut client = self.session().await?;
        let sql = self.0.source.sql(&self.0.database, None)?;
        let info = timeout(self.0.timeouts.read_ms, async {
            Ok(client.execute(sql, None).await?)
        })
        .await?;
        let schema = normalize_schema(&info.clone().try_decode_schema()?);
        emitter.check_schema(&schema)?;
        let token = client.token().cloned();
        stream::iter(info.endpoint.into_iter().map(|e|{let emitter=emitter.clone();let token=token.clone();async move {
            let _raw=emitter.reserve_reader(self.0.max_grpc_message_bytes.saturating_mul(2)).await?;
            let mut client=self.endpoint(&e,token).await?;
            let mut stream=timeout(self.0.timeouts.read_ms,async{Ok(client.do_get(e.ticket.ok_or_else(||Error::new("FLIGHT","Missing endpoint ticket"))?).await?)}).await?;
            loop {
                let batch=tokio::select!{biased;_=emitter.cancelled()=>return Err(Error::cancelled()),batch=timeout(self.0.timeouts.read_ms,async{Ok(stream.try_next().await?)})=>batch?};
                let Some(batch)=batch else{break};emitter.check_schema(&normalize_schema(&batch.schema()))?;
                let mut start=0;
                while start<batch.num_rows(){
                    let mut end=start;let mut bytes=0;
                    while end<batch.num_rows() && end-start<self.0.batch.rows {
                        let size=types::logical_row_bytes(&batch,end);
                        if size>emitter.max_row_bytes(){return Err(Error::new("ROW_TOO_LARGE","Flight row exceeds max_row_bytes"));}
                        if end>start && bytes+size>self.0.batch.bytes{break;}
                        bytes+=size;end+=1;
                    }
                    // concat copies this slice so queued chunks do not retain an entire gRPC batch.
                    let chunk=arrow::compute::concat_batches(&batch.schema(),[&batch.slice(start,end-start)])?;
                    emitter.emit(normalize_batch(chunk)?).await?;start=end;
                }
            }
            Ok(())
        }})).buffer_unordered(self.0.endpoint_parallelism).try_collect::<Vec<_>>().await?;
        Ok(())
    }
}

pub struct DorisSink {
    config: DorisWriter,
    columns: Vec<TargetColumn>,
    client: reqwest::Client,
}
impl DorisSink {
    pub async fn probe(&self) -> Result<()> {
        for base in self
            .config
            .fe_http_urls
            .iter()
            .chain(&self.config.be_http_urls)
        {
            let mut url = reqwest::Url::parse(base).map_err(|e| Error::config(e.to_string()))?;
            url.set_path("/api/health");
            let response = self
                .client
                .get(url)
                .basic_auth(
                    &self.config.sql.credentials.username,
                    Some(self.config.sql.credentials.password()?),
                )
                .timeout(Duration::from_millis(self.config.sql.timeouts.read_ms))
                .send()
                .await?;
            if !response.status().is_success() {
                return Err(Error::new(
                    "DORIS",
                    format!("HTTP health probe failed: {}", response.status()),
                ));
            }
            let status: serde_json::Value = response.json().await?;
            if status.get("code").and_then(|v| v.as_i64()) != Some(0)
                && status.get("status").and_then(|v| v.as_str()) != Some("OK")
            {
                return Err(Error::new(
                    "DORIS",
                    "HTTP endpoint did not return a healthy Doris response",
                ));
            }
        }
        Ok(())
    }
    pub fn new(config: DorisWriter, columns: Vec<TargetColumn>) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(config.sql.timeouts.connect_ms))
            .timeout(Duration::from_millis(config.sql.timeouts.write_ms));
        if let Some(path) = &config.http_tls.ca_certificate {
            builder = builder
                .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
        }
        if let (Some(cert), Some(key)) = (
            &config.http_tls.client_certificate,
            &config.http_tls.client_key,
        ) {
            let mut pem = std::fs::read(cert)?;
            pem.extend(std::fs::read(key)?);
            builder = builder.identity(reqwest::Identity::from_pem(&pem)?);
        }
        for url in config.fe_http_urls.iter().chain(&config.be_http_urls) {
            let url = reqwest::Url::parse(url).map_err(|e| Error::config(e.to_string()))?;
            if !matches!(url.scheme(), "http" | "https")
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(Error::config(
                    "HTTP base URLs must use http(s) with credentials in the credentials object, and no query/fragment",
                ));
            }
            if config.http_tls.enabled != (url.scheme() == "https") {
                return Err(Error::config(
                    "HTTP URL scheme must agree with http_tls.enabled",
                ));
            }
        }
        Ok(Self {
            config,
            columns,
            client: builder.build()?,
        })
    }
    fn load_url(&self, base: &str) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(base).map_err(|e| Error::config(e.to_string()))?;
        url.path_segments_mut()
            .map_err(|_| Error::config("Invalid HTTP base URL"))?
            .clear()
            .extend([
                "api",
                &self.config.sql.database,
                &self.config.table,
                "_stream_load",
            ]);
        Ok(url)
    }
    fn request(
        &self,
        url: reqwest::Url,
        payload: bytes::Bytes,
        label: &str,
    ) -> reqwest::RequestBuilder {
        let c = &self.config;
        let mut r = self
            .client
            .put(url)
            .basic_auth(
                &c.sql.credentials.username,
                c.sql.credentials.password().ok(),
            )
            .header("format", "arrow")
            .header("label", label)
            .header("Expect", "100-continue")
            .header("Content-Type", "application/vnd.apache.arrow.stream")
            .header(
                "columns",
                self.columns
                    .iter()
                    .map(|c| c.field.name().as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .header("strict_mode", c.strict_mode.to_string())
            .header("max_filter_ratio", c.max_filter_ratio.to_string())
            .header("timeout", c.load_timeout_seconds.to_string())
            .header("timezone", &c.time_zone)
            .header("exec_mem_limit", c.exec_mem_limit.to_string());
        if !c.partitions.is_empty() {
            r = r.header("partitions", c.partitions.join(","));
        }
        for (key, value) in &c.headers {
            r = r.header(key, value);
        }
        r.body(payload)
    }
    async fn send(
        &self,
        payload: bytes::Bytes,
        label: &str,
        attempt: usize,
    ) -> Result<serde_json::Value> {
        let bases = if self.config.be_http_urls.is_empty() {
            &self.config.fe_http_urls
        } else {
            &self.config.be_http_urls
        };
        let mut url = self.load_url(&bases[attempt % bases.len()])?;
        for _ in 0..5 {
            let response = self
                .request(url.clone(), payload.clone(), label)
                .send()
                .await?;
            if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| Error::new("DORIS", "Missing Stream Load redirect location"))?;
                let redirect = url
                    .join(location)
                    .map_err(|e| Error::new("DORIS", e.to_string()))?;
                let origin = redirect.origin().ascii_serialization();
                if let Some(mapped) = self.config.endpoint_map.get(&origin) {
                    url = self.load_url(mapped)?;
                } else if self
                    .config
                    .be_http_urls
                    .iter()
                    .chain(&self.config.fe_http_urls)
                    .any(|base| {
                        reqwest::Url::parse(base).is_ok_and(|b| b.origin() == redirect.origin())
                    })
                {
                    url = redirect;
                } else {
                    return Err(Error::new(
                        "DORIS",
                        format!("Redirect origin {origin} needs an explicit endpoint_map entry"),
                    ));
                }
                continue;
            }
            if !response.status().is_success() {
                return Err(Error::new(
                    "HTTP",
                    format!("Stream Load HTTP status {}", response.status()),
                ));
            }
            return Ok(response.json().await?);
        }
        Err(Error::new("DORIS", "Too many Stream Load redirects"))
    }
    async fn load_state(&self, label: &str) -> Result<String> {
        for base in &self.config.fe_http_urls {
            let mut url = reqwest::Url::parse(base).map_err(|e| Error::config(e.to_string()))?;
            url.path_segments_mut()
                .map_err(|_| Error::config("Invalid FE HTTP URL"))?
                .clear()
                .extend(["api", &self.config.sql.database, "get_load_state"]);
            let response = self
                .client
                .get(url)
                .query(&[("label", label)])
                .basic_auth(
                    &self.config.sql.credentials.username,
                    Some(self.config.sql.credentials.password()?),
                )
                .send()
                .await;
            if let Ok(r) = response
                && r.status().is_success()
            {
                let Ok(json) = r.json::<serde_json::Value>().await else {
                    continue;
                };
                if json.get("code").and_then(|v| v.as_i64()) != Some(0) {
                    continue;
                }
                // Doris 4.1.4 returns data: "VISIBLE", not data: {state: "VISIBLE"}.
                if let Some(state) = json
                    .get("data")
                    .and_then(|v| v.as_str())
                    .or_else(|| json.pointer("/data/state").and_then(|v| v.as_str()))
                {
                    return Ok(state.into());
                }
            }
        }
        Err(Error::unknown("Could not query Doris label state"))
    }
    async fn settle(&self, label: &str) -> Result<String> {
        let until = Instant::now() + Duration::from_millis(self.config.sql.timeouts.write_ms);
        loop {
            let state = self.load_state(label).await?;
            if matches!(state.as_str(), "VISIBLE" | "ABORTED") {
                return Ok(state);
            }
            if state == "UNKNOWN" || Instant::now() >= until {
                return Err(Error::unknown(format!(
                    "Doris label {label} has state {state}; inspect before rerunning"
                )));
            }
            tokio::time::sleep(Duration::from_millis(
                self.config.options.retry_backoff_ms.max(100),
            ))
            .await;
        }
    }
    fn receipt(&self, rows: usize, response: Option<serde_json::Value>) -> Result<Receipt> {
        let Some(data) = response else {
            if self.config.max_filter_ratio > 0.0 {
                return Err(Error::unknown(
                    "Doris commit is visible but loaded/filtered counters are unavailable",
                ));
            }
            return Ok(Receipt {
                loaded: rows as u64,
                filtered: 0,
                affected: rows as u64,
                detail: serde_json::json!({"state":"VISIBLE","reconciled":true}),
            });
        };
        let number = |k: &str| {
            data.get(k).and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|v| v.parse().ok()))
            })
        };
        let loaded = number("NumberLoadedRows")
            .ok_or_else(|| Error::unknown("Missing Doris loaded row count"))?;
        let filtered = number("NumberFilteredRows").unwrap_or(0);
        let unselected = number("NumberUnselectedRows").unwrap_or(0);
        if loaded + filtered + unselected != rows as u64 {
            return Err(Error::unknown(
                "Doris row counts disagree with submitted payload",
            ));
        }
        if filtered > 0 && self.config.max_filter_ratio == 0.0 {
            return Err(Error::unknown(
                "Unexpected filtered rows in strict zero-filter import",
            ));
        }
        Ok(Receipt {
            loaded,
            filtered: filtered + unselected,
            affected: loaded,
            detail: data,
        })
    }
}
#[async_trait]
impl Writer for DorisSink {
    async fn write(&mut self, batch: &RecordBatch, label: &str) -> Result<Receipt> {
        let physical = physical_batch(batch, &self.columns)?;
        let mut payload = Vec::new();
        {
            let mut stream = StreamWriter::try_new(&mut payload, &physical.schema())?;
            stream.write(&physical)?;
            stream.finish()?;
        }
        let payload = bytes::Bytes::from(payload);
        for attempt in 0..=self.config.options.retry_attempts {
            let response = self.send(payload.clone(), label, attempt).await;
            match response {
                Ok(data) => match data.get("Status").and_then(|v| v.as_str()).unwrap_or("") {
                    "Success" => return self.receipt(batch.num_rows(), Some(data)),
                    "Publish Timeout" => {
                        if self.settle(label).await? == "VISIBLE" {
                            return self.receipt(batch.num_rows(), Some(data));
                        }
                    }
                    "Label Already Exists" => {
                        if self.settle(label).await? == "VISIBLE" {
                            return self.receipt(batch.num_rows(), None);
                        }
                    }
                    "Fail" => {
                        return Err(Error::new(
                            "DORIS_LOAD",
                            data.get("Message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Stream Load failed"),
                        ));
                    }
                    _ => return Err(Error::unknown("Unrecognized Doris Stream Load response")),
                },
                Err(_) => {
                    if self.settle(label).await? == "VISIBLE" {
                        return self.receipt(batch.num_rows(), None);
                    }
                }
            }
            // Only a confirmed ABORTED transaction reaches this retry path.
            if attempt < self.config.options.retry_attempts {
                tokio::time::sleep(Duration::from_millis(
                    self.config
                        .options
                        .retry_backoff_ms
                        .saturating_mul(1u64 << attempt.min(10)),
                ))
                .await;
            }
        }
        Err(Error::new(
            "DORIS_LOAD",
            "Doris import aborted after bounded retries",
        ))
    }
}
