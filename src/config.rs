use crate::error::{Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

fn yes() -> bool {
    true
}
fn one() -> usize {
    1
}
fn connect_ms() -> u64 {
    10_000
}
fn read_ms() -> u64 {
    60_000
}
fn write_ms() -> u64 {
    120_000
}
fn batch_rows() -> usize {
    10_000
}
fn batch_bytes() -> usize {
    16 * 1024 * 1024
}
fn packet_bytes() -> usize {
    64 * 1024 * 1024
}
fn memory_bytes() -> usize {
    256 * 1024 * 1024
}
fn utc() -> String {
    "+00:00".into()
}
fn charset() -> String {
    "utf8mb4".into()
}
fn mysql_port() -> u16 {
    3306
}
fn localhost() -> String {
    "127.0.0.1".into()
}
fn hour_ms() -> u64 {
    3_600_000
}
fn four() -> usize {
    4
}
fn backoff_ms() -> u64 {
    250
}
fn two() -> usize {
    2
}
fn load_seconds() -> u64 {
    120
}
fn label_prefix() -> String {
    "dunnelean".into()
}
fn exec_memory() -> u64 {
    2 * 1024 * 1024 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub username: String,
    pub password: Option<String>,
    pub password_env: Option<String>,
}
impl Credentials {
    pub fn password(&self) -> Result<String> {
        match (&self.password, &self.password_env) {
            (Some(p), None) => Ok(p.clone()),
            (None, Some(env)) => std::env::var(env).map_err(|_| {
                Error::config(format!("Password environment variable {env} is not set"))
            }),
            _ => Err(Error::config(
                "Specify exactly one of password or password_env (empty password is explicit)",
            )),
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    #[serde(default)]
    pub enabled: bool,
    pub ca_certificate: Option<String>,
    pub client_certificate: Option<String>,
    pub client_key: Option<String>,
    pub server_name: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Timeouts {
    pub connect_ms: u64,
    pub read_ms: u64,
    pub write_ms: u64,
}
impl Default for Timeouts {
    fn default() -> Self {
        Self {
            connect_ms: connect_ms(),
            read_ms: read_ms(),
            write_ms: write_ms(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MysqlConnection {
    #[serde(default = "localhost")]
    pub host: String,
    #[serde(default = "mysql_port")]
    pub port: u16,
    pub database: String,
    pub credentials: Credentials,
    #[serde(default)]
    pub tls: Tls,
    #[serde(default)]
    pub timeouts: Timeouts,
    #[serde(default = "charset")]
    pub charset: String,
    #[serde(default = "utc")]
    pub time_zone: String,
    #[serde(default)]
    pub session_variables: BTreeMap<String, String>,
    #[serde(default = "packet_bytes")]
    pub max_allowed_packet_bytes: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Batch {
    pub rows: usize,
    pub bytes: usize,
}
impl Default for Batch {
    fn default() -> Self {
        Self {
            rows: batch_rows(),
            bytes: batch_bytes(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Parameter {
    Null,
    String(String),
    I64(String),
    U64(String),
    Decimal(String),
    F64(f64),
    Bool(bool),
    Binary(String),
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub table: Option<String>,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(rename = "where")]
    pub predicate: Option<String>,
    pub query: Option<String>,
    #[serde(default)]
    pub params: Vec<Parameter>,
    /// Optional expected Arrow types keyed by output column name; mismatches fail without coercion.
    #[serde(default)]
    pub column_types: BTreeMap<String, String>,
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    #[default]
    Snapshot,
    Statement,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Split {
    pub column: String,
    /// Integer boundaries are strings to preserve unsigned 64-bit values in Java/JSON.
    pub lower_bound: String,
    pub upper_bound: String,
    pub partitions: usize,
    #[serde(default = "one")]
    pub parallelism: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MysqlReader {
    pub connection: MysqlConnection,
    pub source: Source,
    #[serde(default)]
    pub batch: Batch,
    #[serde(default)]
    pub consistency: Consistency,
    pub split: Option<Split>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DorisReader {
    pub flight_uri: String,
    pub database: String,
    pub credentials: Credentials,
    #[serde(default)]
    pub tls: Tls,
    #[serde(default)]
    pub timeouts: Timeouts,
    #[serde(default)]
    pub session_variables: BTreeMap<String, String>,
    #[serde(default)]
    pub endpoint_map: BTreeMap<String, String>,
    pub source: Source,
    #[serde(default)]
    pub batch: Batch,
    #[serde(default = "one")]
    pub endpoint_parallelism: usize,
    #[serde(default = "packet_bytes")]
    pub max_grpc_message_bytes: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReaderConfig {
    Mysql(MysqlReader),
    Doris(DorisReader),
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MysqlWriteMode {
    #[default]
    Insert,
    Upsert,
}
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DorisWriteMode {
    #[default]
    Append,
    Upsert,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WriterOptions {
    pub batch: Batch,
    pub parallelism: usize,
    pub retry_attempts: usize,
    pub retry_backoff_ms: u64,
    pub pre_sql: Vec<String>,
    pub post_sql: Vec<String>,
}
impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            batch: Batch::default(),
            parallelism: 1,
            retry_attempts: two(),
            retry_backoff_ms: backoff_ms(),
            pre_sql: vec![],
            post_sql: vec![],
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MysqlWriter {
    pub connection: MysqlConnection,
    pub table: String,
    #[serde(default)]
    pub mode: MysqlWriteMode,
    #[serde(default)]
    pub key_columns: Vec<String>,
    #[serde(default)]
    pub update_columns: Vec<String>,
    #[serde(default)]
    pub options: WriterOptions,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DorisWriter {
    pub sql: MysqlConnection,
    pub fe_http_urls: Vec<String>,
    #[serde(default)]
    pub be_http_urls: Vec<String>,
    #[serde(default)]
    pub endpoint_map: BTreeMap<String, String>,
    #[serde(default)]
    pub http_tls: Tls,
    pub table: String,
    #[serde(default)]
    pub mode: DorisWriteMode,
    #[serde(default)]
    pub options: WriterOptions,
    #[serde(default = "label_prefix")]
    pub label_prefix: String,
    #[serde(default = "load_seconds")]
    pub load_timeout_seconds: u64,
    #[serde(default = "yes")]
    pub strict_mode: bool,
    #[serde(default)]
    pub max_filter_ratio: f64,
    #[serde(default = "utc")]
    pub time_zone: String,
    #[serde(default)]
    pub partitions: Vec<String>,
    #[serde(default = "exec_memory")]
    pub exec_mem_limit: u64,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WriterConfig {
    Mysql(Box<MysqlWriter>),
    Doris(Box<DorisWriter>),
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub source: String,
    pub target: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Execution {
    pub timeout_ms: u64,
    pub queue_capacity: usize,
    pub memory_bytes: usize,
    pub max_row_bytes: usize,
    pub rows_per_second: Option<u64>,
    pub bytes_per_second: Option<u64>,
}
impl Default for Execution {
    fn default() -> Self {
        Self {
            timeout_ms: hour_ms(),
            queue_capacity: four(),
            memory_bytes: memory_bytes(),
            max_row_bytes: 24 * 1024 * 1024,
            rows_per_second: None,
            bytes_per_second: None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunSpec {
    pub request_id: Option<String>,
    pub reader: ReaderConfig,
    pub writer: WriterConfig,
    #[serde(default)]
    pub mapping: Vec<Mapping>,
    #[serde(default)]
    pub execution: Execution,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: String,
    pub state_path: String,
    pub max_running: usize,
    pub max_queued: usize,
    pub shutdown_timeout_ms: u64,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:9876".into(),
            state_path: "var/dunnelean.sqlite".into(),
            max_running: 2,
            max_queued: 16,
            shutdown_timeout_ms: 60_000,
        }
    }
}
impl ReaderConfig {
    pub fn source(&self) -> &Source {
        match self {
            Self::Mysql(r) => &r.source,
            Self::Doris(r) => &r.source,
        }
    }
    pub fn batch(&self) -> &Batch {
        match self {
            Self::Mysql(r) => &r.batch,
            Self::Doris(r) => &r.batch,
        }
    }
    pub fn lanes(&self) -> usize {
        match self {
            Self::Mysql(r) => r.split.as_ref().map_or(1, |s| s.parallelism),
            Self::Doris(r) => r.endpoint_parallelism,
        }
    }
}
impl WriterConfig {
    pub fn options(&self) -> &WriterOptions {
        match self {
            Self::Mysql(w) => &w.options,
            Self::Doris(w) => &w.options,
        }
    }
    pub fn sql_connection(&self) -> &MysqlConnection {
        match self {
            Self::Mysql(w) => &w.connection,
            Self::Doris(w) => &w.sql,
        }
    }
    pub fn table(&self) -> &str {
        match self {
            Self::Mysql(w) => &w.table,
            Self::Doris(w) => &w.table,
        }
    }
}
pub fn ident(name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        return Err(Error::config("Empty/NUL identifier"));
    }
    Ok(format!("`{}`", name.replace('`', "``")))
}
pub fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}
pub fn offset_seconds(value: &str) -> Result<i32> {
    let b = value.as_bytes();
    if b.len() != 6
        || !matches!(b[0], b'+' | b'-')
        || b[3] != b':'
        || ![1, 2, 4, 5].iter().all(|&i| b[i].is_ascii_digit())
    {
        return Err(Error::config("time_zone must be a fixed offset ±HH:MM"));
    }
    let h = value[1..3]
        .parse::<i32>()
        .map_err(|_| Error::config("Invalid time_zone"))?;
    let m = value[4..6]
        .parse::<i32>()
        .map_err(|_| Error::config("Invalid time_zone"))?;
    if h > 14 || m > 59 || h == 14 && m != 0 {
        return Err(Error::config("Invalid time_zone"));
    }
    Ok((h * 3600 + m * 60) * if b[0] == b'-' { -1 } else { 1 })
}
impl Source {
    pub fn sql(&self, database: &str, extra: Option<&str>) -> Result<String> {
        if let Some(query) = &self.query {
            return Ok(query.trim().trim_end_matches(';').to_owned());
        }
        let columns = if self.columns.is_empty() {
            "*".into()
        } else {
            self.columns
                .iter()
                .map(|s| ident(s))
                .collect::<Result<Vec<_>>>()?
                .join(",")
        };
        let mut predicates = Vec::new();
        if let Some(p) = &self.predicate {
            predicates.push(format!("({p})"));
        }
        if let Some(p) = extra {
            predicates.push(format!("({p})"));
        }
        Ok(format!(
            "SELECT {columns} FROM {}.{}{}",
            ident(database)?,
            ident(
                self.table
                    .as_deref()
                    .ok_or_else(|| Error::config("Missing source table"))?
            )?,
            if predicates.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", predicates.join(" AND "))
            }
        ))
    }
}
impl Split {
    pub fn predicates(&self) -> Result<Vec<String>> {
        let lower = self
            .lower_bound
            .parse::<i128>()
            .map_err(|_| Error::config("Invalid lower_bound integer"))?;
        let upper = self
            .upper_bound
            .parse::<i128>()
            .map_err(|_| Error::config("Invalid upper_bound integer"))?;
        if lower >= upper
            || lower < i64::MIN as i128
            || upper > u64::MAX as i128
            || !(1..=1024).contains(&self.partitions)
        {
            return Err(Error::config("Invalid split bounds/partitions"));
        }
        let col = ident(&self.column)?;
        let cuts = (1..self.partitions)
            .map(|i| lower + (upper - lower) * i as i128 / self.partitions as i128)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if cuts.is_empty() {
            return Ok(vec!["1=1".into()]);
        }
        let mut result = vec![format!("{col} < {} OR {col} IS NULL", cuts[0])];
        for c in cuts.windows(2) {
            result.push(format!("{col} >= {} AND {col} < {}", c[0], c[1]));
        }
        result.push(format!("{col} >= {}", cuts[cuts.len() - 1]));
        Ok(result)
    }
}
fn check_tls(t: &Tls) -> Result<()> {
    if t.client_certificate.is_some() != t.client_key.is_some() {
        return Err(Error::config(
            "Both client_certificate and client_key are required for mTLS",
        ));
    }
    if !t.enabled
        && (t.ca_certificate.is_some() || t.client_key.is_some() || t.server_name.is_some())
    {
        return Err(Error::config("TLS options require enabled=true"));
    }
    Ok(())
}
fn check_connection(c: &MysqlConnection) -> Result<()> {
    c.credentials.password()?;
    check_tls(&c.tls)?;
    offset_seconds(&c.time_zone)?;
    if c.host.is_empty()
        || c.database.is_empty()
        || c.port == 0
        || c.max_allowed_packet_bytes < 4096
    {
        return Err(Error::config("Invalid MySQL connection"));
    }
    if c.tls.server_name.is_some() {
        return Err(Error::config(
            "MySQL TLS uses host for certificate identity; server_name applies only to Flight",
        ));
    }
    if c.charset != "utf8mb4" {
        return Err(Error::config(
            "v1 requires charset=utf8mb4; server transcodes textual columns, binary columns remain binary",
        ));
    }
    if c.timeouts.connect_ms == 0 || c.timeouts.read_ms == 0 || c.timeouts.write_ms == 0 {
        return Err(Error::config("Timeouts must be positive"));
    }
    for key in c.session_variables.keys() {
        if !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            || [
                "time_zone",
                "character_set_results",
                "character_set_client",
                "character_set_connection",
                "autocommit",
                "sql_mode",
            ]
            .contains(&key.to_ascii_lowercase().as_str())
        {
            return Err(Error::config(format!(
                "Reserved/invalid session variable {key}"
            )));
        }
    }
    Ok(())
}
impl RunSpec {
    pub fn validate(&self) -> Result<()> {
        match (&self.reader, &self.writer) {
            (ReaderConfig::Mysql(_), WriterConfig::Doris(_))
            | (ReaderConfig::Doris(_), WriterConfig::Mysql(_)) => {}
            _ => {
                return Err(Error::config(
                    "Only MySQL → Doris and Doris → MySQL are supported",
                ));
            }
        }
        let s = self.reader.source();
        if s.query.is_some() == s.table.is_some()
            || s.query.is_some() && (!s.columns.is_empty() || s.predicate.is_some())
        {
            return Err(Error::config("Choose query or table/columns/where"));
        }
        if let Some(q) = &s.query {
            let q = q.trim().to_ascii_lowercase();
            if !(q.starts_with("select ")
                || q.starts_with("with ")
                || q.starts_with("select\n")
                || q.starts_with("with\n"))
            {
                return Err(Error::config("Reader query must be a SELECT/WITH query"));
            }
        }
        let ex = &self.execution;
        if !(1..=65536).contains(&ex.queue_capacity)
            || ex.timeout_ms == 0
            || ex.memory_bytes > u32::MAX as usize
            || ex.memory_bytes < 1024 * 1024
            || ex.max_row_bytes == 0
            || ex.rows_per_second == Some(0)
            || ex.bytes_per_second == Some(0)
        {
            return Err(Error::config("Invalid execution limits"));
        }
        for batch in [self.reader.batch(), &self.writer.options().batch] {
            if batch.rows == 0 || batch.bytes == 0 || batch.bytes > ex.memory_bytes / 4 {
                return Err(Error::config(
                    "Batch limits must be positive and bytes <= memory_bytes/4",
                ));
            }
        }
        if ex.max_row_bytes > ex.memory_bytes / 4 {
            return Err(Error::config("max_row_bytes must be <= memory_bytes/4"));
        }
        if !(1..=32).contains(&self.reader.lanes())
            || !(1..=32).contains(&self.writer.options().parallelism)
        {
            return Err(Error::config("Parallelism must be 1..32"));
        }
        if self.writer.options().retry_attempts > 16 {
            return Err(Error::config("retry_attempts must be 0..16"));
        }
        check_connection(self.writer.sql_connection())?;
        match &self.reader {
            ReaderConfig::Mysql(r) => {
                check_connection(&r.connection)?;
                if let Some(split) = &r.split {
                    if r.source.query.is_some() {
                        return Err(Error::config("Range splitting requires table mode"));
                    }
                    split.predicates()?;
                }
            }
            ReaderConfig::Doris(r) => {
                r.credentials.password()?;
                check_tls(&r.tls)?;
                if r.database.is_empty()
                    || r.timeouts.connect_ms == 0
                    || r.timeouts.read_ms == 0
                    || r.timeouts.write_ms == 0
                {
                    return Err(Error::config(
                        "Doris database and positive timeouts are required",
                    ));
                }
                if !r.source.params.is_empty() {
                    return Err(Error::config(
                        "Doris Flight SQL requires complete SQL; bound params are unsupported",
                    ));
                }
                if r.max_grpc_message_bytes == 0 || r.max_grpc_message_bytes > ex.memory_bytes / 4 {
                    return Err(Error::config(
                        "max_grpc_message_bytes must be 1..memory_bytes/4 (two decoding buffers in the reader half)",
                    ));
                }
            }
        }
        match &self.writer {
            WriterConfig::Mysql(w) => {
                if w.mode == MysqlWriteMode::Upsert && w.key_columns.is_empty() {
                    return Err(Error::config("upsert requires key_columns"));
                }
                if w.mode == MysqlWriteMode::Insert
                    && (!w.key_columns.is_empty() || !w.update_columns.is_empty())
                {
                    return Err(Error::config("key_columns/update_columns require upsert"));
                }
            }
            WriterConfig::Doris(w) => {
                if w.fe_http_urls.is_empty()
                    || w.load_timeout_seconds == 0
                    || !(0.0..=1.0).contains(&w.max_filter_ratio)
                {
                    return Err(Error::config("Invalid Stream Load options"));
                }
                offset_seconds(&w.time_zone)?;
                check_tls(&w.http_tls)?;
                if w.http_tls.server_name.is_some() {
                    return Err(Error::config(
                        "HTTP TLS uses URL host for certificate identity; server_name applies only to Flight",
                    ));
                }
                if w.label_prefix.len() > 40
                    || w.label_prefix.is_empty()
                    || !w
                        .label_prefix
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    return Err(Error::config(
                        "label_prefix must be 1..40 ASCII letters/digits/underscore",
                    ));
                }
                for (key, value) in &w.headers {
                    reqwest::header::HeaderName::from_bytes(key.as_bytes())
                        .map_err(|_| Error::config("Invalid Stream Load header name"))?;
                    reqwest::header::HeaderValue::from_str(value)
                        .map_err(|_| Error::config("Invalid Stream Load header value"))?;
                    let key = key.to_ascii_lowercase();
                    if [
                        "format",
                        "label",
                        "authorization",
                        "host",
                        "content-length",
                        "content-type",
                        "transfer-encoding",
                        "expect",
                        "columns",
                        "strict_mode",
                        "max_filter_ratio",
                        "timeout",
                        "timezone",
                        "partitions",
                        "exec_mem_limit",
                        "group_commit",
                        "two_phase_commit",
                        "partial_columns",
                        "merge_type",
                    ]
                    .contains(&key.as_str())
                    {
                        return Err(Error::config(format!("Reserved Stream Load header {key}")));
                    }
                }
            }
        }
        let mut names = BTreeSet::new();
        for m in &self.mapping {
            ident(&m.source)?;
            ident(&m.target)?;
            if !names.insert(&m.target) {
                return Err(Error::config("Duplicate target mapping"));
            }
        }
        if let Some(id) = &self.request_id
            && (id.is_empty() || id.len() > 200)
        {
            return Err(Error::config("request_id must be 1..200 bytes"));
        }
        Ok(())
    }
    pub fn redacted(&self) -> serde_json::Value {
        fn redact(v: &mut serde_json::Value) {
            match v {
                serde_json::Value::Object(map) => {
                    for (k, v) in map {
                        if k == "password" {
                            *v = serde_json::Value::String("[REDACTED]".into());
                        } else {
                            redact(v)
                        }
                    }
                }
                serde_json::Value::Array(a) => {
                    for v in a {
                        redact(v)
                    }
                }
                _ => {}
            }
        }
        let mut v = serde_json::to_value(self).expect("serializable config");
        redact(&mut v);
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_covers_null_and_tails() {
        let s = Split {
            column: "id".into(),
            lower_bound: "0".into(),
            upper_bound: "100".into(),
            partitions: 4,
            parallelism: 2,
        };
        assert_eq!(
            s.predicates().unwrap(),
            vec![
                "`id` < 25 OR `id` IS NULL",
                "`id` >= 25 AND `id` < 50",
                "`id` >= 50 AND `id` < 75",
                "`id` >= 75"
            ]
        );
    }
    #[test]
    fn offsets() {
        assert_eq!(offset_seconds("+08:00").unwrap(), 28800);
        assert!(offset_seconds("Asia/Shanghai").is_err());
        assert!(offset_seconds("+14:01").is_err());
        assert!(offset_seconds("+中:1").is_err());
        assert!(offset_seconds("+-1:00").is_err());
    }
    #[test]
    fn unknown_fields_fail() {
        assert!(serde_json::from_str::<Batch>(r#"{"rows":1,"fetch_size":3}"#).is_err());
    }
}
