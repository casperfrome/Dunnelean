use super::{Reader, Receipt, Writer};
use crate::{
    config::*,
    engine::Emitter,
    error::{Error, Result, timeout},
    types::{self, MysqlBatchBuilder, TargetColumn},
};
use arrow::{
    datatypes::{Field, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use base64::Engine;
use futures::{StreamExt, TryStreamExt, stream};
use mysql_async::{Conn, OptsBuilder, Params, Row, SslOpts, Value, prelude::Queryable};
use std::{path::PathBuf, time::Duration};

pub async fn connect(c: &MysqlConnection, doris: bool) -> Result<Conn> {
    let mut opts = OptsBuilder::default()
        .ip_or_hostname(&c.host)
        .tcp_port(c.port)
        .db_name(Some(&c.database))
        .user(Some(&c.credentials.username))
        .pass(Some(c.credentials.password()?))
        .prefer_socket(false)
        .max_allowed_packet(Some(c.max_allowed_packet_bytes));
    if c.tls.enabled {
        let mut ssl = SslOpts::default();
        if let Some(path) = &c.tls.ca_certificate {
            ssl = ssl.with_root_certs(vec![std::fs::read(path)?.into()]);
        }
        if let (Some(cert), Some(key)) = (&c.tls.client_certificate, &c.tls.client_key) {
            ssl = ssl.with_client_identity(Some(mysql_async::ClientIdentity::new(
                PathBuf::from(cert).into(),
                PathBuf::from(key).into(),
            )));
        }
        opts = opts.ssl_opts(ssl);
    }
    let mut conn = timeout(c.timeouts.connect_ms, async { Ok(Conn::new(opts).await?) }).await?;
    timeout(c.timeouts.write_ms, async {
        conn.query_drop(format!("SET NAMES {}", c.charset)).await?;
        conn.query_drop(format!("SET time_zone={}", literal(&c.time_zone)))
            .await?;
        if !doris {
            conn.query_drop("SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'")
                .await?;
            conn.query_drop("SET autocommit=1").await?;
        }
        for (key, value) in &c.session_variables {
            conn.query_drop(format!("SET {}={}", ident(key)?, literal(value)))
                .await?;
        }
        Ok(())
    })
    .await?;
    Ok(conn)
}
pub fn parameters(params: &[Parameter]) -> Result<Params> {
    Ok(Params::Positional(
        params
            .iter()
            .map(|p| {
                Ok(match p {
                    Parameter::Null => Value::NULL,
                    Parameter::String(s) | Parameter::Decimal(s) => {
                        Value::Bytes(s.as_bytes().to_vec())
                    }
                    Parameter::I64(s) => Value::Int(
                        s.parse()
                            .map_err(|_| Error::config("Invalid i64 parameter"))?,
                    ),
                    Parameter::U64(s) => Value::UInt(
                        s.parse()
                            .map_err(|_| Error::config("Invalid u64 parameter"))?,
                    ),
                    Parameter::F64(v) => Value::Double(*v),
                    Parameter::Bool(v) => Value::Int(i64::from(*v)),
                    Parameter::Binary(b) => Value::Bytes(
                        base64::engine::general_purpose::STANDARD
                            .decode(b)
                            .map_err(|_| Error::config("Invalid base64 binary parameter"))?,
                    ),
                })
            })
            .collect::<Result<_>>()?,
    ))
}
pub struct MysqlSource(pub MysqlReader);
#[async_trait]
impl Reader for MysqlSource {
    async fn schema(&self) -> Result<SchemaRef> {
        let c = &self.0;
        let mut conn = connect(&c.connection, false).await?;
        if let Some(split) = &c.split {
            let typ:Option<String>=conn.exec_first("SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",(&c.connection.database,c.source.table.as_deref().unwrap_or(""),&split.column)).await?;
            if !matches!(
                typ.as_deref(),
                Some("tinyint" | "smallint" | "mediumint" | "int" | "bigint")
            ) {
                return Err(Error::config(
                    "split.column must be a physical integer column",
                ));
            }
        }
        let sql = c.source.sql(&c.connection.database, None)?;
        let stmt = timeout(c.connection.timeouts.read_ms, async {
            Ok(conn.prep(sql).await?)
        })
        .await?;
        if stmt.num_params() as usize != c.source.params.len() {
            return Err(Error::config("SQL parameter count mismatch"));
        }
        let schema = types::mysql_schema(&stmt.columns())?;
        types::check_declared(&schema, &c.source.column_types)?;
        conn.disconnect().await?;
        Ok(schema)
    }
    async fn read(&self, emitter: Emitter) -> Result<()> {
        let predicates = match &self.0.split {
            Some(s) => s.predicates()?.into_iter().map(Some).collect::<Vec<_>>(),
            None => vec![None],
        };
        let concurrency = self.0.split.as_ref().map_or(1, |s| s.parallelism);
        stream::iter(predicates.into_iter().map(|p| {
            let emitter = emitter.clone();
            async move { self.read_partition(p, emitter).await }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<_>>()
        .await?;
        Ok(())
    }
}
impl MysqlSource {
    async fn read_partition(&self, predicate: Option<String>, emitter: Emitter) -> Result<()> {
        let c = &self.0;
        let _raw = emitter
            .reserve_reader(emitter.max_row_bytes().max(c.batch.bytes).saturating_mul(2))
            .await?;
        let mut conn = connect(&c.connection, false).await?;
        timeout(c.connection.timeouts.write_ms, async {
            if c.consistency == Consistency::Snapshot {
                conn.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    .await?;
                conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
                    .await?;
            } else {
                conn.query_drop("SET SESSION TRANSACTION READ ONLY").await?;
            }
            Ok(())
        })
        .await?;
        let sql = c.source.sql(&c.connection.database, predicate.as_deref())?;
        let mut result = timeout(c.connection.timeouts.read_ms, async {
            Ok(conn.exec_iter(sql, parameters(&c.source.params)?).await?)
        })
        .await?;
        let schema = types::mysql_schema(result.columns_ref())?;
        emitter.check_schema(&schema)?;
        let mut batch = MysqlBatchBuilder::new(schema, &c.connection.time_zone)?;
        loop {
            let row = tokio::select! { biased; _=emitter.cancelled()=>return Err(Error::cancelled()), row=timeout(c.connection.timeouts.read_ms,async{Ok(result.next().await?)})=>row? };
            let Some(row) = row else { break };
            let row_bytes = MysqlBatchBuilder::row_bytes(&row);
            if row_bytes > emitter.max_row_bytes() {
                return Err(Error::new(
                    "ROW_TOO_LARGE",
                    format!("Source row is {row_bytes} bytes"),
                ));
            }
            if batch.rows() > 0
                && (batch.rows() >= c.batch.rows || batch.bytes() + row_bytes > c.batch.bytes)
            {
                emitter.emit(batch.finish()?).await?;
            }
            batch.append(row)?;
        }
        result.drop_result().await?;
        if batch.rows() > 0 {
            emitter.emit(batch.finish()?).await?;
        }
        if c.consistency == Consistency::Snapshot {
            timeout(c.connection.timeouts.write_ms, async {
                Ok(conn.query_drop("ROLLBACK").await?)
            })
            .await?;
        }
        conn.disconnect().await?;
        Ok(())
    }
}

pub async fn target_columns(config: &WriterConfig) -> Result<Vec<TargetColumn>> {
    let c = config.sql_connection();
    let doris = matches!(config, WriterConfig::Doris(_));
    let mut conn = connect(c, doris).await?;
    let database = literal(&c.database);
    let table = literal(config.table());
    let rows:Vec<Row>=timeout(c.timeouts.read_ms,async{Ok(conn.query(format!("SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_DEFAULT,EXTRA,COLUMN_KEY,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA={database} AND TABLE_NAME={table} ORDER BY ORDINAL_POSITION")).await?)}).await?;
    if rows.is_empty() {
        return Err(Error::new(
            "SCHEMA",
            "Target table does not exist or is not visible",
        ));
    }
    // Doris' MySQL-compatible information_schema disguises BOOLEAN as tinyint(1).
    // Stream Load requires the native physical type (a wrong Arrow array can crash BE).
    let native_types = if doris {
        let native: Vec<Row> = timeout(c.timeouts.read_ms, async {
            Ok(conn
                .query(format!(
                    "SHOW FULL COLUMNS FROM {}.{}",
                    ident(&c.database)?,
                    ident(config.table())?
                ))
                .await?)
        })
        .await?;
        native
            .into_iter()
            .map(|row| {
                let name: String = row.get(0).unwrap_or_default();
                let typ: String = row.get(1).unwrap_or_default();
                (name, typ)
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    } else {
        std::collections::BTreeMap::new()
    };
    let mut cols = Vec::new();
    for row in rows {
        let name: String = row
            .get(0)
            .ok_or_else(|| Error::new("SCHEMA", "Missing column name"))?;
        let sql: String = row
            .get(1)
            .ok_or_else(|| Error::new("SCHEMA", "Missing SQL type"))?;
        let sql = native_types.get(&name).cloned().unwrap_or(sql);
        let nullable: String = row.get(2).unwrap_or_default();
        let default: Option<String> = row.get(3).unwrap_or(None);
        let extra: String = row.get(4).unwrap_or_default();
        let key: String = row.get(5).unwrap_or_default();
        let p: Option<u64> = row.get(6).unwrap_or(None);
        let s: Option<u64> = row.get(7).unwrap_or(None);
        let dp: Option<u64> = row.get(8).unwrap_or(None);
        let typ = types::sql_type(&sql, p, s, dp)?;
        cols.push(TargetColumn {
            field: Field::new(name, typ, nullable == "YES"),
            sql_type: sql,
            has_default: default.is_some() || extra.to_ascii_lowercase().contains("auto_increment"),
            generated: extra.to_ascii_lowercase().contains("generated")
                && !extra.eq_ignore_ascii_case("DEFAULT_GENERATED"),
            key: !key.is_empty(),
            datetime_precision: dp,
        });
    }
    match config {
        WriterConfig::Mysql(w) => {
            let engine:Option<String>=conn.query_first(format!("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA={database} AND TABLE_NAME={table}")).await?;
            if !engine.is_some_and(|e| e.eq_ignore_ascii_case("InnoDB")) {
                return Err(Error::new("SCHEMA", "MySQL target must use InnoDB"));
            }
            if w.mode == MysqlWriteMode::Upsert {
                let indexes:Vec<(String,String)>=conn.query(format!("SELECT INDEX_NAME,COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA={database} AND TABLE_NAME={table} AND NON_UNIQUE=0 ORDER BY INDEX_NAME,SEQ_IN_INDEX")).await?;
                let mut keys = std::collections::BTreeMap::<String, Vec<String>>::new();
                for (name, column) in indexes {
                    keys.entry(name).or_default().push(column);
                }
                if !keys.values().any(|cols| cols == &w.key_columns) {
                    return Err(Error::new(
                        "SCHEMA",
                        "key_columns must exactly match an existing primary/unique index in index order",
                    ));
                }
            }
        }
        WriterConfig::Doris(w) => {
            let row: Option<(String, String)> = conn
                .query_first(format!(
                    "SHOW CREATE TABLE {}.{}",
                    ident(&c.database)?,
                    ident(&w.table)?
                ))
                .await?;
            let ddl = row
                .ok_or_else(|| Error::new("SCHEMA", "Cannot inspect Doris table model"))?
                .1
                .to_ascii_uppercase();
            let valid = match w.mode {
                DorisWriteMode::Append => ddl.contains("DUPLICATE KEY"),
                DorisWriteMode::Upsert => {
                    ddl.contains("UNIQUE KEY")
                        && ddl.contains("\"ENABLE_UNIQUE_KEY_MERGE_ON_WRITE\" = \"TRUE\"")
                }
            };
            if !valid {
                return Err(Error::new(
                    "SCHEMA",
                    "Doris append requires DUPLICATE KEY; upsert requires UNIQUE KEY merge-on-write",
                ));
            }
        }
    }
    conn.disconnect().await?;
    Ok(cols)
}
pub async fn hooks(
    config: &WriterConfig,
    sqls: &[String],
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<()> {
    if sqls.is_empty() {
        return Ok(());
    }
    let c = config.sql_connection();
    let mut conn = connect(c, matches!(config, WriterConfig::Doris(_))).await?;
    for sql in sqls {
        if cancel.is_cancelled() {
            return Err(Error::cancelled());
        }
        let result = timeout(c.timeouts.write_ms, async {
            Ok(conn.query_drop(sql).await?)
        })
        .await;
        if let Err(e) = result {
            return Err(if matches!(e.code.as_str(), "TIMEOUT" | "IO") {
                Error::unknown("Pre/post SQL response was lost; its effects must be checked")
            } else {
                e
            });
        }
    }
    conn.disconnect().await?;
    Ok(())
}
pub struct MysqlSink {
    config: MysqlWriter,
    columns: Vec<TargetColumn>,
    conn: Option<Conn>,
}
impl MysqlSink {
    pub fn new(config: MysqlWriter, columns: Vec<TargetColumn>) -> Self {
        Self {
            config,
            columns,
            conn: None,
        }
    }
    fn insert_sql(&self, rows: usize) -> Result<String> {
        let fields = self
            .columns
            .iter()
            .map(|c| ident(c.field.name()))
            .collect::<Result<Vec<_>>>()?
            .join(",");
        let row = format!("({})", vec!["?"; self.columns.len()].join(","));
        let mut sql = format!(
            "INSERT INTO {}.{} ({fields}) VALUES {}",
            ident(&self.config.connection.database)?,
            ident(&self.config.table)?,
            vec![row; rows].join(",")
        );
        if self.config.mode == MysqlWriteMode::Upsert {
            let updates = if self.config.update_columns.is_empty() {
                self.columns
                    .iter()
                    .map(|c| c.field.name().clone())
                    .filter(|n| !self.config.key_columns.contains(n))
                    .collect::<Vec<_>>()
            } else {
                self.config.update_columns.clone()
            };
            if updates.is_empty() {
                return Err(Error::config(
                    "upsert needs at least one non-key update column",
                ));
            }
            // VALUES() remains supported across the tested MySQL versions, including 8.4/9.7.
            sql.push_str(" ON DUPLICATE KEY UPDATE ");
            sql.push_str(
                &updates
                    .iter()
                    .map(|n| {
                        let n = ident(n)?;
                        Ok(format!("{n}=VALUES({n})"))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(","),
            );
        }
        Ok(sql)
    }
    async fn attempt(&mut self, batch: &RecordBatch) -> Result<Receipt> {
        let c = &self.config.connection;
        if self.conn.is_none() {
            self.conn = Some(connect(c, false).await?);
        }
        let mut conn = self.conn.take().unwrap();
        let packet: Option<u64> = timeout(c.timeouts.read_ms, async {
            Ok(conn.query_first("SELECT @@max_allowed_packet").await?)
        })
        .await?;
        let packet = packet
            .unwrap_or(c.max_allowed_packet_bytes as u64)
            .min(c.max_allowed_packet_bytes as u64) as usize;
        let query_ms = c.timeouts.write_ms;
        let mut affected = 0;
        timeout(query_ms, async {
            Ok(conn.query_drop("START TRANSACTION").await?)
        })
        .await?;
        let write: Result<()> = async {
            let mut start = 0;
            while start < batch.num_rows() {
                let mut values = Vec::new();
                let mut count = 0;
                let mut bytes = 1024;
                while start + count < batch.num_rows() && (count + 1) * batch.num_columns() <= 65535
                {
                    let row_values = batch
                        .columns()
                        .iter()
                        .map(|a| types::mysql_value(a, start + count, &c.time_zone))
                        .collect::<Result<Vec<_>>>()?;
                    let row_size = row_values
                        .iter()
                        .map(|v| match v {
                            Value::Bytes(b) => b.len() + 16,
                            _ => 32,
                        })
                        .sum::<usize>()
                        + batch.num_columns() * 4;
                    if bytes + row_size > packet.saturating_sub(1024) {
                        if count == 0 {
                            return Err(Error::new(
                                "ROW_TOO_LARGE",
                                "Row exceeds MySQL max_allowed_packet",
                            ));
                        }
                        break;
                    }
                    values.extend(row_values);
                    bytes += row_size;
                    count += 1;
                }
                if count == 0 {
                    return Err(Error::config(
                        "Too many target columns for prepared statement",
                    ));
                }
                let sql = self.insert_sql(count)?;
                timeout(query_ms, async {
                    Ok(conn.exec_drop(sql, Params::Positional(values)).await?)
                })
                .await?;
                affected += conn.affected_rows();
                start += count;
            }
            Ok(())
        }
        .await;
        if let Err(mut e) = write {
            let rollback =
                timeout(query_ms, async { Ok(conn.query_drop("ROLLBACK").await?) }).await;
            e.retryable &= rollback.is_ok();
            if rollback.is_ok() {
                self.conn = Some(conn);
            }
            return Err(e);
        }
        if timeout(query_ms, async { Ok(conn.query_drop("COMMIT").await?) })
            .await
            .is_err()
        {
            return Err(Error::unknown(
                "MySQL COMMIT response missing; do not replay this batch automatically",
            ));
        }
        self.conn = Some(conn);
        Ok(Receipt {
            loaded: batch.num_rows() as u64,
            filtered: 0,
            affected,
            detail: serde_json::json!({"transaction":"committed"}),
        })
    }
}
#[async_trait]
impl Writer for MysqlSink {
    async fn write(&mut self, batch: &RecordBatch, _label: &str) -> Result<Receipt> {
        for attempt in 0..=self.config.options.retry_attempts {
            match self.attempt(batch).await {
                Err(e)
                    if e.retryable
                        && !e.commit_unknown
                        && attempt < self.config.options.retry_attempts =>
                {
                    tokio::time::sleep(Duration::from_millis(
                        self.config
                            .options
                            .retry_backoff_ms
                            .saturating_mul(1u64 << attempt.min(10)),
                    ))
                    .await;
                }
                result => return result,
            }
        }
        unreachable!()
    }
}
