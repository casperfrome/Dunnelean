pub mod doris;
pub mod mysql;
use crate::{config::*, engine::Emitter, error::Result, types::TargetColumn};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_trait::async_trait;

#[async_trait]
pub trait Reader: Send + Sync {
    async fn schema(&self) -> Result<SchemaRef>;
    async fn read(&self, emitter: Emitter) -> Result<()>;
}
#[derive(Debug)]
pub struct Receipt {
    pub loaded: u64,
    pub filtered: u64,
    pub affected: u64,
    pub detail: serde_json::Value,
}
#[async_trait]
pub trait Writer: Send {
    async fn write(&mut self, batch: &RecordBatch, label: &str) -> Result<Receipt>;
}
pub fn reader(config: &ReaderConfig) -> Box<dyn Reader> {
    match config {
        ReaderConfig::Mysql(c) => Box::new(mysql::MysqlSource(c.clone())),
        ReaderConfig::Doris(c) => Box::new(doris::DorisSource(c.clone())),
    }
}
pub fn writer(config: &WriterConfig, columns: Vec<TargetColumn>) -> Result<Box<dyn Writer>> {
    Ok(match config {
        WriterConfig::Mysql(c) => Box::new(mysql::MysqlSink::new(c.as_ref().clone(), columns)),
        WriterConfig::Doris(c) => Box::new(doris::DorisSink::new(c.as_ref().clone(), columns)?),
    })
}
