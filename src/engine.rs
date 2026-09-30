use crate::{
    config::*,
    connectors,
    error::{Error, Result},
    store::{Run, Store},
    types::{self, BatchMapping},
};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub struct Validated {
    pub schema: SchemaRef,
    pub mapping: BatchMapping,
}
pub async fn validate(spec: &RunSpec) -> Result<Validated> {
    spec.validate()?;
    let reader = connectors::reader(&spec.reader);
    let (schema, columns) = tokio::try_join!(
        reader.schema(),
        connectors::mysql::target_columns(&spec.writer)
    )?;
    let time_zone = match &spec.writer {
        WriterConfig::Mysql(w) => &w.connection.time_zone,
        WriterConfig::Doris(w) => &w.time_zone,
    };
    let mapping = BatchMapping::plan(&schema, &columns, &spec.mapping, time_zone)?;
    if let WriterConfig::Doris(w) = &spec.writer {
        connectors::doris::DorisSink::new(w.as_ref().clone(), mapping.columns.clone())?
            .probe()
            .await?;
    }
    if let WriterConfig::Mysql(w) = &spec.writer {
        for col in w.key_columns.iter().chain(&w.update_columns) {
            if !mapping.columns.iter().any(|c| c.field.name() == col) {
                return Err(Error::config(format!(
                    "Key/update column {col} must be mapped"
                )));
            }
        }
        if w.update_columns.iter().any(|c| w.key_columns.contains(c)) {
            return Err(Error::config("update_columns cannot include key_columns"));
        }
    }
    if let WriterConfig::Doris(w) = &spec.writer
        && w.mode == DorisWriteMode::Upsert
        && columns.iter().filter(|c| !c.generated).any(|c| {
            !mapping
                .columns
                .iter()
                .any(|m| m.field.name() == c.field.name())
        })
    {
        return Err(Error::config(
            "Doris upsert requires all writable target columns; partial update is unsupported",
        ));
    }
    Ok(Validated { schema, mapping })
}
pub fn validation_json(v: &Validated) -> serde_json::Value {
    let fields = |schema: &arrow::datatypes::Schema| {
        schema.fields().iter().map(|f|serde_json::json!({"name":f.name(),"type":format!("{:?}",f.data_type()),"nullable":f.is_nullable()})).collect::<Vec<_>>()
    };
    serde_json::json!({"valid":true,"source_schema":fields(&v.schema),"target_schema":fields(&v.mapping.target),"semantics":"batch commits; no whole-job rollback or automatic resume"})
}
struct Envelope {
    batch: RecordBatch,
    _memory: OwnedSemaphorePermit,
}
struct Rate {
    started: Instant,
    rows: u64,
    bytes: u64,
}
#[derive(Clone)]
pub struct Emitter {
    sender: mpsc::Sender<Envelope>,
    raw: Arc<Semaphore>,
    budget: Arc<Semaphore>,
    half_budget: usize,
    cancel: CancellationToken,
    schema: SchemaRef,
    spec: Arc<RunSpec>,
    store: Store,
    id: String,
    rate: Arc<Mutex<Rate>>,
}
impl Emitter {
    pub fn max_row_bytes(&self) -> usize {
        self.spec.execution.max_row_bytes
    }
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await
    }
    pub fn check_schema(&self, schema: &SchemaRef) -> Result<()> {
        if schema.fields().len() != self.schema.fields().len()
            || schema
                .fields()
                .iter()
                .zip(self.schema.fields())
                .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
        {
            return Err(Error::new(
                "SCHEMA_CHANGED",
                "Source schema changed after validation",
            ));
        }
        Ok(())
    }
    pub async fn reserve_reader(&self, bytes: usize) -> Result<OwnedSemaphorePermit> {
        if bytes > self.half_budget {
            return Err(Error::config(
                "Reader working buffer exceeds half of memory_bytes; increase memory_bytes or reduce row/gRPC limits",
            ));
        }
        tokio::select! {biased;_=self.cancel.cancelled()=>Err(Error::cancelled()),p=self.raw.clone().acquire_many_owned(bytes as u32)=>p.map_err(|_|Error::cancelled())}
    }
    pub async fn emit(&self, batch: RecordBatch) -> Result<()> {
        self.check_schema(&batch.schema())?;
        self.store
            .read(&self.id, batch.num_rows(), batch.get_array_memory_size())?;
        let writer = &self.spec.writer.options().batch;
        let mut start = 0;
        while start < batch.num_rows() {
            let mut end = start;
            let mut bytes = 0;
            while end < batch.num_rows() && end - start < writer.rows {
                let size = types::logical_row_bytes(&batch, end);
                if size > self.max_row_bytes() {
                    return Err(Error::new("ROW_TOO_LARGE", "Row exceeds max_row_bytes"));
                }
                if end > start && bytes + size > writer.bytes {
                    break;
                }
                bytes += size;
                end += 1;
            }
            let reservation = bytes.saturating_mul(4).saturating_add(64 * 1024);
            if reservation > self.half_budget {
                return Err(Error::new(
                    "MEMORY_LIMIT",
                    "Batch conversion/encoding reservation exceeds memory budget; reduce batch.bytes or increase memory_bytes",
                ));
            }
            let permit = tokio::select! {biased;_=self.cancel.cancelled()=>return Err(Error::cancelled()),p=self.budget.clone().acquire_many_owned(reservation as u32)=>p.map_err(|_|Error::cancelled())?};
            let delay = {
                let mut r = self.rate.lock().await;
                r.rows += (end - start) as u64;
                r.bytes += bytes as u64;
                let rows = self
                    .spec
                    .execution
                    .rows_per_second
                    .map_or(0.0, |rate| r.rows as f64 / rate as f64);
                let bytes = self
                    .spec
                    .execution
                    .bytes_per_second
                    .map_or(0.0, |rate| r.bytes as f64 / rate as f64);
                Duration::from_secs_f64(rows.max(bytes)).saturating_sub(r.started.elapsed())
            };
            tokio::select! {biased;_=self.cancel.cancelled()=>return Err(Error::cancelled()),_=tokio::time::sleep(delay)=>{}}
            let batch = arrow::compute::concat_batches(
                &batch.schema(),
                [&batch.slice(start, end - start)],
            )?;
            let envelope = Envelope {
                batch,
                _memory: permit,
            };
            tokio::select! {biased;_=self.cancel.cancelled()=>return Err(Error::cancelled()),r=self.sender.send(envelope)=>r.map_err(|_|Error::cancelled())?};
            start = end;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Engine {
    pub store: Store,
    running: Arc<Semaphore>,
    admission: Arc<Semaphore>,
    active: Arc<Mutex<HashMap<String, CancellationToken>>>,
    accepting: Arc<AtomicBool>,
}
impl Engine {
    pub fn new(store: Store, config: &ServerConfig) -> Result<Self> {
        if config.max_running == 0 || config.max_running > 64 || config.max_queued > 4096 {
            return Err(Error::config(
                "max_running must be 1..64 and max_queued must be 0..4096",
            ));
        }
        Ok(Self {
            store,
            running: Arc::new(Semaphore::new(config.max_running)),
            admission: Arc::new(Semaphore::new(config.max_running + config.max_queued)),
            active: Arc::new(Mutex::new(HashMap::new())),
            accepting: Arc::new(AtomicBool::new(true)),
        })
    }
    pub fn ready(&self) -> Result<()> {
        if !self.accepting.load(Ordering::Relaxed) {
            return Err(Error::new("BUSY", "Service is shutting down"));
        }
        self.store.healthy()
    }
    pub async fn submit(&self, spec: RunSpec) -> Result<Run> {
        spec.validate()?;
        if let Some(run) = self.store.existing(&spec)? {
            return Ok(run);
        }
        self.ready()?;
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::new("BUSY", "Run queue is full"))?;
        let mut active = self.active.lock().await;
        let (run, created) = self.store.submit(&spec)?;
        if !created {
            return Ok(run);
        }
        let cancel = CancellationToken::new();
        active.insert(run.run_id.clone(), cancel.clone());
        drop(active);
        let engine = self.clone();
        let id = run.run_id.clone();
        tokio::spawn(async move {
            let _admission = admission;
            let permit = tokio::select! {biased;_=cancel.cancelled()=>None,p=engine.running.clone().acquire_owned()=>p.ok()};
            let outcome = if let Some(_permit) = permit {
                match engine.store.mutate(&id, |r| {
                    if r.state == "QUEUED" {
                        r.state = "RUNNING".into()
                    }
                }) {
                    Ok(()) => engine.run(&id, Arc::new(spec), cancel.clone()).await,
                    Err(e) => Err(e),
                }
            } else {
                Err(Error::cancelled())
            };
            let unknown = engine.store.has_pending(&id).unwrap_or(true);
            if let Err(e) = engine.store.mutate(&id, |r| {
                r.commit_unknown =
                    unknown || outcome.as_ref().err().is_some_and(|e| e.commit_unknown);
                match outcome {
                    Ok(()) => {
                        r.state = "SUCCEEDED".into();
                        r.stage = "complete".into();
                    }
                    Err(e) => {
                        r.state = if e.code == "CANCELLED" {
                            "CANCELLED"
                        } else {
                            "FAILED"
                        }
                        .into();
                        r.error = Some(e);
                    }
                }
                r.partial_write = r.state != "SUCCEEDED" && r.rows_committed > 0;
            }) {
                tracing::error!(run_id=%id,error=%e,"Could not persist terminal run state; startup will reconcile interruption");
            }
            engine.active.lock().await.remove(&id);
        });
        Ok(run)
    }
    pub async fn cancel(&self, id: &str) -> Result<Run> {
        let active = self.active.lock().await;
        let run = self.store.get(id)?;
        if let Some(request_id) = &run.request_id {
            self.store.cancel_request(request_id)?;
        }
        if !run.terminal() {
            self.store.mutate(id, |r| {
                if !r.terminal() {
                    r.state = "CANCELLING".into()
                }
            })?;
            if let Some(token) = active.get(id) {
                token.cancel();
            }
        }
        self.store.get(id)
    }
    pub async fn cancel_request(&self, id: &str) -> Result<serde_json::Value> {
        let active = self.active.lock().await;
        if let Some(run) = self.store.cancel_request(id)?
            && let Some(token) = active.get(&run.run_id)
        {
            token.cancel();
        }
        self.store.request(id)
    }
    pub async fn shutdown(&self, wait_ms: u64) {
        self.accepting.store(false, Ordering::Relaxed);
        for token in self.active.lock().await.values() {
            token.cancel();
        }
        let deadline = Instant::now() + Duration::from_millis(wait_ms);
        while !self.active.lock().await.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    async fn run(&self, id: &str, spec: Arc<RunSpec>, cancel: CancellationToken) -> Result<()> {
        let timed_out = Arc::new(AtomicBool::new(false));
        let timer = {
            let cancel = cancel.clone();
            let flag = timed_out.clone();
            let ms = spec.execution.timeout_ms;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                flag.store(true, Ordering::Relaxed);
                cancel.cancel();
            })
        };
        let result = self.run_inner(id, spec, cancel).await;
        timer.abort();
        match result {
            Err(e) if e.code == "CANCELLED" && timed_out.load(Ordering::Relaxed) => {
                Err(Error::new("TIMEOUT", "Task execution timeout reached"))
            }
            other => other,
        }
    }
    async fn run_inner(
        &self,
        id: &str,
        spec: Arc<RunSpec>,
        cancel: CancellationToken,
    ) -> Result<()> {
        self.store.stage(id, "validate")?;
        let initial = tokio::select! {biased;_=cancel.cancelled()=>return Err(Error::cancelled()),v=validate(&spec)=>v?};
        if cancel.is_cancelled() {
            return Err(Error::cancelled());
        }
        self.store.stage(id, "pre_sql")?;
        connectors::mysql::hooks(&spec.writer, &spec.writer.options().pre_sql, &cancel).await?;
        let columns = connectors::mysql::target_columns(&spec.writer).await?;
        let mapping = Arc::new(BatchMapping::plan(
            &initial.schema,
            &columns,
            &spec.mapping,
            &initial.mapping.time_zone,
        )?);
        let (sender, receiver) = mpsc::channel(spec.execution.queue_capacity);
        let half_budget = spec.execution.memory_bytes / 2;
        let emitter = Emitter {
            sender,
            raw: Arc::new(Semaphore::new(half_budget)),
            budget: Arc::new(Semaphore::new(half_budget)),
            half_budget,
            cancel: cancel.clone(),
            schema: initial.schema,
            spec: spec.clone(),
            store: self.store.clone(),
            id: id.into(),
            rate: Arc::new(Mutex::new(Rate {
                started: Instant::now(),
                rows: 0,
                bytes: 0,
            })),
        };
        let receiver = Arc::new(Mutex::new(receiver));
        let seq = Arc::new(AtomicU64::new(0));
        let error = Arc::new(Mutex::new(None::<Error>));
        let mut tasks = tokio::task::JoinSet::new();
        let writers = (0..spec.writer.options().parallelism)
            .map(|_| connectors::writer(&spec.writer, mapping.columns.clone()))
            .collect::<Result<Vec<_>>>()?;
        self.store.stage(id, "transfer")?;
        let source = connectors::reader(&spec.reader);
        let fail = error.clone();
        let stop = cancel.clone();
        tasks.spawn(async move {
            if let Err(e) = source.read(emitter).await {
                record_error(&fail, e).await;
                stop.cancel();
            }
        });
        for mut writer in writers {
            let receiver = receiver.clone();
            let seq = seq.clone();
            let cancel = cancel.clone();
            let mapping = mapping.clone();
            let store = self.store.clone();
            let id = id.to_owned();
            let error = error.clone();
            let spec = spec.clone();
            tasks.spawn(async move {
                let result:Result<()>=async {
                    loop {
                        let envelope=tokio::select!{biased;_=cancel.cancelled()=>return Err(Error::cancelled()),v=async{receiver.lock().await.recv().await}=>v};
                        let Some(envelope)=envelope else{break};
                        let batch=mapping.apply(&envelope.batch)?;
                        if cancel.is_cancelled(){return Err(Error::cancelled());}
                        let batch_id=seq.fetch_add(1,Ordering::Relaxed);
                        let prefix=match &spec.writer{WriterConfig::Doris(w)=>w.label_prefix.as_str(),_=>"dunnelean"};
                        let label=format!("{prefix}_{}_{batch_id}",id.replace('-',""));
                        store.intent(&id,batch_id,batch.num_rows(),batch.get_array_memory_size(),Some(&label))?;
                        // Once intent is persisted, await the writer even on cancellation so its outcome is recorded.
                        match writer.write(&batch,&label).await {
                            Ok(receipt)=>store.confirmed(&id,batch_id,receipt.loaded,receipt.filtered,receipt.affected,&receipt.detail).map_err(|_|Error::unknown("Target committed but the local receipt could not be persisted"))?,
                            Err(e)=>{store.rejected(&id,batch_id,&e)?;return Err(e);}
                        }
                    }
                    Ok(())
                }.await;
                if let Err(e)=result{record_error(&error,e).await;cancel.cancel();}
            });
        }
        // Every reader and writer is joined; no child keeps writing after the run becomes terminal.
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                record_error(
                    &error,
                    Error::unknown(format!("Worker terminated unexpectedly: {e}")),
                )
                .await;
                cancel.cancel();
            }
        }
        if let Some(e) = error.lock().await.take() {
            return Err(e);
        }
        if cancel.is_cancelled() {
            return Err(Error::cancelled());
        }
        self.store.stage(id, "post_sql")?;
        connectors::mysql::hooks(&spec.writer, &spec.writer.options().post_sql, &cancel).await?;
        Ok(())
    }
}
async fn record_error(slot: &Mutex<Option<Error>>, e: Error) {
    let mut slot = slot.lock().await;
    if slot.is_none()
        || e.commit_unknown
        || slot
            .as_ref()
            .is_some_and(|old| old.code == "CANCELLED" && e.code != "CANCELLED")
    {
        *slot = Some(e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };

    #[tokio::test]
    async fn slow_writer_backpressure_is_bounded_and_cancel_releases_memory() {
        let temp = tempfile::tempdir().unwrap();
        let store = Store::open(temp.path().join("state.sqlite")).unwrap();
        let mut spec: RunSpec =
            serde_json::from_str(include_str!("../examples/mysql-to-doris.json")).unwrap();
        if let WriterConfig::Doris(w) = &mut spec.writer {
            w.options.batch.rows = 1;
        }
        let (run, _) = store.submit(&spec).unwrap();
        let (sender, mut receiver) = mpsc::channel(1);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let budget = Arc::new(Semaphore::new(128 * 1024 * 1024));
        let cancel = CancellationToken::new();
        let emitter = Emitter {
            sender,
            raw: budget.clone(),
            budget: budget.clone(),
            half_budget: 128 * 1024 * 1024,
            cancel: cancel.clone(),
            schema: schema.clone(),
            spec: Arc::new(spec),
            store,
            id: run.run_id,
            rate: Arc::new(Mutex::new(Rate {
                started: Instant::now(),
                rows: 0,
                bytes: 0,
            })),
        };
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();
        let producer = tokio::spawn(async move { emitter.emit(batch).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while receiver.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(receiver.len(), 1);
        assert!(
            !producer.is_finished(),
            "reader must block behind the full queue"
        );
        drop(receiver.recv().await);
        tokio::time::timeout(Duration::from_secs(5), async {
            while receiver.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(receiver.len(), 1);
        assert!(!producer.is_finished());
        cancel.cancel();
        assert_eq!(producer.await.unwrap().unwrap_err().code, "CANCELLED");
        drop(receiver);
        assert_eq!(budget.available_permits(), 128 * 1024 * 1024);
    }
}
