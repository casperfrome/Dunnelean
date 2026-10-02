use crate::{
    config::RunSpec,
    error::{Error, Result},
};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Run {
    pub run_id: String,
    pub request_id: Option<String>,
    pub state: String,
    pub stage: String,
    pub created_at: String,
    pub updated_at: String,
    pub rows_read: u64,
    pub bytes_read: u64,
    pub rows_submitted: u64,
    pub rows_committed: u64,
    pub rows_filtered: u64,
    pub server_affected_rows: u64,
    pub batches_committed: u64,
    pub partial_write: bool,
    pub commit_unknown: bool,
    pub error: Option<Error>,
    pub config: serde_json::Value,
}
impl Run {
    pub fn terminal(&self) -> bool {
        matches!(
            self.state.as_str(),
            "SUCCEEDED" | "FAILED" | "CANCELLED" | "INTERRUPTED"
        )
    }
}
#[derive(Clone)]
pub struct Store {
    db: Arc<Mutex<Connection>>,
    _process_lock: Arc<std::fs::File>,
}
impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let process_lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.as_ref().with_extension("sqlite.lock"))?;
        process_lock.try_lock().map_err(|_| {
            Error::new(
                "STATE_STORE",
                "State database is already owned by another Dunnelean process",
            )
        })?;
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY, request_id TEXT UNIQUE, fingerprint TEXT NOT NULL, data TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS batches(run_id TEXT NOT NULL, id INTEGER NOT NULL, state TEXT NOT NULL, rows INTEGER NOT NULL, bytes INTEGER NOT NULL, label TEXT, detail TEXT, PRIMARY KEY(run_id,id));
          CREATE TABLE IF NOT EXISTS health(id INTEGER PRIMARY KEY, probe INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS request_cancellations(request_id TEXT PRIMARY KEY, created_at TEXT NOT NULL);
          INSERT OR IGNORE INTO health VALUES(1,0);")?;
        db.execute(
            "INSERT OR IGNORE INTO metadata VALUES('state_store_id',?)",
            [uuid::Uuid::new_v4().to_string()],
        )?;
        let store = Self {
            db: Arc::new(Mutex::new(db)),
            _process_lock: Arc::new(process_lock),
        };
        let mut offset = 0;
        loop {
            let page = store.list(1000, offset)?;
            if page.is_empty() {
                break;
            }
            offset += page.len();
            for mut run in page {
                if !run.terminal() {
                    run.state = "INTERRUPTED".into();
                    run.error = Some(Error::new(
                        "INTERRUPTED",
                        "Service restarted; run is not automatically resumed",
                    ));
                    let hooks_pending = matches!(run.stage.as_str(), "pre_sql" | "post_sql")
                        && run
                            .config
                            .pointer(&format!("/writer/options/{}", run.stage))
                            .and_then(|v| v.as_array())
                            .is_some_and(|v| !v.is_empty());
                    run.commit_unknown = store.has_pending(&run.run_id)? || hooks_pending;
                    run.partial_write = run.rows_committed > 0;
                    store.save(&run)?;
                }
            }
        }
        Ok(store)
    }
    /// Report whether this store is the last owner of its connection and lock.
    /// Callers must prevent new owners before using this to finalize shutdown.
    pub fn is_exclusively_owned(&self) -> bool {
        Arc::strong_count(&self.db) == 1 && Arc::strong_count(&self._process_lock) == 1
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.db
            .lock()
            .map_err(|_| Error::new("STATE_STORE", "State lock poisoned"))
    }
    pub fn healthy(&self) -> Result<()> {
        self.lock()?
            .execute("UPDATE health SET probe=1-probe WHERE id=1", [])?;
        Ok(())
    }
    pub fn state_store_id(&self) -> Result<String> {
        Ok(self.lock()?.query_row(
            "SELECT value FROM metadata WHERE key='state_store_id'",
            [],
            |r| r.get(0),
        )?)
    }
    pub fn request(&self, id: &str) -> Result<serde_json::Value> {
        let db = self.lock()?;
        let data: Option<String> = db
            .query_row("SELECT data FROM runs WHERE request_id=?", [id], |r| {
                r.get(0)
            })
            .optional()?;
        let cancelled = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM request_cancellations WHERE request_id=?)",
            [id],
            |r| r.get::<_, bool>(0),
        )?;
        if data.is_none() && !cancelled {
            return Err(Error::new("NOT_FOUND", "Request not found"));
        }
        let run = data
            .map(|s| serde_json::from_str::<serde_json::Value>(&s))
            .transpose()?;
        Ok(serde_json::json!({"request_id":id,"cancel_requested":cancelled,"run":run}))
    }
    pub fn cancel_request(&self, id: &str) -> Result<Option<Run>> {
        if id.is_empty() || id.len() > 200 {
            return Err(Error::config("request_id must be 1..200 bytes"));
        }
        let mut db = self.lock()?;
        let tx = db.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO request_cancellations VALUES(?,?)",
            params![id, Utc::now().to_rfc3339()],
        )?;
        let data: Option<String> = tx
            .query_row("SELECT data FROM runs WHERE request_id=?", [id], |r| {
                r.get(0)
            })
            .optional()?;
        let mut run = data.map(|s| serde_json::from_str::<Run>(&s)).transpose()?;
        if let Some(r) = &mut run
            && !r.terminal()
        {
            r.state = "CANCELLING".into();
            r.updated_at = Utc::now().to_rfc3339();
            tx.execute(
                "UPDATE runs SET data=? WHERE id=?",
                params![serde_json::to_string(r)?, r.run_id],
            )?;
        }
        tx.commit()?;
        Ok(run)
    }
    pub fn existing(&self, spec: &RunSpec) -> Result<Option<Run>> {
        let Some(id) = &spec.request_id else {
            return Ok(None);
        };
        let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(spec)?));
        let row: Option<(String, String)> = self
            .lock()?
            .query_row(
                "SELECT fingerprint,data FROM runs WHERE request_id=?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match row {
            Some((hash, data)) if hash == fingerprint => Ok(Some(serde_json::from_str(&data)?)),
            Some(_) => Err(Error::new(
                "CONFLICT",
                "request_id already belongs to a different configuration",
            )),
            None => Ok(None),
        }
    }
    pub fn submit(&self, spec: &RunSpec) -> Result<(Run, bool)> {
        let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(spec)?));
        let db = self.lock()?;
        if let Some(id) = &spec.request_id
            && let Some((hash, data)) = db
                .query_row(
                    "SELECT fingerprint,data FROM runs WHERE request_id=?",
                    [id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()?
        {
            if hash != fingerprint {
                return Err(Error::new(
                    "CONFLICT",
                    "request_id already belongs to a different configuration",
                ));
            }
            return Ok((serde_json::from_str(&data)?, false));
        }
        let now = Utc::now().to_rfc3339();
        if let Some(id) = &spec.request_id
            && db.query_row(
                "SELECT EXISTS(SELECT 1 FROM request_cancellations WHERE request_id=?)",
                [id],
                |r| r.get::<_, bool>(0),
            )?
        {
            return Err(Error::new(
                "REQUEST_CANCELLED",
                "Request was cancelled before submission",
            ));
        }
        let run = Run {
            run_id: uuid::Uuid::new_v4().to_string(),
            request_id: spec.request_id.clone(),
            state: "QUEUED".into(),
            stage: "queued".into(),
            created_at: now.clone(),
            updated_at: now,
            rows_read: 0,
            bytes_read: 0,
            rows_submitted: 0,
            rows_committed: 0,
            rows_filtered: 0,
            server_affected_rows: 0,
            batches_committed: 0,
            partial_write: false,
            commit_unknown: false,
            error: None,
            config: spec.redacted(),
        };
        db.execute(
            "INSERT INTO runs VALUES(?,?,?,?)",
            params![
                run.run_id,
                run.request_id,
                fingerprint,
                serde_json::to_string(&run)?
            ],
        )?;
        Ok((run, true))
    }
    pub fn get(&self, id: &str) -> Result<Run> {
        let data: Option<String> = self
            .lock()?
            .query_row("SELECT data FROM runs WHERE id=?", [id], |r| r.get(0))
            .optional()?;
        serde_json::from_str(&data.ok_or_else(|| Error::new("NOT_FOUND", "Run not found"))?)
            .map_err(Into::into)
    }
    pub fn list(&self, limit: usize, offset: usize) -> Result<Vec<Run>> {
        let db = self.lock()?;
        let mut stmt = db.prepare("SELECT data FROM runs ORDER BY rowid DESC LIMIT ? OFFSET ?")?;
        let rows = stmt.query_map(params![limit, offset], |r| r.get::<_, String>(0))?;
        rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
    }
    pub fn save(&self, run: &Run) -> Result<()> {
        let mut run = run.clone();
        run.updated_at = Utc::now().to_rfc3339();
        self.lock()?.execute(
            "UPDATE runs SET data=? WHERE id=?",
            params![serde_json::to_string(&run)?, run.run_id],
        )?;
        Ok(())
    }
    pub fn mutate(&self, id: &str, f: impl FnOnce(&mut Run)) -> Result<()> {
        let mut db = self.lock()?;
        let tx = db.transaction()?;
        let data: String = tx.query_row("SELECT data FROM runs WHERE id=?", [id], |r| r.get(0))?;
        let mut run: Run = serde_json::from_str(&data)?;
        f(&mut run);
        run.updated_at = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE runs SET data=? WHERE id=?",
            params![serde_json::to_string(&run)?, id],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn stage(&self, id: &str, stage: &str) -> Result<()> {
        self.mutate(id, |r| r.stage = stage.into())
    }
    pub fn read(&self, id: &str, rows: usize, bytes: usize) -> Result<()> {
        self.mutate(id, |r| {
            r.rows_read += rows as u64;
            r.bytes_read += bytes as u64;
        })
    }
    pub fn intent(
        &self,
        id: &str,
        batch: u64,
        rows: usize,
        bytes: usize,
        label: Option<&str>,
    ) -> Result<()> {
        let mut db = self.lock()?;
        let tx = db.transaction()?;
        tx.execute(
            "INSERT INTO batches VALUES(?,?,'INTENT',?,?,?,NULL)",
            params![id, batch, rows, bytes, label],
        )?;
        let data: String = tx.query_row("SELECT data FROM runs WHERE id=?", [id], |r| r.get(0))?;
        let mut run: Run = serde_json::from_str(&data)?;
        run.rows_submitted += rows as u64;
        run.updated_at = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE runs SET data=? WHERE id=?",
            params![serde_json::to_string(&run)?, id],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn confirmed(
        &self,
        id: &str,
        batch: u64,
        loaded: u64,
        filtered: u64,
        affected: u64,
        detail: &serde_json::Value,
    ) -> Result<()> {
        let mut db = self.lock()?;
        let tx = db.transaction()?;
        let changed=tx.execute("UPDATE batches SET state='CONFIRMED',detail=? WHERE run_id=? AND id=? AND state='INTENT'",params![detail.to_string(),id,batch])?;
        if changed != 1 {
            return Err(Error::new(
                "STATE_STORE",
                "Cannot confirm an unprepared/already finalized batch",
            ));
        }
        let data: String = tx.query_row("SELECT data FROM runs WHERE id=?", [id], |r| r.get(0))?;
        let mut run: Run = serde_json::from_str(&data)?;
        run.rows_committed += loaded;
        run.rows_filtered += filtered;
        run.server_affected_rows += affected;
        run.batches_committed += 1;
        run.updated_at = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE runs SET data=? WHERE id=?",
            params![serde_json::to_string(&run)?, id],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn rejected(&self, id: &str, batch: u64, e: &Error) -> Result<()> {
        self.lock()?.execute(
            "UPDATE batches SET state=?,detail=? WHERE run_id=? AND id=?",
            params![
                if e.commit_unknown {
                    "UNKNOWN"
                } else {
                    "NOT_COMMITTED"
                },
                serde_json::to_string(e)?,
                id,
                batch
            ],
        )?;
        Ok(())
    }
    pub fn has_pending(&self, id: &str) -> Result<bool> {
        Ok(self.lock()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM batches WHERE run_id=? AND state IN ('INTENT','UNKNOWN'))",
            [id],
            |r| r.get(0),
        )?)
    }
    pub fn batches(&self, id: &str) -> Result<Vec<serde_json::Value>> {
        let db = self.lock()?;
        let mut s = db.prepare(
            "SELECT id,state,rows,bytes,label,detail FROM batches WHERE run_id=? ORDER BY id",
        )?;
        let rows=s.query_map([id],|r|Ok(serde_json::json!({"batch_id":r.get::<_,u64>(0)?,"state":r.get::<_,String>(1)?,"rows":r.get::<_,u64>(2)?,"bytes":r.get::<_,u64>(3)?,"label":r.get::<_,Option<String>>(4)?,"detail":r.get::<_,Option<String>>(5)?})))?;
        rows.map(|r| r.map_err(Into::into)).collect()
    }
}
