//! Orchestrates extract -> transform -> load and records every execution in `runs`.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use crate::db::Db;
use crate::extract;
use crate::model::{RawRow, RunRecord};
use crate::transform::{clean, DEFAULT_Z_THRESHOLD};

/// Where data comes from.
#[derive(Debug, Clone)]
pub enum Source {
    /// Open-Meteo public API (no key).
    Api,
    /// Local CSV file (works offline).
    Csv(PathBuf),
}

impl Source {
    pub fn label(&self) -> String {
        match self {
            Source::Api => "open-meteo".to_string(),
            Source::Csv(p) => format!("csv:{}", p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub z_threshold: f64,
    pub past_days: u32,
    pub attempts: u32,
    pub backoff: Duration,
    pub api_url: String,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        PipelineConfig {
            z_threshold: DEFAULT_Z_THRESHOLD,
            past_days: 14,
            attempts: 4,
            backoff: Duration::from_secs(2),
            api_url: extract::OPEN_METEO_URL.to_string(),
        }
    }
}

/// Transform + load already-extracted rows into an open run.
fn process(db: &Db, run_id: i64, rows: &[RawRow], cfg: &PipelineConfig) -> Result<RunRecord> {
    let out = clean(rows);
    if let Err(e) = db.load(&out.readings, cfg.z_threshold) {
        return db.finish_run(run_id, "failed", out.rows_in, 0, out.rejected.len(), Some(&format!("load failed: {e:#}")));
    }
    let rejected = out.rejected.len();
    let (status, message) = if rejected == 0 {
        ("success", None)
    } else {
        let sample: Vec<String> = out.rejected.iter().take(3).map(|r| format!("row {}: {}", r.row, r.reason)).collect();
        ("partial", Some(format!("{rejected} rows rejected; e.g. {}", sample.join("; "))))
    };
    db.finish_run(run_id, status, out.rows_in, out.rows_ok, rejected, message.as_deref())
}

/// Run the transform and load stages on in-memory rows (used by tests and CSV ingestion).
pub fn ingest_rows(db: &Db, source_label: &str, rows: &[RawRow], cfg: &PipelineConfig) -> Result<RunRecord> {
    let id = db.start_run(source_label)?;
    process(db, id, rows, cfg)
}

/// Full pipeline. Extraction/load problems are recorded as a `failed` run and returned
/// as `Ok(record)`; `Err` is reserved for the run table itself being unusable.
pub async fn run_ingest(db: &Db, source: &Source, cfg: &PipelineConfig) -> Result<RunRecord> {
    let id = db.start_run(&source.label())?;
    let extracted = match source {
        Source::Csv(path) => extract::read_csv(path),
        Source::Api => extract::fetch_open_meteo(&cfg.api_url, cfg.past_days, cfg.attempts, cfg.backoff).await,
    };
    match extracted {
        Ok(rows) => process(db, id, &rows, cfg),
        Err(e) => db.finish_run(id, "failed", 0, 0, 0, Some(&format!("extract failed: {e:#}"))),
    }
}
