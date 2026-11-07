//! Orchestrates extract -> transform -> load and records every execution as a run.
//!
//! The pipeline only knows the [`Source`] and [`Sink`] traits, so both ends can be swapped
//! (or faked in tests). The SQLite implementation of [`Sink`] lives in [`crate::db`].

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::StoreError;
use crate::extract::Source;
use crate::model::{RawRow, Reading, RunRecord, RunStatus, SourceFailure};
use crate::transform::{clean, AnomalyConfig, CleanOutcome};

/// Final numbers of a run, handed to the sink when the run is closed.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub status: RunStatus,
    pub rows_in: usize,
    pub rows_out: usize,
    pub rows_rejected: usize,
    pub message: Option<String>,
    pub failures: Vec<SourceFailure>,
}

impl RunOutcome {
    fn failed(message: String, failures: Vec<SourceFailure>) -> Self {
        RunOutcome { status: RunStatus::Failed, rows_in: 0, rows_out: 0, rows_rejected: 0, message: Some(message), failures }
    }
}

/// Destination of the load stage and the run ledger.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Open a run and return its id.
    async fn start_run(&self, source: &str) -> Result<i64, StoreError>;
    /// Idempotently store the readings and refresh whatever is derived from them.
    async fn load(&self, readings: Vec<Reading>, anomaly: AnomalyConfig) -> Result<(), StoreError>;
    /// Close a run with its outcome and return the stored record.
    async fn finish_run(&self, id: i64, outcome: RunOutcome) -> Result<RunRecord, StoreError>;
}

pub struct Pipeline {
    source: Arc<dyn Source>,
    sink: Arc<dyn Sink>,
    anomaly: AnomalyConfig,
}

impl Pipeline {
    pub fn new(source: Arc<dyn Source>, sink: Arc<dyn Sink>, anomaly: AnomalyConfig) -> Self {
        Pipeline { source, sink, anomaly }
    }

    pub fn source_label(&self) -> String {
        self.source.label()
    }

    /// Run the pipeline once. Extraction and load problems are recorded as a `failed` run and
    /// returned as `Ok(record)`; `Err` is reserved for the run ledger itself being unusable.
    pub async fn run(&self) -> Result<RunRecord, StoreError> {
        let id = self.sink.start_run(&self.source.label()).await?;
        match self.source.extract().await {
            Ok(extraction) => self.process(id, extraction.rows, extraction.failures).await,
            Err(e) => {
                tracing::error!("run #{id}: extract failed: {e}");
                self.sink.finish_run(id, RunOutcome::failed(format!("extract failed: {e}"), Vec::new())).await
            }
        }
    }

    async fn process(&self, id: i64, rows: Vec<RawRow>, failures: Vec<SourceFailure>) -> Result<RunRecord, StoreError> {
        let cleaned = tokio::task::spawn_blocking(move || clean(&rows)).await;
        let CleanOutcome { readings, rows_in, rows_ok, rejected } = match cleaned {
            Ok(c) => c,
            Err(e) => {
                return self.sink.finish_run(id, RunOutcome::failed(format!("transform failed: {e}"), failures)).await;
            }
        };
        if let Err(e) = self.sink.load(readings, self.anomaly).await {
            tracing::error!("run #{id}: load failed: {e}");
            let outcome = RunOutcome {
                rows_in,
                rows_rejected: rejected.len(),
                ..RunOutcome::failed(format!("load failed: {e}"), failures)
            };
            return self.sink.finish_run(id, outcome).await;
        }

        let mut notes = Vec::new();
        if !rejected.is_empty() {
            let sample: Vec<String> = rejected.iter().take(3).map(|r| format!("row {}: {}", r.row, r.reason)).collect();
            notes.push(format!("{} rows rejected; e.g. {}", rejected.len(), sample.join("; ")));
        }
        if !failures.is_empty() {
            let detail: Vec<String> = failures.iter().map(|f| format!("{} ({})", f.source, f.error)).collect();
            notes.push(format!("{} source(s) failed: {}", failures.len(), detail.join("; ")));
        }
        let status = if notes.is_empty() { RunStatus::Success } else { RunStatus::Partial };
        let outcome = RunOutcome {
            status,
            rows_in,
            rows_out: rows_ok,
            rows_rejected: rejected.len(),
            message: (!notes.is_empty()).then(|| notes.join(" | ")),
            failures,
        };
        self.sink.finish_run(id, outcome).await
    }
}
