//! Run coordination: at most one pipeline run at a time, however it was triggered
//! (schedule or API), plus the periodic scheduler with cooperative shutdown.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::error::StoreError;
use crate::model::RunRecord;
use crate::pipeline::Pipeline;

/// Serialises pipeline runs. Cheap to clone.
#[derive(Clone)]
pub struct Runner {
    pipeline: Arc<Pipeline>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl Runner {
    pub fn new(pipeline: Arc<Pipeline>) -> Self {
        Runner { pipeline, lock: Arc::new(tokio::sync::Mutex::new(())) }
    }

    pub fn source_label(&self) -> String {
        self.pipeline.source_label()
    }

    /// Run the pipeline once. Returns `Ok(None)` if another run is already in progress.
    ///
    /// The run executes on its own task: if the caller goes away (client disconnect, shutdown
    /// of a request future) the run still finishes and its `runs` row is closed properly
    /// instead of being left in `running`.
    pub async fn trigger(&self) -> Result<Option<RunRecord>, StoreError> {
        let Ok(guard) = self.lock.clone().try_lock_owned() else {
            return Ok(None);
        };
        let pipeline = self.pipeline.clone();
        let handle = tokio::spawn(async move {
            let _guard = guard;
            pipeline.run().await
        });
        match handle.await {
            Ok(res) => res.map(Some),
            Err(e) => Err(StoreError::Join(e.to_string())),
        }
    }
}

/// Ingest every `every`, starting immediately, until `cancel` fires. A run that is already in
/// flight when cancellation arrives is allowed to finish; no new run is started afterwards.
pub async fn run_scheduler(runner: Runner, every: Duration, cancel: CancellationToken) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {}
        }
        if cancel.is_cancelled() {
            break;
        }
        match runner.trigger().await {
            Ok(Some(r)) => tracing::info!(
                "scheduled run #{} {}: {} in, {} ok, {} rejected, {} source failure(s)",
                r.id, r.status, r.rows_in, r.rows_out, r.rows_rejected, r.failures.len()
            ),
            Ok(None) => tracing::info!("scheduled run skipped: another run is in progress"),
            Err(e) => tracing::error!("scheduled run error: {e}"),
        }
    }
    tracing::info!("scheduler stopped");
}
