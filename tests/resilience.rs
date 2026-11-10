//! Failure handling, run tracking, scheduler lifecycle and async correctness.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use common::{pipeline, sample};
use pulse_etl::db::Db;
use pulse_etl::error::{ExtractError, StoreError};
use pulse_etl::extract::{City, CsvSource, Extraction, MemorySource, OpenMeteoSource, Source};
use pulse_etl::model::{Metric, RawRow, Reading, RunRecord, RunStatus};
use pulse_etl::pipeline::{Pipeline, RunOutcome, Sink};
use pulse_etl::scheduler::{run_scheduler, Runner};
use pulse_etl::transform::AnomalyConfig;
use serde_json::json;
use tokio_util::sync::CancellationToken;

// ---------- mock Open-Meteo: Tokyo (lat 35.68) always errors ----------

async fn mock_open_meteo(Query(q): Query<std::collections::HashMap<String, String>>) -> impl IntoResponse {
    if q.get("latitude").is_some_and(|l| l.starts_with("35.68")) {
        return (StatusCode::INTERNAL_SERVER_ERROR, "upstream exploded").into_response();
    }
    let time: Vec<String> = (0..48).map(|h| format!("2026-10-{:02}T{:02}:00", 1 + h / 24, h % 24)).collect();
    let temp: Vec<f64> = (0..48).map(|h| 10.0 + f64::from(h) * 0.1).collect();
    Json(json!({"hourly": {
        "time": time, "temperature_2m": temp,
        "relative_humidity_2m": vec![70.0; 48], "wind_speed_10m": vec![5.0; 48]
    }}))
    .into_response()
}

async fn spawn_mock() -> String {
    let app = Router::new().route("/", get(mock_open_meteo));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

fn city(name: &str, lat: f64, lon: f64) -> City {
    City::new(name, lat, lon)
}

fn open_meteo(url: String, cities: Vec<City>) -> OpenMeteoSource {
    OpenMeteoSource::new(url, 2).unwrap().with_retry(2, Duration::from_millis(1)).with_cities(cities)
}

#[tokio::test]
async fn partial_api_failure_is_recorded_on_the_run() {
    let url = spawn_mock().await;
    let db = Db::open_in_memory().unwrap();
    let src = open_meteo(url, vec![city("Berlin", 52.52, 13.41), city("Tokyo", 35.68, 139.69)]);
    let run = pipeline(&db, src).run().await.unwrap();

    assert_eq!(run.status, RunStatus::Partial, "{run:?}");
    assert_eq!(run.failures.len(), 1);
    assert_eq!(run.failures[0].source, "Tokyo");
    assert!(run.failures[0].error.contains("500"), "{:?}", run.failures[0]);
    assert!(run.message.as_deref().unwrap().contains("Tokyo"));
    assert_eq!(run.rows_rejected, 0);
    assert_eq!(run.rows_out, 48);

    // The healthy city was still loaded, and the failure is durable, not just a log line.
    assert_eq!(db.series("Berlin", Metric::Temperature).unwrap().len(), 2);
    assert!(db.series("Tokyo", Metric::Temperature).unwrap().is_empty());
    let stored = &db.runs(1).unwrap()[0];
    assert_eq!(stored.failures, run.failures);
}

#[tokio::test]
async fn total_api_failure_fails_the_run() {
    let url = spawn_mock().await;
    let db = Db::open_in_memory().unwrap();
    let run = pipeline(&db, open_meteo(url, vec![city("Tokyo", 35.68, 139.69)])).run().await.unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert!(run.message.unwrap().contains("unreachable for all cities (Tokyo)"));
    assert_eq!(db.count_readings().unwrap(), 0);
}

#[tokio::test]
async fn healthy_api_run_is_a_success() {
    let url = spawn_mock().await;
    let db = Db::open_in_memory().unwrap();
    let run = pipeline(&db, open_meteo(url, vec![city("Berlin", 52.52, 13.41)])).run().await.unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert!(run.failures.is_empty() && run.message.is_none());
}

// ---------- the pipeline works against any Sink ----------

#[derive(Default)]
struct RecordingSink {
    loaded: Mutex<Vec<Reading>>,
    finished: Mutex<Vec<RunOutcome>>,
    fail_load: bool,
}

#[async_trait]
impl Sink for RecordingSink {
    async fn start_run(&self, _source: &str) -> Result<i64, StoreError> {
        Ok(1)
    }
    async fn load(&self, readings: Vec<Reading>, _a: AnomalyConfig) -> Result<(), StoreError> {
        if self.fail_load {
            return Err(StoreError::Join("disk full".into()));
        }
        self.loaded.lock().unwrap().extend(readings);
        Ok(())
    }
    async fn finish_run(&self, id: i64, o: RunOutcome) -> Result<RunRecord, StoreError> {
        self.finished.lock().unwrap().push(o.clone());
        Ok(RunRecord {
            id,
            source: "mem".into(),
            started_at: String::new(),
            finished_at: None,
            rows_in: o.rows_in as i64,
            rows_out: o.rows_out as i64,
            rows_rejected: o.rows_rejected as i64,
            status: o.status,
            message: o.message,
            failures: o.failures,
        })
    }
}

fn rows() -> Vec<RawRow> {
    let row = |t: &str| RawRow {
        city: Some("Oslo".into()),
        timestamp: Some("2026-10-01T00:00".into()),
        temperature: Some(t.into()),
        ..Default::default()
    };
    vec![row("5"), row("oops")]
}

#[tokio::test]
async fn pipeline_runs_against_a_fake_sink() {
    let sink = Arc::new(RecordingSink::default());
    let p = Pipeline::new(Arc::new(MemorySource::new("mem", rows())), sink.clone(), AnomalyConfig::default());
    let run = p.run().await.unwrap();
    assert_eq!((run.status, run.rows_in, run.rows_out, run.rows_rejected), (RunStatus::Partial, 2, 1, 1));
    assert_eq!(sink.loaded.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn load_failure_closes_the_run_as_failed() {
    let sink = Arc::new(RecordingSink { fail_load: true, ..Default::default() });
    let p = Pipeline::new(Arc::new(MemorySource::new("mem", rows())), sink.clone(), AnomalyConfig::default());
    let run = p.run().await.unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert!(run.message.unwrap().contains("load failed"));
    assert_eq!(sink.finished.lock().unwrap().len(), 1);
}

// ---------- runner + scheduler lifecycle ----------

struct SlowSource(Duration);

#[async_trait]
impl Source for SlowSource {
    fn label(&self) -> String {
        "slow".into()
    }
    async fn extract(&self) -> Result<Extraction, ExtractError> {
        tokio::time::sleep(self.0).await;
        Ok(Extraction::default())
    }
}

async fn wait_for_status(db: &Db, status: RunStatus) -> RunRecord {
    for _ in 0..200 {
        if let Some(r) = db.runs(1).unwrap().into_iter().next() {
            if r.status == status {
                return r;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run never reached {status}");
}

#[tokio::test]
async fn overlapping_trigger_is_rejected_and_run_survives_caller_drop() {
    let db = Db::open_in_memory().unwrap();
    let runner = Runner::new(pipeline(&db, SlowSource(Duration::from_millis(300))));

    // The caller gives up (as a disconnecting HTTP client would) while the run is in flight.
    let abandoned = tokio::time::timeout(Duration::from_millis(50), runner.trigger()).await;
    assert!(abandoned.is_err(), "trigger should still be running");

    // Overlap protection still holds because the run task owns the lock.
    assert!(runner.trigger().await.unwrap().is_none());

    // ...and the run is completed and recorded rather than stuck in 'running'.
    let rec = wait_for_status(&db, RunStatus::Success).await;
    assert!(rec.finished_at.is_some());
    assert_eq!(db.runs(10).unwrap().len(), 1);

    // Lock released afterwards.
    assert!(runner.trigger().await.unwrap().is_some());
}

#[tokio::test]
async fn scheduler_runs_immediately_then_stops_on_cancel() {
    let db = Db::open_in_memory().unwrap();
    let runner = Runner::new(pipeline(&db, CsvSource::new(sample())));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(run_scheduler(runner, Duration::from_secs(3600), cancel.clone()));

    wait_for_status(&db, RunStatus::Partial).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle).await.expect("scheduler stops promptly").unwrap();
    assert_eq!(db.runs(10).unwrap().len(), 1);
}

#[tokio::test]
async fn scheduler_finishes_in_flight_run_before_stopping() {
    let db = Db::open_in_memory().unwrap();
    let runner = Runner::new(pipeline(&db, SlowSource(Duration::from_millis(200))));
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(run_scheduler(runner, Duration::from_secs(3600), cancel.clone()));
    wait_for_status(&db, RunStatus::Running).await;
    cancel.cancel();
    handle.await.unwrap();
    // The run that was in flight was closed properly, not abandoned.
    assert_eq!(db.runs(1).unwrap()[0].status, RunStatus::Success);
}

#[tokio::test]
async fn scheduler_does_not_start_when_already_cancelled() {
    let db = Db::open_in_memory().unwrap();
    let runner = Runner::new(pipeline(&db, SlowSource(Duration::from_millis(1))));
    let cancel = CancellationToken::new();
    cancel.cancel();
    run_scheduler(runner, Duration::from_secs(1), cancel).await;
    assert!(db.runs(1).unwrap().is_empty());
}

// ---------- async correctness ----------

#[tokio::test(flavor = "current_thread")]
async fn blocking_db_work_does_not_stall_the_runtime() {
    let db = Db::open_in_memory().unwrap();
    let ticks = Arc::new(AtomicUsize::new(0));
    let t = ticks.clone();
    let ticker = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(5)).await;
            t.fetch_add(1, Ordering::SeqCst);
        }
    });
    // 300 ms of synchronous work on a single-threaded runtime: if it ran on the runtime
    // thread the ticker could not advance at all.
    db.blocking(|_| {
        std::thread::sleep(Duration::from_millis(300));
        Ok(())
    })
    .await
    .unwrap();
    ticker.abort();
    assert!(ticks.load(Ordering::SeqCst) >= 10, "ticks = {}", ticks.load(Ordering::SeqCst));
}
