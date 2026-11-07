//! HTTP layer: JSON API plus the embedded dashboard.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

use crate::db::Db;
use crate::model::{Metric, RunRecord};
use crate::pipeline::{run_ingest, PipelineConfig, Source};

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub source: Arc<Source>,
    pub cfg: Arc<PipelineConfig>,
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl AppState {
    pub fn new(db: Db, source: Source, cfg: PipelineConfig) -> Self {
        AppState { db, source: Arc::new(source), cfg: Arc::new(cfg), lock: Arc::new(tokio::sync::Mutex::new(())) }
    }

    /// Run the pipeline once. Returns `None` if another run is already in progress.
    pub async fn trigger(&self) -> Result<Option<RunRecord>> {
        let Ok(_guard) = self.lock.try_lock() else {
            return Ok(None);
        };
        Ok(Some(run_ingest(&self.db, &self.source, &self.cfg).await?))
    }
}

/// Periodically ingest. The first tick fires immediately.
pub async fn run_scheduler(state: AppState, every: Duration) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match state.trigger().await {
            Ok(Some(r)) => tracing::info!(
                "scheduled run #{} {}: {} in, {} ok, {} rejected",
                r.id, r.status, r.rows_in, r.rows_out, r.rows_rejected
            ),
            Ok(None) => tracing::info!("scheduled run skipped: another run is in progress"),
            Err(e) => tracing::error!("scheduled run error: {e:#}"),
        }
    }
}

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("request failed: {e:#}");
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| asset("text/html; charset=utf-8", include_str!("../static/index.html"))))
        .route("/app.js", get(|| asset("text/javascript; charset=utf-8", include_str!("../static/app.js"))))
        .route("/style.css", get(|| asset("text/css; charset=utf-8", include_str!("../static/style.css"))))
        .route("/favicon.svg", get(|| asset("image/svg+xml", include_str!("../static/favicon.svg"))))
        .route("/api/health", get(health))
        .route("/api/metrics", get(metrics))
        .route("/api/series", get(series))
        .route("/api/runs", get(runs))
        .route("/api/ingest", post(ingest))
        .with_state(state)
}

async fn asset(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    ([(header::CONTENT_TYPE, content_type), (header::CACHE_CONTROL, "no-cache")], body)
}

async fn health(State(s): State<AppState>) -> Response {
    match s.db.ping().and_then(|_| s.db.runs(1)) {
        Ok(runs) => Json(json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "database": "ok",
            "source": s.source.label(),
            "last_run": runs.into_iter().next(),
        }))
        .into_response(),
        Err(e) => {
            tracing::error!("health check failed: {e:#}");
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "degraded", "database": "error" }))).into_response()
        }
    }
}

async fn metrics(State(s): State<AppState>) -> ApiResult<crate::db::Overview> {
    Ok(Json(s.db.overview()?))
}

#[derive(Deserialize)]
struct SeriesParams {
    city: Option<String>,
    metric: Option<String>,
}

async fn series(State(s): State<AppState>, Query(p): Query<SeriesParams>) -> ApiResult<serde_json::Value> {
    let bad = |m: &str| ApiError(StatusCode::BAD_REQUEST, m.to_string());
    let city = p.city.filter(|c| !c.trim().is_empty()).ok_or_else(|| bad("query parameter 'city' is required"))?;
    let metric = match p.metric.as_deref() {
        None | Some("") => Metric::Temperature,
        Some(m) => Metric::parse(m).ok_or_else(|| bad("unknown metric; use temperature, humidity or wind"))?,
    };
    let points = s.db.series(&city, metric)?;
    Ok(Json(json!({ "city": city, "metric": metric, "unit": metric.unit(), "label": metric.label(), "points": points })))
}

#[derive(Deserialize)]
struct RunsParams {
    limit: Option<usize>,
}

async fn runs(State(s): State<AppState>, Query(p): Query<RunsParams>) -> ApiResult<Vec<RunRecord>> {
    Ok(Json(s.db.runs(p.limit.unwrap_or(25).clamp(1, 500))?))
}

async fn ingest(State(s): State<AppState>) -> Response {
    match s.trigger().await {
        Ok(Some(rec)) => Json(rec).into_response(),
        Ok(None) => ApiError(StatusCode::CONFLICT, "an ingest run is already in progress".into()).into_response(),
        Err(e) => ApiError::from(e).into_response(),
    }
}
