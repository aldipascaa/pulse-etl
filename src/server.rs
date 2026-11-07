//! HTTP layer: JSON API plus the embedded dashboard.
//!
//! Every database call goes through [`Db::blocking`], so handlers never block the async runtime.
//! All errors are returned as `{"error": "<message>", "code": "<machine_code>"}`.

use axum::extract::{FromRequestParts, Query, State};
use axum::http::request::Parts;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::NaiveDate;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::db::{Db, Overview, SeriesFilter, DEFAULT_SERIES_LIMIT, MAX_SERIES_LIMIT};
use crate::error::StoreError;
use crate::model::{Metric, RunRecord};
use crate::scheduler::Runner;

const MAX_CITY_LEN: usize = 100;
const DEFAULT_RUNS_LIMIT: usize = 25;
const MAX_RUNS_LIMIT: usize = 500;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub runner: Runner,
    /// When set, `POST /api/ingest` requires `Authorization: Bearer <token>`.
    api_token: Option<Arc<str>>,
}

impl AppState {
    pub fn new(db: Db, runner: Runner) -> Self {
        AppState { db, runner, api_token: None }
    }

    /// Protect the mutating endpoint with a bearer token. An empty token is treated as "no token".
    pub fn with_api_token(mut self, token: Option<String>) -> Self {
        self.api_token = token.filter(|t| !t.is_empty()).map(Arc::from);
        self
    }

    fn authorize(&self, headers: &HeaderMap) -> Result<(), ApiError> {
        let Some(expected) = &self.api_token else {
            return Ok(());
        };
        let presented = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, token)| token.trim());
        match presented {
            Some(t) if constant_time_eq(t.as_bytes(), expected.as_bytes()) => Ok(()),
            Some(_) => Err(ApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", "invalid API token")),
            None => Err(ApiError::new(StatusCode::UNAUTHORIZED, "unauthorized", "missing bearer token")),
        }
    }
}

/// Compare secrets without an early exit on the first differing byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= usize::from(a.get(i).copied().unwrap_or(0) ^ b.get(i).copied().unwrap_or(0));
    }
    diff == 0
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        ApiError { status, code, message: message.into() }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(json!({ "error": self.message, "code": self.code }));
        if self.status == StatusCode::UNAUTHORIZED {
            return (self.status, [(header::WWW_AUTHENTICATE, "Bearer")], body).into_response();
        }
        (self.status, body).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        tracing::error!("request failed: {e}");
        ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", "internal error")
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

/// `Query` whose rejection is our JSON error instead of axum's plain-text one.
pub struct ApiQuery<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequestParts<S> for ApiQuery<T> {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(v)| ApiQuery(v))
            .map_err(|e| ApiError::bad_request(format!("invalid query string: {}", e.body_text())))
    }
}

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
        .fallback(not_found)
        .with_state(state)
}

async fn asset(content_type: &'static str, body: &'static str) -> impl IntoResponse {
    ([(header::CONTENT_TYPE, content_type), (header::CACHE_CONTROL, "no-cache")], body)
}

async fn not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not_found", "no such route")
}

async fn health(State(s): State<AppState>) -> Response {
    let probe = s
        .db
        .blocking(|db| {
            db.ping()?;
            Ok((db.runs(1)?, db.schema_version()?))
        })
        .await;
    match probe {
        Ok((runs, schema_version)) => Json(json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "database": "ok",
            "schema_version": schema_version,
            "source": s.runner.source_label(),
            "ingest_protected": s.api_token.is_some(),
            "last_run": runs.into_iter().next(),
        }))
        .into_response(),
        Err(e) => {
            tracing::error!("health check failed: {e}");
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "status": "degraded", "database": "error" }))).into_response()
        }
    }
}

async fn metrics(State(s): State<AppState>) -> ApiResult<Overview> {
    Ok(Json(s.db.blocking(|db| db.overview()).await?))
}

#[derive(Deserialize)]
struct SeriesParams {
    city: Option<String>,
    metric: Option<String>,
    /// First day (inclusive), `YYYY-MM-DD`.
    from: Option<String>,
    /// Last day (inclusive), `YYYY-MM-DD`.
    to: Option<String>,
    /// Page size; default 366, clamped to 1..=1000.
    limit: Option<u32>,
    offset: Option<u32>,
}

fn parse_day(name: &str, v: Option<String>) -> Result<Option<String>, ApiError> {
    match v.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        None => Ok(None),
        Some(s) => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map(|d| Some(d.format("%Y-%m-%d").to_string()))
            .map_err(|_| ApiError::bad_request(format!("'{name}' must be a date like 2026-10-31"))),
    }
}

async fn series(State(s): State<AppState>, ApiQuery(p): ApiQuery<SeriesParams>) -> ApiResult<serde_json::Value> {
    let city = p
        .city
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .ok_or_else(|| ApiError::bad_request("query parameter 'city' is required"))?;
    if city.chars().count() > MAX_CITY_LEN {
        return Err(ApiError::bad_request(format!("'city' is longer than {MAX_CITY_LEN} characters")));
    }
    let metric = match p.metric.as_deref() {
        None | Some("") => Metric::Temperature,
        Some(m) => Metric::parse(m).ok_or_else(|| ApiError::bad_request("unknown metric; use temperature, humidity or wind"))?,
    };
    let (from, to) = (parse_day("from", p.from)?, parse_day("to", p.to)?);
    if let (Some(f), Some(t)) = (&from, &to) {
        if f > t {
            return Err(ApiError::bad_request("'from' must not be after 'to'"));
        }
    }
    let filter = SeriesFilter {
        from,
        to,
        limit: p.limit.unwrap_or(DEFAULT_SERIES_LIMIT).clamp(1, MAX_SERIES_LIMIT),
        offset: p.offset.unwrap_or(0),
    };
    let (limit, offset) = (filter.limit, filter.offset);
    let (city_q, filter_q) = (city.clone(), filter.clone());
    let page = s.db.blocking(move |db| db.series_page(&city_q, metric, &filter_q)).await?;
    Ok(Json(json!({
        "city": city,
        "metric": metric,
        "unit": metric.unit(),
        "label": metric.label(),
        "from": filter.from,
        "to": filter.to,
        "total": page.total,
        "limit": limit,
        "offset": offset,
        "points": page.points,
    })))
}

#[derive(Deserialize)]
struct RunsParams {
    /// Number of runs, newest first; default 25, clamped to 1..=500.
    limit: Option<usize>,
}

async fn runs(State(s): State<AppState>, ApiQuery(p): ApiQuery<RunsParams>) -> ApiResult<Vec<RunRecord>> {
    let limit = p.limit.unwrap_or(DEFAULT_RUNS_LIMIT).clamp(1, MAX_RUNS_LIMIT);
    Ok(Json(s.db.blocking(move |db| db.runs(limit)).await?))
}

async fn ingest(State(s): State<AppState>, headers: HeaderMap) -> Result<Json<RunRecord>, ApiError> {
    s.authorize(&headers)?;
    match s.runner.trigger().await? {
        Some(rec) => Ok(Json(rec)),
        None => Err(ApiError::new(StatusCode::CONFLICT, "run_in_progress", "an ingest run is already in progress")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_normal_equality() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }
}
