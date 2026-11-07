use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use pulse_etl::db::Db;
use pulse_etl::pipeline::{PipelineConfig, Source};
use pulse_etl::server::{router, AppState};
use serde_json::Value;
use tower::ServiceExt;

async fn call(app: &axum::Router, method: &str, uri: &str) -> (StatusCode, Value) {
    let req = Request::builder().method(method).uri(uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn api_endpoints_serve_ingested_data() {
    let csv = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/sample_weather.csv");
    let state = AppState::new(Db::open_in_memory().unwrap(), Source::Csv(csv), PipelineConfig::default());
    let app = router(state);

    let (st, health) = call(&app, "GET", "/api/health").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(health["status"], "ok");

    // "Run now"
    let (st, run) = call(&app, "POST", "/api/ingest").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(run["status"], "partial");

    let (_, m) = call(&app, "GET", "/api/metrics").await;
    assert_eq!(m["cities"].as_array().unwrap().len(), 4);
    assert!(m["totals"]["anomalies"].as_i64().unwrap() >= 3);
    assert_eq!(m["last_run"]["id"], run["id"]);

    let (st, s) = call(&app, "GET", "/api/series?city=Berlin&metric=temperature").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s["points"].as_array().unwrap().len(), 21);
    assert_eq!(s["unit"], "°C");

    let (st, e) = call(&app, "GET", "/api/series?metric=wind").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert!(e["error"].as_str().unwrap().contains("city"));
    let (st, _) = call(&app, "GET", "/api/series?city=Berlin&metric=nope").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (_, runs) = call(&app, "GET", "/api/runs?limit=5").await;
    assert_eq!(runs.as_array().unwrap().len(), 1);
}
