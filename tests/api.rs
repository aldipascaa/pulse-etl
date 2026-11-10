mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{csv_pipeline, sample};
use http_body_util::BodyExt;
use pulse_etl::db::Db;
use pulse_etl::scheduler::Runner;
use pulse_etl::server::{router, AppState};
use serde_json::Value;
use tower::ServiceExt;

async fn call_with(app: &axum::Router, method: &str, uri: &str, auth: Option<&str>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(a) = auth {
        req = req.header("authorization", a);
    }
    let resp = app.clone().oneshot(req.body(Body::empty()).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

async fn call(app: &axum::Router, method: &str, uri: &str) -> (StatusCode, Value) {
    call_with(app, method, uri, None).await
}

fn app(token: Option<&str>) -> axum::Router {
    let db = Db::open_in_memory().unwrap();
    let runner = Runner::new(csv_pipeline(&db, sample()));
    router(AppState::new(db, runner).with_api_token(token.map(String::from)))
}

#[tokio::test]
async fn api_endpoints_serve_ingested_data() {
    let app = app(None);

    let (st, health) = call(&app, "GET", "/api/health").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(health["status"], "ok");
    assert_eq!(health["ingest_protected"], false);
    assert_eq!(health["schema_version"], 2);

    // "Run now"
    let (st, run) = call(&app, "POST", "/api/ingest").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(run["status"], "partial");
    assert!(run["failures"].as_array().unwrap().is_empty());

    let (_, m) = call(&app, "GET", "/api/metrics").await;
    assert_eq!(m["cities"].as_array().unwrap().len(), 4);
    assert!(m["totals"]["anomalies"].as_i64().unwrap() >= 3);
    assert_eq!(m["last_run"]["id"], run["id"]);

    let (st, s) = call(&app, "GET", "/api/series?city=Berlin&metric=temperature").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s["points"].as_array().unwrap().len(), 21);
    assert_eq!(s["total"], 21);
    assert_eq!(s["limit"], 366);
    assert_eq!(s["offset"], 0);
    assert_eq!(s["unit"], "\u{b0}C");

    let (_, runs) = call(&app, "GET", "/api/runs?limit=5").await;
    assert_eq!(runs.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn series_supports_pagination_and_date_range() {
    let app = app(None);
    call(&app, "POST", "/api/ingest").await;

    let (_, p) = call(&app, "GET", "/api/series?city=Berlin&limit=5&offset=4").await;
    assert_eq!((p["total"].as_i64(), p["limit"].as_i64(), p["offset"].as_i64()), (Some(21), Some(5), Some(4)));
    let days: Vec<&str> = p["points"].as_array().unwrap().iter().map(|x| x["day"].as_str().unwrap()).collect();
    assert_eq!(days.len(), 5);
    assert!(days.windows(2).all(|w| w[0] < w[1]), "ascending: {days:?}");

    let (_, r) = call(&app, "GET", &format!("/api/series?city=Berlin&from={}&to={}", days[0], days[2])).await;
    assert_eq!(r["total"], 3);

    // limit is clamped to the allowed range rather than trusted
    let (_, big) = call(&app, "GET", "/api/series?city=Berlin&limit=999999").await;
    assert_eq!(big["limit"], 1000);
    let (_, zero) = call(&app, "GET", "/api/series?city=Berlin&limit=0").await;
    assert_eq!(zero["limit"], 1);
}

#[tokio::test]
async fn invalid_requests_get_consistent_json_errors() {
    let app = app(None);
    for (uri, needle) in [
        ("/api/series?metric=wind", "city"),
        ("/api/series?city=Berlin&metric=nope", "metric"),
        ("/api/series?city=Berlin&from=yesterday", "from"),
        ("/api/series?city=Berlin&from=2026-10-05&to=2026-10-01", "after"),
        ("/api/series?city=Berlin&limit=abc", "invalid query"),
        ("/api/runs?limit=-3", "invalid query"),
    ] {
        let (st, e) = call(&app, "GET", uri).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(e["code"], "bad_request", "{uri}");
        assert!(e["error"].as_str().unwrap().contains(needle), "{uri}: {e}");
    }
    let (st, e) = call(&app, "GET", "/api/nope").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(e["code"], "not_found");
}

#[tokio::test]
async fn ingest_requires_token_when_configured() {
    let app = app(Some("s3cret"));

    let (st, e) = call(&app, "POST", "/api/ingest").await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_eq!(e["code"], "unauthorized");
    let (st, _) = call_with(&app, "POST", "/api/ingest", Some("Bearer wrong")).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    let (st, _) = call_with(&app, "POST", "/api/ingest", Some("Basic s3cret")).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    // Nothing ran for the rejected calls.
    let (_, runs) = call(&app, "GET", "/api/runs").await;
    assert!(runs.as_array().unwrap().is_empty());

    let (st, run) = call_with(&app, "POST", "/api/ingest", Some("Bearer s3cret")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(run["status"], "partial");

    // Read endpoints stay public, and health advertises that ingest is protected.
    let (st, h) = call(&app, "GET", "/api/health").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(h["ingest_protected"], true);
}

#[tokio::test]
async fn open_by_default_without_token() {
    let (st, _) = call(&app(None), "POST", "/api/ingest").await;
    assert_eq!(st, StatusCode::OK);
    // An empty token counts as unset instead of "allow the empty bearer".
    let (st, _) = call(&app(Some("")), "POST", "/api/ingest").await;
    assert_eq!(st, StatusCode::OK);
}

#[tokio::test]
async fn export_returns_the_series_as_csv() {
    let app = app(None);
    call(&app, "POST", "/api/ingest").await;

    let (_, series) = call(&app, "GET", "/api/series?city=Berlin&metric=temperature&limit=1000").await;
    let points = series["points"].as_array().unwrap();
    assert!(!points.is_empty());

    let req = Request::builder().uri("/api/export?city=Berlin&metric=temperature").body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-type"], "text/csv; charset=utf-8");
    assert_eq!(resp.headers()["content-disposition"], "attachment; filename=\"pulse-berlin-temperature.csv\"");
    let body = String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines[0], "city,metric,unit,day,min,max,avg,rolling7,zscore,anomaly,readings");
    assert_eq!(lines.len(), points.len() + 1, "one row per day plus the header");
    let first: Vec<&str> = lines[1].split(',').collect();
    assert_eq!(first.len(), 11);
    assert_eq!(first[0], "Berlin");
    assert_eq!(first[3], points[0]["day"].as_str().unwrap());
    assert_eq!(first[6].parse::<f64>().unwrap(), points[0]["avg"].as_f64().unwrap());
    let anomalies = points.iter().filter(|p| p["anomaly"] == true).count();
    assert_eq!(lines[1..].iter().filter(|l| l.split(',').nth(9) == Some("true")).count(), anomalies);

    // A date range narrows the export; an unknown city is just an empty file.
    let day = points[0]["day"].as_str().unwrap();
    let req = Request::builder().uri(format!("/api/export?city=Berlin&from={day}&to={day}")).body(Body::empty()).unwrap();
    let body = app.clone().oneshot(req).await.unwrap().into_body().collect().await.unwrap().to_bytes();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap().lines().count(), 2);
    let req = Request::builder().uri("/api/export?city=Atlantis").body(Body::empty()).unwrap();
    let body = app.clone().oneshot(req).await.unwrap().into_body().collect().await.unwrap().to_bytes();
    assert_eq!(String::from_utf8(body.to_vec()).unwrap().lines().count(), 1);

    // Same validation as /api/series.
    assert_eq!(call(&app, "GET", "/api/export").await.0, StatusCode::BAD_REQUEST);
    assert_eq!(call(&app, "GET", "/api/export?city=Berlin&metric=rain").await.0, StatusCode::BAD_REQUEST);
}
