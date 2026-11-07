//! Extract stage: local CSV files and the Open-Meteo public API.

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::model::RawRow;

pub const OPEN_METEO_URL: &str = "https://api.open-meteo.com/v1/forecast";

pub struct City {
    pub name: &'static str,
    pub lat: f64,
    pub lon: f64,
}

pub const CITIES: &[City] = &[
    City { name: "Berlin", lat: 52.52, lon: 13.41 },
    City { name: "Tokyo", lat: 35.68, lon: 139.69 },
    City { name: "New York", lat: 40.71, lon: -74.01 },
    City { name: "Sydney", lat: -33.87, lon: 151.21 },
    City { name: "Nairobi", lat: -1.29, lon: 36.82 },
];

/// Read a CSV with the header `city,timestamp,temperature,unit,humidity,wind_speed`.
/// Malformed lines become empty rows so the transform stage rejects and counts them.
pub fn read_csv(path: &Path) -> Result<Vec<RawRow>> {
    let mut rdr = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("opening CSV {}", path.display()))?;
    Ok(rdr.deserialize::<RawRow>().map(|r| r.unwrap_or_default()).collect())
}

/// Run `op` up to `attempts` times, sleeping `base * 2^n` between failures.
pub async fn retry<T, F, Fut>(attempts: u32, base: Duration, mut op: F) -> Result<T>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let attempts = attempts.max(1);
    let mut n = 0;
    loop {
        match op(n).await {
            Ok(v) => return Ok(v),
            Err(e) if n + 1 >= attempts => return Err(e.context(format!("giving up after {attempts} attempts"))),
            Err(e) => {
                let wait = base * 2u32.pow(n);
                tracing::warn!("attempt {} failed: {e:#}; retrying in {wait:?}", n + 1);
                tokio::time::sleep(wait).await;
                n += 1;
            }
        }
    }
}

#[derive(Deserialize)]
struct OmResponse {
    hourly: OmHourly,
}

#[derive(Deserialize)]
struct OmHourly {
    time: Vec<String>,
    temperature_2m: Vec<Option<f64>>,
    relative_humidity_2m: Vec<Option<f64>>,
    wind_speed_10m: Vec<Option<f64>>,
}

fn om_to_rows(city: &str, h: OmHourly) -> Vec<RawRow> {
    let s = |v: Option<f64>| v.map(|x| x.to_string());
    h.time
        .into_iter()
        .enumerate()
        .map(|(i, t)| RawRow {
            city: Some(city.to_string()),
            timestamp: Some(t),
            temperature: s(h.temperature_2m.get(i).copied().flatten()),
            unit: Some("C".into()),
            humidity: s(h.relative_humidity_2m.get(i).copied().flatten()),
            wind_speed: s(h.wind_speed_10m.get(i).copied().flatten()),
        })
        .collect()
}

/// Fetch hourly data for all [`CITIES`]. Each city is retried with exponential backoff;
/// the call fails only if no city could be fetched.
pub async fn fetch_open_meteo(base_url: &str, past_days: u32, attempts: u32, backoff: Duration) -> Result<Vec<RawRow>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent(concat!("pulse-etl/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let mut rows = Vec::new();
    let mut failures = Vec::new();
    for c in CITIES {
        let url = format!(
            "{base_url}?latitude={}&longitude={}&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&past_days={past_days}&forecast_days=1&timezone=UTC",
            c.lat, c.lon
        );
        let res = retry(attempts, backoff, |_| {
            let (client, url) = (client.clone(), url.clone());
            async move {
                let resp: OmResponse = client.get(&url).send().await?.error_for_status()?.json().await?;
                Ok(resp)
            }
        })
        .await;
        match res {
            Ok(r) => rows.extend(om_to_rows(c.name, r.hourly)),
            Err(e) => {
                tracing::error!("fetching {} failed: {e:#}", c.name);
                failures.push(c.name);
            }
        }
    }
    if rows.is_empty() {
        bail!("Open-Meteo unreachable for all cities ({})", failures.join(", "));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn retry_succeeds_after_transient_failures() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let out = retry(4, Duration::from_millis(1), move |_| {
            let c = c.clone();
            async move {
                if c.fetch_add(1, Ordering::SeqCst) < 2 {
                    bail!("boom")
                }
                Ok(42)
            }
        })
        .await
        .unwrap();
        assert_eq!(out, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_gives_up() {
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let res: Result<()> = retry(3, Duration::from_millis(1), move |_| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                bail!("down")
            }
        })
        .await;
        assert!(res.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn open_meteo_json_maps_to_rows_with_nulls() {
        let json = r#"{"hourly":{"time":["2026-10-01T00:00","2026-10-01T01:00"],
            "temperature_2m":[10.5,null],"relative_humidity_2m":[80,81],"wind_speed_10m":[5.2,6.0]}}"#;
        let r: OmResponse = serde_json::from_str(json).unwrap();
        let rows = om_to_rows("Berlin", r.hourly);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].temperature.as_deref(), Some("10.5"));
        assert_eq!(rows[1].temperature, None);
        assert_eq!(rows[1].humidity.as_deref(), Some("81"));
    }
}
