//! Extract stage: the [`Source`] trait and its implementations (CSV file, Open-Meteo API,
//! in-memory rows for tests).

use std::fmt::Display;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::error::ExtractError;
use crate::model::{RawRow, SourceFailure};

pub const OPEN_METEO_URL: &str = "https://api.open-meteo.com/v1/forecast";

/// What a source hands to the transform stage.
#[derive(Debug, Default)]
pub struct Extraction {
    pub rows: Vec<RawRow>,
    /// Sub-sources that failed while others succeeded (recorded on the run, not just logged).
    pub failures: Vec<SourceFailure>,
}

/// A pluggable data source. Returning `Err` means the whole extraction failed;
/// partial problems go into [`Extraction::failures`].
#[async_trait]
pub trait Source: Send + Sync {
    /// Stable label stored in `runs.source`.
    fn label(&self) -> String;
    async fn extract(&self) -> Result<Extraction, ExtractError>;
}

/// Rows already in memory (tests, embedding).
pub struct MemorySource {
    label: String,
    rows: Vec<RawRow>,
}

impl MemorySource {
    pub fn new(label: impl Into<String>, rows: Vec<RawRow>) -> Self {
        MemorySource { label: label.into(), rows }
    }
}

#[async_trait]
impl Source for MemorySource {
    fn label(&self) -> String {
        self.label.clone()
    }
    async fn extract(&self) -> Result<Extraction, ExtractError> {
        Ok(Extraction { rows: self.rows.clone(), failures: Vec::new() })
    }
}

/// Local CSV file with the header `city,timestamp,temperature,unit,humidity,wind_speed`.
pub struct CsvSource {
    path: PathBuf,
}

impl CsvSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        CsvSource { path: path.into() }
    }
}

#[async_trait]
impl Source for CsvSource {
    fn label(&self) -> String {
        format!("csv:{}", self.path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default())
    }

    async fn extract(&self) -> Result<Extraction, ExtractError> {
        let path = self.path.clone();
        let rows = tokio::task::spawn_blocking(move || read_csv(&path))
            .await
            .map_err(|e| ExtractError::Join(e.to_string()))??;
        Ok(Extraction { rows, failures: Vec::new() })
    }
}

/// Blocking CSV read. Malformed lines become empty rows so the transform stage rejects and counts them.
pub fn read_csv(path: &Path) -> Result<Vec<RawRow>, ExtractError> {
    let open_err = |source| ExtractError::CsvOpen { path: path.to_path_buf(), source };
    let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).flexible(true).from_path(path).map_err(open_err)?;
    Ok(rdr.deserialize::<RawRow>().map(|r| r.unwrap_or_default()).collect())
}

/// Run `op` up to `attempts` times, sleeping `base * 2^n` between failures (capped at 60 s).
pub async fn retry<T, E, F, Fut>(attempts: u32, base: Duration, mut op: F) -> Result<T, E>
where
    E: Display,
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let attempts = attempts.max(1);
    let mut n = 0;
    loop {
        match op(n).await {
            Ok(v) => return Ok(v),
            Err(e) if n + 1 >= attempts => {
                tracing::warn!("giving up after {attempts} attempts: {e}");
                return Err(e);
            }
            Err(e) => {
                let wait = base.saturating_mul(2u32.saturating_pow(n)).min(Duration::from_secs(60));
                tracing::warn!("attempt {} failed: {e}; retrying in {wait:?}", n + 1);
                tokio::time::sleep(wait).await;
                n += 1;
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
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

/// Open-Meteo public API (no key). Every city is retried independently with exponential backoff.
pub struct OpenMeteoSource {
    base_url: String,
    past_days: u32,
    attempts: u32,
    backoff: Duration,
    cities: Vec<City>,
    client: reqwest::Client,
}

impl OpenMeteoSource {
    pub fn new(base_url: impl Into<String>, past_days: u32) -> Result<Self, ExtractError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent(concat!("pulse-etl/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| ExtractError::Client(e.to_string()))?;
        Ok(OpenMeteoSource {
            base_url: base_url.into(),
            past_days,
            attempts: 4,
            backoff: Duration::from_secs(2),
            cities: CITIES.to_vec(),
            client,
        })
    }

    pub fn with_retry(mut self, attempts: u32, backoff: Duration) -> Self {
        self.attempts = attempts;
        self.backoff = backoff;
        self
    }

    pub fn with_cities(mut self, cities: Vec<City>) -> Self {
        self.cities = cities;
        self
    }
}

#[async_trait]
impl Source for OpenMeteoSource {
    fn label(&self) -> String {
        "open-meteo".to_string()
    }

    /// Fetch hourly data for every configured city. Cities that still fail after the retries are
    /// reported in [`Extraction::failures`]; the call itself fails only if no city could be fetched.
    async fn extract(&self) -> Result<Extraction, ExtractError> {
        let mut out = Extraction::default();
        for c in &self.cities {
            let url = format!(
                "{}?latitude={}&longitude={}&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&past_days={}&forecast_days=1&timezone=UTC",
                self.base_url, c.lat, c.lon, self.past_days
            );
            let res = retry(self.attempts, self.backoff, |_| {
                let (client, url) = (self.client.clone(), url.clone());
                async move {
                    let resp: OmResponse = client.get(&url).send().await?.error_for_status()?.json().await?;
                    Ok::<_, reqwest::Error>(resp)
                }
            })
            .await;
            match res {
                Ok(r) => out.rows.extend(om_to_rows(c.name, r.hourly)),
                Err(e) => {
                    tracing::error!("fetching {} failed: {e}", c.name);
                    out.failures.push(SourceFailure { source: c.name.to_string(), error: e.to_string() });
                }
            }
        }
        if out.rows.is_empty() && !out.failures.is_empty() {
            let names: Vec<&str> = out.failures.iter().map(|f| f.source.as_str()).collect();
            return Err(ExtractError::AllSourcesFailed(format!(
                "Open-Meteo unreachable for all cities ({})",
                names.join(", ")
            )));
        }
        Ok(out)
    }
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
        let out: i32 = retry(4, Duration::from_millis(1), move |_| {
            let c = c.clone();
            async move {
                if c.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err("boom".to_string())
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
        let res: Result<(), String> = retry(3, Duration::from_millis(1), move |_| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err("down".to_string())
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
