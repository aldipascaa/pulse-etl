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

/// A place to fetch weather for.
#[derive(Debug, Clone, PartialEq)]
pub struct City {
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

impl City {
    pub fn new(name: impl Into<String>, lat: f64, lon: f64) -> City {
        City { name: name.into(), lat, lon }
    }
}

/// The cities fetched when none are configured.
pub fn default_cities() -> Vec<City> {
    vec![
        City::new("Berlin", 52.52, 13.41),
        City::new("Tokyo", 35.68, 139.69),
        City::new("New York", 40.71, -74.01),
        City::new("Sydney", -33.87, 151.21),
        City::new("Nairobi", -1.29, 36.82),
    ]
}

/// Most cities one run may fetch (they are requested one after another).
pub const MAX_CITIES: usize = 25;

/// Parse a city list like `Jakarta:-6.2:106.8;Paris:48.85:2.35`.
/// Names must be unique (case-insensitive); latitude is -90..=90 and longitude -180..=180.
pub fn parse_cities(spec: &str) -> Result<Vec<City>, String> {
    let mut out: Vec<City> = Vec::new();
    for part in spec.split(';').map(str::trim).filter(|p| !p.is_empty()) {
        let fields: Vec<&str> = part.split(':').map(str::trim).collect();
        let [name, lat, lon] = fields[..] else {
            return Err(format!("'{part}' is not NAME:LAT:LON"));
        };
        if name.is_empty() || name.chars().count() > 60 {
            return Err(format!("city name in '{part}' must be 1-60 characters"));
        }
        let coord = |what: &str, v: &str, max: f64| -> Result<f64, String> {
            match v.parse::<f64>() {
                Ok(x) if x.is_finite() && x.abs() <= max => Ok(x),
                _ => Err(format!("{what} '{v}' of {name} must be a number between -{max} and {max}")),
            }
        };
        let city = City::new(name, coord("latitude", lat, 90.0)?, coord("longitude", lon, 180.0)?);
        if out.iter().any(|c| c.name.eq_ignore_ascii_case(&city.name)) {
            return Err(format!("city '{name}' is listed twice"));
        }
        out.push(city);
    }
    if out.is_empty() {
        return Err("no cities given; expected NAME:LAT:LON[;NAME:LAT:LON...]".into());
    }
    if out.len() > MAX_CITIES {
        return Err(format!("at most {MAX_CITIES} cities are supported"));
    }
    Ok(out)
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
            cities: default_cities(),
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
                Ok(r) => out.rows.extend(om_to_rows(&c.name, r.hourly)),
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

    #[test]
    fn parse_cities_accepts_a_valid_list() {
        let c = parse_cities(" Jakarta:-6.2:106.8 ; New York : 40.71 : -74.01 ;").unwrap();
        assert_eq!(c, vec![City::new("Jakarta", -6.2, 106.8), City::new("New York", 40.71, -74.01)]);
    }

    #[test]
    fn parse_cities_rejects_bad_input() {
        for bad in ["", " ; ", "Paris", "Paris:48.85", "Paris:48.85:2.35:9", ":1:2", "Paris:north:2", "Paris:91:2", "Paris:48:181", "Paris:NaN:2", "Paris:1:2;paris:3:4"] {
            assert!(parse_cities(bad).is_err(), "{bad:?} should be rejected");
        }
        let many = (0..=MAX_CITIES).map(|i| format!("c{i}:1:2")).collect::<Vec<_>>().join(";");
        assert!(parse_cities(&many).is_err());
    }

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
