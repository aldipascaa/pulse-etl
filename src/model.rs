//! Core data types shared by every pipeline stage.

use serde::{Deserialize, Serialize};

/// One un-validated input record (a CSV line or an API hourly sample).
/// Every field is optional text so that bad data can be detected and counted
/// rather than aborting the whole load.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct RawRow {
    pub city: Option<String>,
    pub timestamp: Option<String>,
    pub temperature: Option<String>,
    pub unit: Option<String>,
    pub humidity: Option<String>,
    pub wind_speed: Option<String>,
}

/// The measurements the pipeline tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Metric {
    Temperature,
    Humidity,
    Wind,
}

impl Metric {
    pub const ALL: [Metric; 3] = [Metric::Temperature, Metric::Humidity, Metric::Wind];

    pub fn as_str(self) -> &'static str {
        match self {
            Metric::Temperature => "temperature",
            Metric::Humidity => "humidity",
            Metric::Wind => "wind",
        }
    }

    pub fn unit(self) -> &'static str {
        match self {
            Metric::Temperature => "°C",
            Metric::Humidity => "%",
            Metric::Wind => "km/h",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Metric::Temperature => "Temperature",
            Metric::Humidity => "Relative humidity",
            Metric::Wind => "Wind speed",
        }
    }

    /// Typical sensor/measurement noise; the floor for the anomaly baseline spread.
    pub fn min_spread(self) -> f64 {
        match self {
            Metric::Temperature => 0.5,
            Metric::Humidity => 2.0,
            Metric::Wind => 1.0,
        }
    }

    pub fn parse(s: &str) -> Option<Metric> {
        Metric::ALL.into_iter().find(|m| m.as_str().eq_ignore_ascii_case(s.trim()))
    }
}

/// A validated, unit-normalised measurement (temperature in °C, humidity in %, wind in km/h).
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    pub city: String,
    /// Normalised `YYYY-MM-DDTHH:MM` (UTC / station local, as supplied).
    pub ts: String,
    pub metric: Metric,
    pub value: f64,
}

/// Daily aggregate for one city/metric, with rolling mean and anomaly flag.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DailyAgg {
    pub city: String,
    pub metric: Metric,
    pub day: String,
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub count: u32,
    pub rolling7: f64,
    pub zscore: f64,
    pub anomaly: bool,
}

/// Lifecycle state of a pipeline run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Running,
    /// Everything extracted and loaded, nothing rejected.
    Success,
    /// Loaded, but some rows were rejected or some sources failed.
    Partial,
    Failed,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Success => "success",
            RunStatus::Partial => "partial",
            RunStatus::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<RunStatus> {
        [RunStatus::Running, RunStatus::Success, RunStatus::Partial, RunStatus::Failed]
            .into_iter()
            .find(|v| v.as_str() == s)
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One sub-source (for example a city) that could not be fetched during a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceFailure {
    pub source: String,
    pub error: String,
}

/// One pipeline execution as stored in the `runs` table.
#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub id: i64,
    pub source: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub rows_in: i64,
    pub rows_out: i64,
    pub rows_rejected: i64,
    pub status: RunStatus,
    pub message: Option<String>,
    /// Sub-sources that failed even though the run as a whole may have succeeded.
    pub failures: Vec<SourceFailure>,
}
