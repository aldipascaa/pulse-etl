#![allow(dead_code)]
use std::path::PathBuf;
use std::sync::Arc;

use pulse_etl::db::Db;
use pulse_etl::extract::{CsvSource, Source};
use pulse_etl::pipeline::Pipeline;
use pulse_etl::transform::AnomalyConfig;

pub fn sample() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data").join("sample_weather.csv")
}

pub fn pipeline(db: &Db, source: impl Source + 'static) -> Arc<Pipeline> {
    Arc::new(Pipeline::new(Arc::new(source), Arc::new(db.clone()), AnomalyConfig::default()))
}

pub fn csv_pipeline(db: &Db, path: impl Into<PathBuf>) -> Arc<Pipeline> {
    pipeline(db, CsvSource::new(path))
}
