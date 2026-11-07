//! Typed errors for the library. The binary converts these to `anyhow` at the edge.

use std::path::PathBuf;

/// Failures of the SQLite storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("connection pool error: {0}")]
    Pool(#[from] r2d2::Error),
    #[error("database worker task failed: {0}")]
    Join(String),
    #[error("database schema version {found} is newer than this build supports ({supported}); upgrade pulse-etl")]
    SchemaTooNew { found: i64, supported: i64 },
}

/// Failures of an extract stage.
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("cannot read CSV {path}: {source}")]
    CsvOpen { path: PathBuf, source: csv::Error },
    #[error("extract worker task failed: {0}")]
    Join(String),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("could not build HTTP client: {0}")]
    Client(String),
    #[error("{0}")]
    AllSourcesFailed(String),
}
