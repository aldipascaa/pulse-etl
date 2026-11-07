//! Load stage and read queries: SQLite storage with idempotent upserts.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, Row};
use serde::Serialize;

use crate::model::{DailyAgg, Metric, Reading, RunRecord};
use crate::transform::aggregate;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS readings (
    city   TEXT NOT NULL,
    ts     TEXT NOT NULL,
    metric TEXT NOT NULL,
    value  REAL NOT NULL,
    PRIMARY KEY (city, ts, metric)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS daily (
    city     TEXT NOT NULL,
    metric   TEXT NOT NULL,
    day      TEXT NOT NULL,
    min_v    REAL NOT NULL,
    max_v    REAL NOT NULL,
    avg_v    REAL NOT NULL,
    n        INTEGER NOT NULL,
    rolling7 REAL NOT NULL,
    zscore   REAL NOT NULL,
    anomaly  INTEGER NOT NULL,
    PRIMARY KEY (city, metric, day)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    rows_in       INTEGER NOT NULL DEFAULT 0,
    rows_out      INTEGER NOT NULL DEFAULT 0,
    rows_rejected INTEGER NOT NULL DEFAULT 0,
    status        TEXT NOT NULL,
    message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_daily_anomaly ON daily (anomaly, day);
";

const AGG_COLS: &str = "city, metric, day, min_v, max_v, avg_v, n, rolling7, zscore, anomaly";
const RUN_COLS: &str = "id, source, started_at, finished_at, rows_in, rows_out, rows_rejected, status, message";

fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Cheap-to-clone handle to the SQLite database.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Serialize)]
pub struct MetricInfo {
    pub name: &'static str,
    pub label: &'static str,
    pub unit: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Totals {
    pub readings: i64,
    pub daily_points: i64,
    pub anomalies: i64,
    pub cities: i64,
    pub runs: i64,
}

#[derive(Debug, Serialize)]
pub struct Overview {
    pub cities: Vec<String>,
    pub metrics: Vec<MetricInfo>,
    pub totals: Totals,
    /// Most recent daily aggregate for every city/metric.
    pub latest: Vec<DailyAgg>,
    pub recent_anomalies: Vec<DailyAgg>,
    pub last_run: Option<RunRecord>,
}

fn row_to_agg(r: &Row) -> rusqlite::Result<DailyAgg> {
    let metric: String = r.get(1)?;
    Ok(DailyAgg {
        city: r.get(0)?,
        metric: Metric::parse(&metric).ok_or(rusqlite::Error::InvalidColumnName(metric))?,
        day: r.get(2)?,
        min: r.get(3)?,
        max: r.get(4)?,
        avg: r.get(5)?,
        count: r.get(6)?,
        rolling7: r.get(7)?,
        zscore: r.get(8)?,
        anomaly: r.get::<_, i64>(9)? != 0,
    })
}

fn row_to_run(r: &Row) -> rusqlite::Result<RunRecord> {
    Ok(RunRecord {
        id: r.get(0)?,
        source: r.get(1)?,
        started_at: r.get(2)?,
        finished_at: r.get(3)?,
        rows_in: r.get(4)?,
        rows_out: r.get(5)?,
        rows_rejected: r.get(6)?,
        status: r.get(7)?,
        message: r.get(8)?,
    })
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).with_context(|| format!("opening database {}", path.display()))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        // WAL is a no-op for in-memory databases, which is fine.
        let _: String = conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))?;
        conn.execute_batch(SCHEMA)?;
        Ok(Db { conn: Arc::new(Mutex::new(conn)) })
    }

    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn ping(&self) -> Result<()> {
        self.conn().query_row("SELECT 1", [], |_| Ok(()))?;
        Ok(())
    }

    /// Idempotently upsert readings, then rebuild the daily aggregates of every
    /// city/metric touched (so rolling means and z-scores stay consistent), atomically.
    pub fn load(&self, readings: &[Reading], z_threshold: f64) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        {
            let mut up = tx.prepare_cached(
                "INSERT INTO readings (city, ts, metric, value) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (city, ts, metric) DO UPDATE SET value = excluded.value",
            )?;
            for r in readings {
                up.execute(params![r.city, r.ts, r.metric.as_str(), r.value])?;
            }
        }
        let touched: BTreeSet<(&str, Metric)> = readings.iter().map(|r| (r.city.as_str(), r.metric)).collect();
        for (city, metric) in touched {
            let all: Vec<Reading> = {
                let mut q = tx.prepare_cached("SELECT ts, value FROM readings WHERE city = ?1 AND metric = ?2")?;
                let rows = q.query_map(params![city, metric.as_str()], |r| {
                    Ok(Reading { city: city.to_string(), ts: r.get(0)?, metric, value: r.get(1)? })
                })?;
                rows.collect::<rusqlite::Result<_>>()?
            };
            let aggs = aggregate(&all, z_threshold);
            tx.execute("DELETE FROM daily WHERE city = ?1 AND metric = ?2", params![city, metric.as_str()])?;
            let mut ins = tx.prepare_cached(&format!(
                "INSERT INTO daily ({AGG_COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
            ))?;
            for a in &aggs {
                ins.execute(params![
                    a.city, a.metric.as_str(), a.day, a.min, a.max, a.avg, a.count, a.rolling7, a.zscore, a.anomaly as i64
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn start_run(&self, source: &str) -> Result<i64> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO runs (source, started_at, status) VALUES (?1, ?2, 'running')",
            params![source, now()],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_run(
        &self,
        id: i64,
        status: &str,
        rows_in: usize,
        rows_out: usize,
        rows_rejected: usize,
        message: Option<&str>,
    ) -> Result<RunRecord> {
        let conn = self.conn();
        conn.execute(
            "UPDATE runs SET finished_at = ?2, status = ?3, rows_in = ?4, rows_out = ?5, rows_rejected = ?6, message = ?7
             WHERE id = ?1",
            params![id, now(), status, rows_in as i64, rows_out as i64, rows_rejected as i64, message],
        )?;
        Ok(conn.query_row(&format!("SELECT {RUN_COLS} FROM runs WHERE id = ?1"), [id], row_to_run)?)
    }

    pub fn runs(&self, limit: usize) -> Result<Vec<RunRecord>> {
        let conn = self.conn();
        let mut q = conn.prepare(&format!("SELECT {RUN_COLS} FROM runs ORDER BY id DESC LIMIT ?1"))?;
        let rows = q.query_map([limit as i64], row_to_run)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn series(&self, city: &str, metric: Metric) -> Result<Vec<DailyAgg>> {
        let conn = self.conn();
        let mut q = conn.prepare(&format!(
            "SELECT {AGG_COLS} FROM daily WHERE city = ?1 AND metric = ?2 ORDER BY day"
        ))?;
        let rows = q.query_map(params![city, metric.as_str()], row_to_agg)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    pub fn count_readings(&self) -> Result<i64> {
        Ok(self.conn().query_row("SELECT COUNT(*) FROM readings", [], |r| r.get(0))?)
    }

    pub fn overview(&self) -> Result<Overview> {
        let conn = self.conn();
        let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0));
        let totals = Totals {
            readings: count("SELECT COUNT(*) FROM readings")?,
            daily_points: count("SELECT COUNT(*) FROM daily")?,
            anomalies: count("SELECT COUNT(*) FROM daily WHERE anomaly = 1")?,
            cities: count("SELECT COUNT(DISTINCT city) FROM daily")?,
            runs: count("SELECT COUNT(*) FROM runs")?,
        };
        let cities = {
            let mut q = conn.prepare("SELECT DISTINCT city FROM daily ORDER BY city")?;
            let rows = q.query_map([], |r| r.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let latest = {
            let mut q = conn.prepare(&format!(
                "SELECT {AGG_COLS} FROM daily d
                 WHERE day = (SELECT MAX(day) FROM daily WHERE city = d.city AND metric = d.metric)
                 ORDER BY city, metric"
            ))?;
            let rows = q.query_map([], row_to_agg)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let recent_anomalies = {
            let mut q = conn.prepare(&format!(
                "SELECT {AGG_COLS} FROM daily WHERE anomaly = 1 ORDER BY day DESC, ABS(zscore) DESC LIMIT 12"
            ))?;
            let rows = q.query_map([], row_to_agg)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let last_run = conn
            .query_row(&format!("SELECT {RUN_COLS} FROM runs ORDER BY id DESC LIMIT 1"), [], row_to_run)
            .ok();
        let metrics = Metric::ALL
            .iter()
            .map(|m| MetricInfo { name: m.as_str(), label: m.label(), unit: m.unit() })
            .collect();
        Ok(Overview { cities, metrics, totals, latest, recent_anomalies, last_run })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(ts: &str, v: f64) -> Reading {
        Reading { city: "Oslo".into(), ts: ts.into(), metric: Metric::Temperature, value: v }
    }

    #[test]
    fn upsert_is_idempotent_and_updates_values() {
        let db = Db::open_in_memory().unwrap();
        db.load(&[r("2026-10-01T00:00", 1.0), r("2026-10-01T01:00", 3.0)], 2.0).unwrap();
        db.load(&[r("2026-10-01T00:00", 1.0), r("2026-10-01T01:00", 3.0)], 2.0).unwrap();
        assert_eq!(db.count_readings().unwrap(), 2);
        db.load(&[r("2026-10-01T01:00", 5.0)], 2.0).unwrap(); // corrected value
        assert_eq!(db.count_readings().unwrap(), 2);
        let s = db.series("Oslo", Metric::Temperature).unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].avg, 3.0); // mean(1, 5)
    }

    #[test]
    fn run_lifecycle() {
        let db = Db::open_in_memory().unwrap();
        let id = db.start_run("test").unwrap();
        assert_eq!(db.runs(5).unwrap()[0].status, "running");
        let rec = db.finish_run(id, "partial", 10, 8, 2, Some("2 bad")).unwrap();
        assert_eq!((rec.rows_in, rec.rows_out, rec.rows_rejected), (10, 8, 2));
        assert!(rec.finished_at.is_some());
    }
}
