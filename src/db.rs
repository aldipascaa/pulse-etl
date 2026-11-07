//! SQLite storage: connection pool, versioned migrations, the load stage and read queries.
//!
//! All methods on [`Db`] are synchronous (rusqlite is blocking). Async callers must go through
//! [`Db::blocking`] (or the [`Sink`] implementation, which does so internally) so that database
//! work never runs on a tokio worker thread.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::Serialize;

use crate::error::StoreError;
use crate::model::{DailyAgg, Metric, Reading, RunRecord, RunStatus, SourceFailure};
use crate::pipeline::{RunOutcome, Sink};
use crate::transform::{aggregate, AnomalyConfig};

/// Rows returned by `/api/series` when the caller gives no limit.
pub const DEFAULT_SERIES_LIMIT: u32 = 366;
/// Hard upper bound for a single page of series points.
pub const MAX_SERIES_LIMIT: u32 = 1000;

struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

/// Ordered, append-only list. Never edit an applied migration; add a new one.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial schema",
        // `IF NOT EXISTS` so that databases created by v1.0.0 (which had no version table) adopt this baseline.
        sql: "
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
        ",
    },
    Migration {
        version: 2,
        name: "per-source run failures, covering index for series rebuilds",
        sql: "
        CREATE TABLE run_failures (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
            source TEXT NOT NULL,
            error  TEXT NOT NULL
        );
        CREATE INDEX idx_run_failures_run ON run_failures (run_id);

        -- The primary key is (city, ts, metric); rebuilds filter by (city, metric) and would scan
        -- every metric of the city. This index (with value, so it is covering) makes that a range scan.
        CREATE INDEX idx_readings_city_metric_ts ON readings (city, metric, ts, value);
        ",
    },
];

const AGG_COLS: &str = "city, metric, day, min_v, max_v, avg_v, n, rolling7, zscore, anomaly";
const RUN_COLS: &str = "id, source, started_at, finished_at, rows_in, rows_out, rows_rejected, status, message";

fn now() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Apply every pending migration, each in its own transaction, and record it in `schema_migrations`.
fn migrate(conn: &mut Connection) -> Result<(), StoreError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )",
    )?;
    let latest = MIGRATIONS.last().map_or(0, |m| m.version);
    let current: i64 =
        conn.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", [], |r| r.get(0))?;
    if current > latest {
        return Err(StoreError::SchemaTooNew { found: current, supported: latest });
    }
    for m in MIGRATIONS.iter().filter(|m| m.version > current) {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Another process may have applied it while we waited for the write lock.
        let done: bool =
            tx.query_row("SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = ?1)", [m.version], |r| r.get(0))?;
        if !done {
            tx.execute_batch(m.sql)?;
            tx.execute(
                "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
                params![m.version, m.name, now()],
            )?;
            tracing::info!("applied migration {} ({})", m.version, m.name);
        }
        tx.commit()?;
    }
    Ok(())
}

/// Cheap-to-clone handle to the SQLite database (a connection pool).
#[derive(Clone)]
pub struct Db {
    pool: Pool<SqliteConnectionManager>,
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

/// Filter and page window for [`Db::series_page`]. `from`/`to` are inclusive `YYYY-MM-DD` days.
#[derive(Debug, Clone)]
pub struct SeriesFilter {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: u32,
    pub offset: u32,
}

impl Default for SeriesFilter {
    fn default() -> Self {
        SeriesFilter { from: None, to: None, limit: DEFAULT_SERIES_LIMIT, offset: 0 }
    }
}

/// One page of a daily series plus the number of points matching the filter overall.
#[derive(Debug)]
pub struct SeriesPage {
    pub total: i64,
    pub points: Vec<DailyAgg>,
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
    let status: String = r.get(7)?;
    Ok(RunRecord {
        id: r.get(0)?,
        source: r.get(1)?,
        started_at: r.get(2)?,
        finished_at: r.get(3)?,
        rows_in: r.get(4)?,
        rows_out: r.get(5)?,
        rows_rejected: r.get(6)?,
        status: RunStatus::parse(&status).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, format!("unknown run status '{status}'").into())
        })?,
        message: r.get(8)?,
        failures: Vec::new(),
    })
}

/// Fill in `failures` for runs fetched without them.
fn attach_failures(conn: &Connection, runs: &mut [RunRecord]) -> Result<(), StoreError> {
    let (Some(lo), Some(hi)) = (runs.iter().map(|r| r.id).min(), runs.iter().map(|r| r.id).max()) else {
        return Ok(());
    };
    let mut q = conn.prepare_cached("SELECT run_id, source, error FROM run_failures WHERE run_id BETWEEN ?1 AND ?2 ORDER BY id")?;
    let mut by_run: HashMap<i64, Vec<SourceFailure>> = HashMap::new();
    let rows = q.query_map([lo, hi], |r| Ok((r.get::<_, i64>(0)?, SourceFailure { source: r.get(1)?, error: r.get(2)? })))?;
    for row in rows {
        let (id, f) = row?;
        by_run.entry(id).or_default().push(f);
    }
    for run in runs {
        run.failures = by_run.remove(&run.id).unwrap_or_default();
    }
    Ok(())
}

impl Db {
    /// Open (creating if needed) a database file and bring its schema up to date.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let manager = SqliteConnectionManager::file(path).with_init(init_connection);
        Self::from_pool(Pool::builder().max_size(4).build(manager)?)
    }

    /// Private in-memory database. Uses a single connection, because every SQLite
    /// in-memory connection is its own database.
    pub fn open_in_memory() -> Result<Self, StoreError> {
        let manager = SqliteConnectionManager::memory().with_init(init_connection);
        let pool = Pool::builder().max_size(1).min_idle(Some(1)).idle_timeout(None).max_lifetime(None).build(manager)?;
        Self::from_pool(pool)
    }

    fn from_pool(pool: Pool<SqliteConnectionManager>) -> Result<Self, StoreError> {
        {
            let mut conn = pool.get()?;
            migrate(&mut conn)?;
        }
        Ok(Db { pool })
    }

    fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>, StoreError> {
        Ok(self.pool.get()?)
    }

    /// Run blocking database work on tokio's blocking thread pool.
    pub async fn blocking<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&Db) -> Result<T, StoreError> + Send + 'static,
    {
        let db = self.clone();
        tokio::task::spawn_blocking(move || f(&db)).await.map_err(|e| StoreError::Join(e.to_string()))?
    }

    pub fn ping(&self) -> Result<(), StoreError> {
        self.conn()?.query_row("SELECT 1", [], |_| Ok(()))?;
        Ok(())
    }

    /// Highest applied schema version.
    pub fn schema_version(&self) -> Result<i64, StoreError> {
        Ok(self.conn()?.query_row("SELECT COALESCE(MAX(version), 0) FROM schema_migrations", [], |r| r.get(0))?)
    }

    /// Idempotently upsert readings, then rebuild the daily aggregates of every
    /// city/metric touched (so rolling means and z-scores stay consistent), atomically.
    pub fn load(&self, readings: &[Reading], anomaly: &AnomalyConfig) -> Result<(), StoreError> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
            let aggs = aggregate(&all, anomaly);
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

    pub fn start_run(&self, source: &str) -> Result<i64, StoreError> {
        let conn = self.conn()?;
        conn.execute("INSERT INTO runs (source, started_at, status) VALUES (?1, ?2, 'running')", params![source, now()])?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_run(&self, id: i64, o: &RunOutcome) -> Result<RunRecord, StoreError> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE runs SET finished_at = ?2, status = ?3, rows_in = ?4, rows_out = ?5, rows_rejected = ?6, message = ?7
             WHERE id = ?1",
            params![id, now(), o.status.as_str(), o.rows_in as i64, o.rows_out as i64, o.rows_rejected as i64, o.message],
        )?;
        tx.execute("DELETE FROM run_failures WHERE run_id = ?1", [id])?;
        for f in &o.failures {
            tx.execute("INSERT INTO run_failures (run_id, source, error) VALUES (?1, ?2, ?3)", params![id, f.source, f.error])?;
        }
        let mut run = tx.query_row(&format!("SELECT {RUN_COLS} FROM runs WHERE id = ?1"), [id], row_to_run)?;
        run.failures = o.failures.clone();
        tx.commit()?;
        Ok(run)
    }

    /// Mark runs left in `running` (the process died mid-run) as failed. Call once at startup of a
    /// long-lived process; do not call while another process may legitimately be running an ingest.
    pub fn fail_stale_runs(&self) -> Result<usize, StoreError> {
        Ok(self.conn()?.execute(
            "UPDATE runs SET status = 'failed', finished_at = ?1, message = 'interrupted: process stopped before the run finished'
             WHERE status = 'running'",
            [now()],
        )?)
    }

    pub fn runs(&self, limit: usize) -> Result<Vec<RunRecord>, StoreError> {
        let conn = self.conn()?;
        let mut q = conn.prepare(&format!("SELECT {RUN_COLS} FROM runs ORDER BY id DESC LIMIT ?1"))?;
        let mut runs: Vec<RunRecord> = q.query_map([limit as i64], row_to_run)?.collect::<rusqlite::Result<_>>()?;
        attach_failures(&conn, &mut runs)?;
        Ok(runs)
    }

    /// The complete daily series, unpaged (library use and tests; the API uses [`Db::series_page`]).
    pub fn series(&self, city: &str, metric: Metric) -> Result<Vec<DailyAgg>, StoreError> {
        Ok(self.series_page(city, metric, &SeriesFilter { limit: u32::MAX, ..Default::default() })?.points)
    }

    /// A filtered, paged slice of one daily series in ascending day order.
    pub fn series_page(&self, city: &str, metric: Metric, f: &SeriesFilter) -> Result<SeriesPage, StoreError> {
        let conn = self.conn()?;
        const WHERE: &str = "city = ?1 AND metric = ?2 AND (?3 IS NULL OR day >= ?3) AND (?4 IS NULL OR day <= ?4)";
        let total: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM daily WHERE {WHERE}"),
            params![city, metric.as_str(), f.from, f.to],
            |r| r.get(0),
        )?;
        let mut q = conn.prepare(&format!("SELECT {AGG_COLS} FROM daily WHERE {WHERE} ORDER BY day LIMIT ?5 OFFSET ?6"))?;
        let points = q
            .query_map(params![city, metric.as_str(), f.from, f.to, i64::from(f.limit), i64::from(f.offset)], row_to_agg)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(SeriesPage { total, points })
    }

    pub fn count_readings(&self) -> Result<i64, StoreError> {
        Ok(self.conn()?.query_row("SELECT COUNT(*) FROM readings", [], |r| r.get(0))?)
    }

    pub fn overview(&self) -> Result<Overview, StoreError> {
        let conn = self.conn()?;
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
        let last_run = {
            let mut last: Vec<RunRecord> = conn
                .query_row(&format!("SELECT {RUN_COLS} FROM runs ORDER BY id DESC LIMIT 1"), [], row_to_run)
                .optional()?
                .into_iter()
                .collect();
            attach_failures(&conn, &mut last)?;
            last.pop()
        };
        let metrics = Metric::ALL.iter().map(|m| MetricInfo { name: m.as_str(), label: m.label(), unit: m.unit() }).collect();
        Ok(Overview { cities, metrics, totals, latest, recent_anomalies, last_run })
    }
}

fn init_connection(c: &mut Connection) -> rusqlite::Result<()> {
    c.busy_timeout(Duration::from_secs(5))?;
    // WAL lets readers proceed during a write; it is a harmless no-op for in-memory databases.
    c.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;")
}

#[async_trait]
impl Sink for Db {
    async fn start_run(&self, source: &str) -> Result<i64, StoreError> {
        let source = source.to_string();
        self.blocking(move |db| db.start_run(&source)).await
    }

    async fn load(&self, readings: Vec<Reading>, anomaly: AnomalyConfig) -> Result<(), StoreError> {
        self.blocking(move |db| db.load(&readings, &anomaly)).await
    }

    async fn finish_run(&self, id: i64, outcome: RunOutcome) -> Result<RunRecord, StoreError> {
        self.blocking(move |db| db.finish_run(id, &outcome)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(ts: &str, v: f64) -> Reading {
        Reading { city: "Oslo".into(), ts: ts.into(), metric: Metric::Temperature, value: v }
    }

    fn outcome(status: RunStatus, failures: Vec<SourceFailure>) -> RunOutcome {
        RunOutcome { status, rows_in: 10, rows_out: 8, rows_rejected: 2, message: Some("2 bad".into()), failures }
    }

    #[test]
    fn upsert_is_idempotent_and_updates_values() {
        let db = Db::open_in_memory().unwrap();
        let cfg = AnomalyConfig::default();
        db.load(&[r("2026-10-01T00:00", 1.0), r("2026-10-01T01:00", 3.0)], &cfg).unwrap();
        db.load(&[r("2026-10-01T00:00", 1.0), r("2026-10-01T01:00", 3.0)], &cfg).unwrap();
        assert_eq!(db.count_readings().unwrap(), 2);
        db.load(&[r("2026-10-01T01:00", 5.0)], &cfg).unwrap(); // corrected value
        assert_eq!(db.count_readings().unwrap(), 2);
        let s = db.series("Oslo", Metric::Temperature).unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].avg, 3.0); // mean(1, 5)
    }

    #[test]
    fn run_lifecycle_records_source_failures() {
        let db = Db::open_in_memory().unwrap();
        let id = db.start_run("test").unwrap();
        assert_eq!(db.runs(5).unwrap()[0].status, RunStatus::Running);
        let failures = vec![SourceFailure { source: "Tokyo".into(), error: "timeout".into() }];
        let rec = db.finish_run(id, &outcome(RunStatus::Partial, failures.clone())).unwrap();
        assert_eq!((rec.rows_in, rec.rows_out, rec.rows_rejected), (10, 8, 2));
        assert!(rec.finished_at.is_some());
        assert_eq!(rec.failures, failures);
        // Persisted and returned by the listing and the overview too.
        assert_eq!(db.runs(5).unwrap()[0].failures, failures);
        assert_eq!(db.overview().unwrap().last_run.unwrap().failures, failures);
    }

    #[test]
    fn stale_running_runs_are_failed_on_request() {
        let db = Db::open_in_memory().unwrap();
        let done = db.start_run("a").unwrap();
        db.finish_run(done, &outcome(RunStatus::Success, vec![])).unwrap();
        db.start_run("b").unwrap();
        assert_eq!(db.fail_stale_runs().unwrap(), 1);
        let runs = db.runs(5).unwrap();
        assert_eq!(runs[0].status, RunStatus::Failed);
        assert!(runs[0].message.as_deref().unwrap().contains("interrupted"));
        assert_eq!(runs[1].status, RunStatus::Success);
        assert_eq!(db.fail_stale_runs().unwrap(), 0);
    }

    fn seeded(days: u32) -> Db {
        let db = Db::open_in_memory().unwrap();
        let rs: Vec<Reading> = (1..=days).map(|d| r(&format!("2026-10-{d:02}T12:00"), f64::from(d))).collect();
        db.load(&rs, &AnomalyConfig::default()).unwrap();
        db
    }

    #[test]
    fn series_pages_and_filters() {
        let db = seeded(20);
        let page = |f: SeriesFilter| db.series_page("Oslo", Metric::Temperature, &f).unwrap();

        let p = page(SeriesFilter { limit: 5, ..Default::default() });
        assert_eq!((p.total, p.points.len()), (20, 5));
        assert_eq!(p.points[0].day, "2026-10-01");

        let p = page(SeriesFilter { limit: 5, offset: 18, ..Default::default() });
        assert_eq!((p.total, p.points.len()), (20, 2));
        assert_eq!(p.points[1].day, "2026-10-20");

        let p = page(SeriesFilter { from: Some("2026-10-10".into()), to: Some("2026-10-12".into()), ..Default::default() });
        assert_eq!(p.total, 3);
        assert_eq!(p.points.iter().map(|x| x.day.as_str()).collect::<Vec<_>>(), ["2026-10-10", "2026-10-11", "2026-10-12"]);

        let p = page(SeriesFilter { from: Some("2027-01-01".into()), ..Default::default() });
        assert_eq!((p.total, p.points.len()), (0, 0));
    }

    #[test]
    fn migrations_are_recorded_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.db");
        let db = Db::open(&path).unwrap();
        assert_eq!(db.schema_version().unwrap(), MIGRATIONS.last().unwrap().version);
        db.load(&[r("2026-10-01T00:00", 1.0)], &AnomalyConfig::default()).unwrap();
        drop(db);
        let db = Db::open(&path).unwrap(); // reopening applies nothing and keeps data
        assert_eq!(db.count_readings().unwrap(), 1);
        let n: i64 = db.conn().unwrap().query_row("SELECT COUNT(*) FROM schema_migrations", [], |r| r.get(0)).unwrap();
        assert_eq!(n, MIGRATIONS.len() as i64);
    }

    #[test]
    fn legacy_v1_database_is_upgraded_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.db");
        {
            // Exactly what v1.0.0 created: the baseline schema, no version table.
            let c = Connection::open(&path).unwrap();
            c.execute_batch(MIGRATIONS[0].sql).unwrap();
            c.execute("INSERT INTO readings VALUES ('Oslo', '2026-10-01T00:00', 'temperature', 4.0)", []).unwrap();
            c.execute("INSERT INTO runs (source, started_at, status) VALUES ('old', '2026-10-01T00:00:00Z', 'success')", []).unwrap();
        }
        let db = Db::open(&path).unwrap();
        assert_eq!(db.schema_version().unwrap(), 2);
        assert_eq!(db.count_readings().unwrap(), 1);
        let runs = db.runs(5).unwrap();
        assert_eq!((runs.len(), runs[0].status, runs[0].failures.len()), (1, RunStatus::Success, 0));
        let idx: i64 = db
            .conn()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM sqlite_master WHERE name = 'idx_readings_city_metric_ts'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn database_from_a_newer_build_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.db");
        drop(Db::open(&path).unwrap());
        Connection::open(&path)
            .unwrap()
            .execute("INSERT INTO schema_migrations VALUES (99, 'future', '2030-01-01T00:00:00Z')", [])
            .unwrap();
        match Db::open(&path) {
            Err(StoreError::SchemaTooNew { found: 99, .. }) => {}
            other => panic!("expected SchemaTooNew, got {:?}", other.map(|_| ())),
        }
    }

    #[tokio::test]
    async fn blocking_runs_db_work_off_the_runtime() {
        let db = Db::open_in_memory().unwrap();
        let n = db.blocking(|db| db.count_readings()).await.unwrap();
        assert_eq!(n, 0);
        // A panic inside the worker surfaces as a typed error instead of poisoning the caller.
        let err = db.blocking::<(), _>(|_| panic!("boom")).await.unwrap_err();
        assert!(matches!(err, StoreError::Join(_)));
    }
}
