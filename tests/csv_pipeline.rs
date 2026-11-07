use std::io::Write;
use std::path::PathBuf;

use pulse_etl::db::Db;
use pulse_etl::extract::read_csv;
use pulse_etl::model::Metric;
use pulse_etl::pipeline::{ingest_rows, run_ingest, PipelineConfig, Source};

fn sample() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data").join("sample_weather.csv")
}

#[tokio::test]
async fn sample_csv_loads_into_temp_db_idempotently() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("test.db")).unwrap();
    let cfg = PipelineConfig::default();
    let source = Source::Csv(sample());

    let first = run_ingest(&db, &source, &cfg).await.unwrap();
    assert_eq!(first.status, "partial");
    assert_eq!(first.rows_in, 2027);
    assert_eq!(first.rows_rejected, 8);
    assert_eq!(first.rows_out, 2019);
    assert!(first.message.as_deref().unwrap().contains("8 rows rejected"));
    let readings = db.count_readings().unwrap();
    // 4 cities x 21 days x 24 h = 2016 rows; 3 duplicates collapse; 3 fully-valid bad-row look-alikes excluded.
    assert!(readings > 5000 && readings <= 2016 * 3, "readings = {readings}");

    // Second run: same data -> same stored state, a second run row.
    let overview1 = db.overview().unwrap();
    let second = run_ingest(&db, &source, &cfg).await.unwrap();
    assert_eq!(second.rows_in, first.rows_in);
    assert_eq!(db.count_readings().unwrap(), readings);
    let overview2 = db.overview().unwrap();
    assert_eq!(overview1.totals.daily_points, overview2.totals.daily_points);
    assert_eq!(overview1.totals.anomalies, overview2.totals.anomalies);
    assert_eq!(overview2.totals.runs, 2);
    assert_eq!(db.runs(10).unwrap().len(), 2);

    // Series shape: 21 days for each city/metric.
    assert_eq!(overview2.cities, vec!["Berlin", "New York", "Sydney", "Tokyo"]);
    let berlin = db.series("Berlin", Metric::Temperature).unwrap();
    assert_eq!(berlin.len(), 21);
    assert!(berlin.iter().all(|p| p.min <= p.avg && p.avg <= p.max));

    // Injected anomalies are detected.
    let spike = berlin.iter().find(|p| p.day == "2026-10-14").unwrap();
    assert!(spike.anomaly && spike.zscore > 2.0, "{spike:?}");
    let tokyo = db.series("Tokyo", Metric::Wind).unwrap();
    assert!(tokyo.iter().any(|p| p.day == "2026-10-09" && p.anomaly));
    assert!(berlin.iter().filter(|p| p.anomaly).count() <= 2);

    // Fahrenheit rows were converted: New York stays plausible on those days.
    let ny = db.series("New York", Metric::Temperature).unwrap();
    assert!(ny.iter().all(|p| p.avg > 5.0 && p.avg < 25.0), "{ny:?}");
}

#[test]
fn small_csv_counts_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mini.csv");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(f, "city,timestamp,temperature,unit,humidity,wind_speed").unwrap();
    writeln!(f, "Oslo,2026-10-01T00:00,50,F,70,10").unwrap();
    writeln!(f, "Oslo,2026-10-01T12:00,20,C,,").unwrap();
    writeln!(f, "Oslo,garbage,20,C,,").unwrap();
    writeln!(f, "Oslo,2026-10-02T00:00").unwrap(); // short line
    drop(f);

    let rows = read_csv(&path).unwrap();
    assert_eq!(rows.len(), 4);
    let db = Db::open_in_memory().unwrap();
    let run = ingest_rows(&db, "csv:mini.csv", &rows, &PipelineConfig::default()).unwrap();
    assert_eq!((run.rows_in, run.rows_out, run.rows_rejected), (4, 2, 2));
    assert_eq!(db.count_readings().unwrap(), 4); // 3 for first row + 1
    let s = db.series("Oslo", Metric::Temperature).unwrap();
    assert_eq!((s[0].min, s[0].max, s[0].avg), (10.0, 20.0, 15.0));
}

#[tokio::test]
async fn missing_file_records_failed_run() {
    let db = Db::open_in_memory().unwrap();
    let run = run_ingest(&db, &Source::Csv("does-not-exist.csv".into()), &PipelineConfig::default()).await.unwrap();
    assert_eq!(run.status, "failed");
    assert!(run.message.unwrap().contains("extract failed"));
    assert_eq!(db.runs(5).unwrap().len(), 1);
}
