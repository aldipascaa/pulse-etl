//! Binary edge: CLI parsing, wiring and process lifecycle. This is the only place that uses `anyhow`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use pulse_etl::db::Db;
use pulse_etl::extract::{parse_cities, City, CsvSource, OpenMeteoSource, Source, OPEN_METEO_URL};
use pulse_etl::model::RunStatus;
use pulse_etl::pipeline::Pipeline;
use pulse_etl::scheduler::{run_scheduler, Runner};
use pulse_etl::server::{router, AppState};
use pulse_etl::transform::AnomalyConfig;
use tokio_util::sync::CancellationToken;

/// How long to wait for an in-flight scheduled run to finish after a shutdown signal.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

fn positive_f64(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|_| format!("'{s}' is not a number"))?;
    if v.is_finite() && v > 0.0 {
        Ok(v)
    } else {
        Err("must be a positive number".into())
    }
}

/// A validated `--cities` value.
#[derive(Clone)]
struct CityList(Vec<City>);

fn city_list(s: &str) -> Result<CityList, String> {
    parse_cities(s).map(CityList)
}

#[derive(Parser)]
#[command(name = "pulse-etl", version, about = "ETL pipeline + analytics dashboard (Open-Meteo / CSV -> SQLite)")]
struct Cli {
    /// SQLite database file.
    #[arg(long, global = true, default_value = "pulse.db", env = "PULSE_DB")]
    db: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct SourceArgs {
    /// Ingest this CSV file instead of calling the Open-Meteo API (works offline).
    #[arg(long, env = "PULSE_CSV")]
    csv: Option<PathBuf>,
    /// Cities to fetch from the API as `NAME:LAT:LON;NAME:LAT:LON` (default: five world cities).
    #[arg(long, value_parser = city_list, env = "PULSE_CITIES", value_name = "LIST")]
    cities: Option<CityList>,
    /// Days of history requested from the API (Open-Meteo allows up to 92).
    #[arg(long, default_value_t = 14, value_parser = clap::value_parser!(u32).range(1..=92), env = "PULSE_PAST_DAYS")]
    past_days: u32,
    /// |z-score| at or above which a day is flagged as an anomaly.
    #[arg(long, default_value_t = 2.0, value_parser = positive_f64, env = "PULSE_Z_THRESHOLD")]
    z_threshold: f64,
    /// Trailing baseline window for anomaly detection, in days.
    #[arg(long, default_value_t = 7, value_parser = clap::value_parser!(u32).range(2..=90), env = "PULSE_ANOMALY_WINDOW")]
    anomaly_window: u32,
    /// Baseline days required before a day can be scored at all.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(2..=30), env = "PULSE_MIN_BASELINE")]
    min_baseline: u32,
}

#[derive(Args, Clone)]
struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8080", env = "PULSE_ADDR")]
    addr: String,
    /// Require `Authorization: Bearer <token>` on POST /api/ingest. Unset = endpoint is open.
    #[arg(long, env = "PULSE_API_TOKEN", hide_env_values = true)]
    api_token: Option<String>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the pipeline once and exit.
    Ingest(SourceArgs),
    /// Serve the dashboard and JSON API.
    Serve {
        #[command(flatten)]
        serve: ServeArgs,
        #[command(flatten)]
        src: SourceArgs,
    },
    /// Scheduled ingest plus server.
    Run {
        #[command(flatten)]
        serve: ServeArgs,
        #[command(flatten)]
        src: SourceArgs,
        /// Seconds between scheduled ingests.
        #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..), env = "PULSE_INTERVAL_SECS")]
        interval_secs: u64,
    },
}

fn build_pipeline(src: &SourceArgs, db: &Db) -> Result<Arc<Pipeline>> {
    let source: Arc<dyn Source> = match &src.csv {
        Some(path) => Arc::new(CsvSource::new(path)),
        None => {
            let api = OpenMeteoSource::new(OPEN_METEO_URL, src.past_days)?;
            Arc::new(match &src.cities {
                Some(list) => api.with_cities(list.0.clone()),
                None => api,
            })
        }
    };
    let anomaly =
        AnomalyConfig { z_threshold: src.z_threshold, window_days: src.anomaly_window, min_baseline: src.min_baseline };
    Ok(Arc::new(Pipeline::new(source, Arc::new(db.clone()), anomaly)))
}

/// Resolves on Ctrl-C (all platforms) or SIGTERM (unix).
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!("cannot listen for Ctrl-C: {e}");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!("cannot listen for SIGTERM: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}

/// Bind, optionally start the scheduler, serve until a shutdown signal, then drain.
async fn serve(db: Db, pipeline: Arc<Pipeline>, args: &ServeArgs, schedule: Option<Duration>) -> Result<()> {
    let listener =
        tokio::net::TcpListener::bind(&args.addr).await.with_context(|| format!("binding {}", args.addr))?;
    let local = listener.local_addr()?;
    let token = args.api_token.clone().filter(|t| !t.is_empty());
    if token.is_none() && !local.ip().is_loopback() {
        tracing::warn!("listening on {local} without PULSE_API_TOKEN: POST /api/ingest is open to the network");
    }

    // Anything still 'running' belongs to a process that died; close those rows.
    let stale = db.blocking(|d| d.fail_stale_runs()).await?;
    if stale > 0 {
        tracing::warn!("marked {stale} interrupted run(s) as failed");
    }

    let runner = Runner::new(pipeline);
    let state = AppState::new(db, runner.clone()).with_api_token(token);
    let cancel = CancellationToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            println!("shutting down");
            cancel.cancel();
        });
    }
    let scheduler = schedule.map(|every| tokio::spawn(run_scheduler(runner, every, cancel.clone())));

    println!("PulseETL dashboard: http://{local}");
    let served = {
        let cancel = cancel.clone();
        axum::serve(listener, router(state)).with_graceful_shutdown(async move { cancel.cancelled().await }).await
    };
    cancel.cancel(); // also stops the scheduler if the server died on its own
    if let Some(handle) = scheduler {
        if tokio::time::timeout(SHUTDOWN_GRACE, handle).await.is_err() {
            tracing::warn!("scheduler did not stop within {SHUTDOWN_GRACE:?}; exiting anyway");
        }
    }
    served.context("server error")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "pulse_etl=info".into()))
        .init();
    let cli = Cli::parse();
    let db = Db::open(&cli.db).with_context(|| format!("opening database {}", cli.db.display()))?;

    match cli.cmd {
        Cmd::Ingest(src) => {
            let r = build_pipeline(&src, &db)?.run().await?;
            println!(
                "run #{} [{}] source={} rows_in={} rows_out={} rejected={}{}",
                r.id,
                r.status,
                r.source,
                r.rows_in,
                r.rows_out,
                r.rows_rejected,
                r.message.as_deref().map(|m| format!("\n  {m}")).unwrap_or_default()
            );
            if r.status == RunStatus::Failed {
                std::process::exit(1);
            }
        }
        Cmd::Serve { serve: s, src } => {
            let pipeline = build_pipeline(&src, &db)?;
            serve(db, pipeline, &s, None).await?;
        }
        Cmd::Run { serve: s, src, interval_secs } => {
            let pipeline = build_pipeline(&src, &db)?;
            serve(db, pipeline, &s, Some(Duration::from_secs(interval_secs))).await?;
        }
    }
    Ok(())
}
