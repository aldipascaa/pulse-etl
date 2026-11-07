use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use pulse_etl::db::Db;
use pulse_etl::pipeline::{run_ingest, PipelineConfig, Source};
use pulse_etl::server::{router, run_scheduler, AppState};

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
    #[arg(long)]
    csv: Option<PathBuf>,
    /// Days of history requested from the API.
    #[arg(long, default_value_t = 14)]
    past_days: u32,
    /// |z-score| at or above which a day is flagged as an anomaly.
    #[arg(long, default_value_t = 2.0)]
    z_threshold: f64,
}

#[derive(Args, Clone)]
struct ServeArgs {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,
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
        #[arg(long, default_value_t = 3600)]
        interval_secs: u64,
    },
}

fn setup(src: &SourceArgs) -> (Source, PipelineConfig) {
    let source = src.csv.clone().map(Source::Csv).unwrap_or(Source::Api);
    let cfg = PipelineConfig { z_threshold: src.z_threshold, past_days: src.past_days, ..Default::default() };
    (source, cfg)
}

async fn serve(state: AppState, addr: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("binding {addr}"))?;
    println!("PulseETL dashboard: http://{}", listener.local_addr()?);
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            println!("shutting down");
        })
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "pulse_etl=info".into()))
        .init();
    let cli = Cli::parse();
    let db = Db::open(&cli.db)?;

    match cli.cmd {
        Cmd::Ingest(src) => {
            let (source, cfg) = setup(&src);
            let r = run_ingest(&db, &source, &cfg).await?;
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
            if r.status == "failed" {
                std::process::exit(1);
            }
        }
        Cmd::Serve { serve: s, src } => {
            let (source, cfg) = setup(&src);
            serve(AppState::new(db, source, cfg), &s.addr).await?;
        }
        Cmd::Run { serve: s, src, interval_secs } => {
            let (source, cfg) = setup(&src);
            let state = AppState::new(db, source, cfg);
            tokio::spawn(run_scheduler(state.clone(), Duration::from_secs(interval_secs.max(1))));
            serve(state, &s.addr).await?;
        }
    }
    Ok(())
}
