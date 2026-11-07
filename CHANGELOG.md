# Changelog

## 1.1.0 - 2026-11-08

Architecture review and repair; see `ARCHITECTURE_REVIEW.md` for the findings behind each item.

Fixed
- Database access no longer blocks the async runtime: r2d2 connection pool (WAL) with all work behind `spawn_blocking`.
- Anomaly detection is now a trailing window (default 7 days, `--anomaly-window`) that excludes the day being tested and gross outliers, needs a minimum baseline (`--min-baseline`) and uses a per-metric noise floor. Previously the z-score used the whole stored series, so a spike hid itself.
- Per-source failures (for example one city that could not be fetched) are stored in a new `run_failures` table, returned as `failures` on every run, summarised in the run message, and mark the run `partial`.
- A run triggered from the API is no longer abandoned if the client disconnects; runs left `running` by a crash are marked failed at server start.
- `retry` could overflow its backoff shift with a large attempt count; `overview` no longer hides database errors.

Added
- `Source` and `Sink` traits (`CsvSource`, `OpenMeteoSource`, `MemorySource`; `Db` is the SQLite `Sink`).
- Versioned schema migrations (`schema_migrations`), a covering index for series rebuilds, and a refusal to open databases from a newer build. Existing v1.0.0 databases upgrade in place.
- `/api/series` pagination and date filters (`from`, `to`, `limit`, `offset`, `total`).
- Optional bearer token for `POST /api/ingest` (`PULSE_API_TOKEN`); dashboard prompts for it on `401`.
- Consistent JSON errors (`error` + `code`) including bad query strings and unknown routes.
- Graceful shutdown of server and scheduler on Ctrl-C / SIGTERM; `PULSE_*` environment variables and range validation for all options.
- `/api/health` reports `schema_version` and `ingest_protected`.
- 25 new tests (42 total): rolling anomalies, migrations, pagination, auth, mock-API partial failure, fake sink, scheduler cancellation, runtime non-blocking.

Changed
- Library errors are typed (`thiserror`); `anyhow` is used only in the binary. Run status is an enum.
- Library API: `Pipeline::new(source, sink, anomaly_config).run()` replaces `run_ingest` / `ingest_rows`; `AppState::new(db, runner)`.
- Anomaly flags differ from 1.0.0 by design. Daily rows keep their old flags until the next ingest recomputes them, so run one ingest after upgrading.

Not changed (deliberately): incremental aggregation, config file, TLS/rate limiting, SIGTERM on Windows.


## 1.0.0 - 2026-11-07

First stable release.

- CLI + server in one binary: `ingest`, `serve`, `run` (scheduled ingest + server).
- Extract from the Open-Meteo API (no key, retry with exponential backoff) or a local CSV.
- Transform: validation, bad-row rejection, unit normalisation (F/K to C), daily min/max/avg,
  rolling 7-day average, z-score anomaly detection.
- Load into SQLite with idempotent upserts; every execution tracked in the `runs` table.
- JSON API: `/api/health`, `/api/metrics`, `/api/series`, `/api/runs`, `POST /api/ingest`.
- Dashboard: KPI tiles, SVG line charts with anomaly markers, per-city comparison, run history,
  "Run now", light/dark theme, responsive and keyboard accessible, no external assets.
- Unit, integration and API tests.
