# Changelog

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
