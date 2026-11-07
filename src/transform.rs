//! Transform stage: validation, cleaning, unit normalisation and analytics.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime};

use crate::model::{DailyAgg, Metric, RawRow, Reading};

/// |z| at or above this marks a day as anomalous.
pub const DEFAULT_Z_THRESHOLD: f64 = 2.0;
/// Rolling-average window in days.
pub const ROLLING_DAYS: i64 = 7;

const TEMP_RANGE: (f64, f64) = (-90.0, 60.0);
const HUMIDITY_RANGE: (f64, f64) = (0.0, 100.0);
const WIND_RANGE: (f64, f64) = (0.0, 250.0);

#[derive(Debug, Clone, PartialEq)]
pub struct Rejection {
    /// 1-based data row number (header excluded).
    pub row: usize,
    pub reason: String,
}

#[derive(Debug, Default)]
pub struct CleanOutcome {
    pub readings: Vec<Reading>,
    pub rows_in: usize,
    pub rows_ok: usize,
    pub rejected: Vec<Rejection>,
}

/// Parse the timestamp formats we accept and normalise to `YYYY-MM-DDTHH:MM`.
pub fn parse_timestamp(s: &str) -> Option<String> {
    let s = s.trim();
    const FORMATS: [&str; 4] = ["%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"];
    let naive = FORMATS
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(s, f).ok())
        .or_else(|| DateTime::parse_from_rfc3339(s).ok().map(|d| d.naive_utc()))?;
    Some(naive.format("%Y-%m-%dT%H:%M").to_string())
}

/// Convert a temperature to °C. An empty unit means Celsius.
pub fn to_celsius(value: f64, unit: &str) -> Result<f64, String> {
    match unit.trim().to_ascii_lowercase().as_str() {
        "" | "c" | "°c" | "celsius" => Ok(value),
        "f" | "°f" | "fahrenheit" => Ok((value - 32.0) * 5.0 / 9.0),
        "k" | "kelvin" => Ok(value - 273.15),
        other => Err(format!("unknown temperature unit '{other}'")),
    }
}

fn round(v: f64, places: i32) -> f64 {
    let f = 10f64.powi(places);
    (v * f).round() / f
}

fn field(v: &Option<String>) -> Option<&str> {
    v.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

fn parse_in_range(name: &str, raw: &str, (lo, hi): (f64, f64)) -> Result<f64, String> {
    let v: f64 = raw.parse().map_err(|_| format!("{name} '{raw}' is not a number"))?;
    if !v.is_finite() || v < lo || v > hi {
        return Err(format!("{name} {raw} outside plausible range {lo}..{hi}"));
    }
    Ok(v)
}

fn clean_row(r: &RawRow) -> Result<Vec<Reading>, String> {
    let city = field(&r.city).ok_or("missing city")?.split_whitespace().collect::<Vec<_>>().join(" ");
    let ts_raw = field(&r.timestamp).ok_or("missing timestamp")?;
    let ts = parse_timestamp(ts_raw).ok_or_else(|| format!("invalid timestamp '{ts_raw}'"))?;

    let t_raw = field(&r.temperature).ok_or("missing temperature")?;
    let t: f64 = t_raw.parse().map_err(|_| format!("temperature '{t_raw}' is not a number"))?;
    if !t.is_finite() {
        return Err(format!("temperature '{t_raw}' is not finite"));
    }
    let t_c = to_celsius(t, field(&r.unit).unwrap_or(""))?;
    if t_c < TEMP_RANGE.0 || t_c > TEMP_RANGE.1 {
        return Err(format!("temperature {t_raw} outside plausible range"));
    }

    let mut out = vec![Reading { city: city.clone(), ts: ts.clone(), metric: Metric::Temperature, value: round(t_c, 2) }];
    // Optional fields: absent is fine, present-but-invalid rejects the row.
    if let Some(h) = field(&r.humidity) {
        let v = parse_in_range("humidity", h, HUMIDITY_RANGE)?;
        out.push(Reading { city: city.clone(), ts: ts.clone(), metric: Metric::Humidity, value: round(v, 2) });
    }
    if let Some(w) = field(&r.wind_speed) {
        let v = parse_in_range("wind_speed", w, WIND_RANGE)?;
        out.push(Reading { city, ts, metric: Metric::Wind, value: round(v, 2) });
    }
    Ok(out)
}

/// Validate and normalise raw rows. Bad rows are dropped and reported, never fatal.
pub fn clean(rows: &[RawRow]) -> CleanOutcome {
    let mut out = CleanOutcome { rows_in: rows.len(), ..Default::default() };
    for (i, r) in rows.iter().enumerate() {
        match clean_row(r) {
            Ok(rs) => {
                out.rows_ok += 1;
                out.readings.extend(rs);
            }
            Err(reason) => out.rejected.push(Rejection { row: i + 1, reason }),
        }
    }
    out
}

pub fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        0.0
    } else {
        xs.iter().sum::<f64>() / xs.len() as f64
    }
}

/// Population standard deviation.
pub fn std_dev(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let m = mean(xs);
    (xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / xs.len() as f64).sqrt()
}

/// Tunables for anomaly detection.
///
/// A day is judged against a *trailing baseline*: the daily averages of the previous
/// `window_days` calendar days. The day itself is never part of its own baseline, and days
/// that were gross outliers (|z| at least twice the threshold) are left out, so one spike
/// cannot inflate the spread and hide the days that follow it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnomalyConfig {
    /// |z| at or above this marks a day as anomalous.
    pub z_threshold: f64,
    /// Length of the trailing baseline window, in calendar days.
    pub window_days: u32,
    /// Minimum number of baseline days required before a day can be scored at all.
    pub min_baseline: u32,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        AnomalyConfig { z_threshold: DEFAULT_Z_THRESHOLD, window_days: ROLLING_DAYS as u32, min_baseline: 3 }
    }
}

/// Trailing-window z-scores. Returns `(z, anomaly)` per input day; `days` must be sorted ascending.
///
/// `min_spread` is a noise floor for the baseline standard deviation (in the metric's own unit):
/// in a very calm week the raw deviation is tiny, and without a floor an ordinary wiggle would
/// score as a huge z. A day with fewer than `min_baseline` usable baseline days scores 0 and is
/// never flagged.
pub fn rolling_z_scores(days: &[NaiveDate], values: &[f64], cfg: &AnomalyConfig, min_spread: f64) -> (Vec<f64>, Vec<bool>) {
    let n = values.len();
    let mut z = vec![0.0; n];
    let mut flagged = vec![false; n];
    let mut gross = vec![false; n];
    let min_baseline = cfg.min_baseline.max(2) as usize;
    let floor = min_spread.max(1e-9);
    for i in 0..n {
        let baseline: Vec<f64> = (0..i)
            .filter(|&j| !gross[j] && (days[i] - days[j]).num_days() <= i64::from(cfg.window_days))
            .map(|j| values[j])
            .collect();
        if baseline.len() < min_baseline {
            continue;
        }
        z[i] = (values[i] - mean(&baseline)) / std_dev(&baseline).max(floor);
        flagged[i] = z[i].abs() >= cfg.z_threshold;
        gross[i] = z[i].abs() >= 2.0 * cfg.z_threshold;
    }
    (z, flagged)
}

/// Trailing mean over a calendar window of `window` days (inclusive of the current day).
/// `days` must be sorted ascending.
pub fn rolling_mean(days: &[NaiveDate], values: &[f64], window: i64) -> Vec<f64> {
    (0..values.len())
        .map(|i| {
            let lo = (0..=i).find(|&j| (days[i] - days[j]).num_days() < window).unwrap_or(i);
            mean(&values[lo..=i])
        })
        .collect()
}

/// Build daily aggregates (min/max/avg, rolling 7-day average, trailing-window z-score, anomaly flag)
/// for every city/metric present in `readings`.
pub fn aggregate(readings: &[Reading], cfg: &AnomalyConfig) -> Vec<DailyAgg> {
    let mut groups: BTreeMap<(String, Metric), BTreeMap<NaiveDate, Vec<f64>>> = BTreeMap::new();
    for r in readings {
        let Some(day) = r.ts.get(..10).and_then(|d| NaiveDate::parse_from_str(d, "%Y-%m-%d").ok()) else {
            continue;
        };
        groups.entry((r.city.clone(), r.metric)).or_default().entry(day).or_default().push(r.value);
    }

    let mut out = Vec::new();
    for ((city, metric), by_day) in groups {
        let days: Vec<NaiveDate> = by_day.keys().copied().collect();
        let avgs: Vec<f64> = by_day.values().map(|v| mean(v)).collect();
        let rolling = rolling_mean(&days, &avgs, ROLLING_DAYS);
        let (zs, flags) = rolling_z_scores(&days, &avgs, cfg, metric.min_spread());
        for (i, (day, vals)) in by_day.iter().enumerate() {
            out.push(DailyAgg {
                city: city.clone(),
                metric,
                day: day.format("%Y-%m-%d").to_string(),
                min: round(vals.iter().cloned().fold(f64::INFINITY, f64::min), 2),
                max: round(vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max), 2),
                avg: round(avgs[i], 2),
                count: vals.len() as u32,
                rolling7: round(rolling[i], 2),
                zscore: round(zs[i], 3),
                anomaly: flags[i],
            });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPREAD: f64 = 0.1;

    fn raw(city: &str, ts: &str, t: &str, unit: &str, h: &str, w: &str) -> RawRow {
        let o = |s: &str| if s.is_empty() { None } else { Some(s.to_string()) };
        RawRow { city: o(city), timestamp: o(ts), temperature: o(t), unit: o(unit), humidity: o(h), wind_speed: o(w) }
    }

    fn reading(city: &str, ts: &str, v: f64) -> Reading {
        Reading { city: city.into(), ts: ts.into(), metric: Metric::Temperature, value: v }
    }

    #[test]
    fn timestamp_formats_are_normalised() {
        assert_eq!(parse_timestamp("2026-10-01T05:00").as_deref(), Some("2026-10-01T05:00"));
        assert_eq!(parse_timestamp("2026-10-01 05:00:30").as_deref(), Some("2026-10-01T05:00"));
        assert_eq!(parse_timestamp("2026-10-01T05:00:00+02:00").as_deref(), Some("2026-10-01T03:00"));
        assert_eq!(parse_timestamp("yesterday"), None);
    }

    #[test]
    fn unit_conversion() {
        assert!((to_celsius(50.0, "F").unwrap() - 10.0).abs() < 1e-9);
        assert!((to_celsius(300.0, "k").unwrap() - 26.85).abs() < 1e-9);
        assert_eq!(to_celsius(12.5, "").unwrap(), 12.5);
        assert!(to_celsius(1.0, "X").is_err());
    }

    #[test]
    fn clean_normalises_and_rejects() {
        let rows = vec![
            raw("Berlin", "2026-10-01T00:00", "50", "F", "80", "12"),
            raw("  Tokyo ", "2026-10-01 01:00:00", "20", "C", "", ""), // optional fields absent
            raw("", "2026-10-01T00:00", "10", "C", "", ""),             // no city
            raw("Berlin", "nope", "10", "C", "", ""),                   // bad ts
            raw("Berlin", "2026-10-01T00:00", "abc", "C", "", ""),      // NaN temperature
            raw("Berlin", "2026-10-01T00:00", "999", "C", "", ""),      // out of range
            raw("Berlin", "2026-10-01T00:00", "10", "C", "150", ""),    // humidity > 100
            raw("Berlin", "2026-10-01T00:00", "10", "C", "50", "-3"),   // negative wind
            raw("Berlin", "2026-10-01T00:00", "", "C", "50", "3"),      // missing temperature
        ];
        let out = clean(&rows);
        assert_eq!(out.rows_in, 9);
        assert_eq!(out.rows_ok, 2);
        assert_eq!(out.rejected.len(), 7);
        assert_eq!(out.readings.len(), 4); // 3 + 1
        assert_eq!(out.readings[0].value, 10.0); // 50F -> 10C
        assert_eq!(out.readings[3].city, "Tokyo");
        assert_eq!(out.rejected[0].row, 3);
    }

    #[test]
    fn daily_min_max_avg() {
        let rs = vec![
            reading("A", "2026-10-01T00:00", 10.0),
            reading("A", "2026-10-01T12:00", 20.0),
            reading("A", "2026-10-01T18:00", 30.0),
            reading("A", "2026-10-02T00:00", 5.0),
        ];
        let d = aggregate(&rs, &AnomalyConfig::default());
        assert_eq!(d.len(), 2);
        assert_eq!((d[0].min, d[0].max, d[0].avg, d[0].count), (10.0, 30.0, 20.0, 3));
        assert_eq!(d[1].day, "2026-10-02");
    }

    #[test]
    fn rolling_mean_uses_calendar_window() {
        let d = |n: u32| NaiveDate::from_ymd_opt(2026, 10, n).unwrap();
        let days: Vec<_> = (1..=9).map(d).collect();
        let vals: Vec<f64> = (1..=9).map(f64::from).collect();
        let r = rolling_mean(&days, &vals, 7);
        assert_eq!(r[0], 1.0);
        assert_eq!(r[2], 2.0); // mean(1,2,3)
        assert_eq!(r[8], 6.0); // mean(3..=9)

        // A gap in the data shrinks the window instead of reaching back by index.
        let days = vec![d(1), d(2), d(20)];
        let r = rolling_mean(&days, &[10.0, 20.0, 100.0], 7);
        assert_eq!(r[2], 100.0);
    }

    fn days(n: u32) -> Vec<NaiveDate> {
        (1..=n).map(|d| NaiveDate::from_ymd_opt(2026, 10, d).unwrap()).collect()
    }

    #[test]
    fn rolling_zscore_flags_spike_and_keeps_baseline_clean() {
        let mut v: Vec<f64> = (0..14).map(|i| 10.0 + (i % 3) as f64 * 0.3).collect();
        v[8] = 30.0;
        let (z, flags) = rolling_z_scores(&days(14), &v, &AnomalyConfig::default(), SPREAD);
        assert!(flags[8] && z[8] > 5.0, "z = {}", z[8]);
        // The spike does not poison the baseline: the day after is normal, not masked or flagged.
        assert!(!flags[9] && z[9].abs() < 2.0, "z9 = {}", z[9]);
        assert_eq!(flags.iter().filter(|f| **f).count(), 1);
    }

    #[test]
    fn rolling_zscore_adapts_to_a_trend_unlike_a_global_one() {
        // A steady climb of 1 degree/day: every day is "normal" relative to the recent past,
        // but a global z-score would flag both ends of the series.
        let v: Vec<f64> = (0..20).map(|i| 5.0 + f64::from(i) + if i % 2 == 0 { 0.2 } else { -0.2 }).collect();
        let cfg = AnomalyConfig { z_threshold: 4.0, ..AnomalyConfig::default() };
        let (_, flags) = rolling_z_scores(&days(20), &v, &cfg, SPREAD);
        assert!(flags.iter().all(|f| !f));
    }

    #[test]
    fn rolling_zscore_needs_a_baseline() {
        let (z, f) = rolling_z_scores(&days(3), &[1.0, 1.5, 100.0], &AnomalyConfig::default(), SPREAD);
        assert_eq!(z, vec![0.0; 3]);
        assert_eq!(f, vec![false; 3]);
        let (_, f) = rolling_z_scores(&days(5), &[1.0, 1.5, 0.5, 1.0, 100.0], &AnomalyConfig::default(), SPREAD);
        assert!(f[4], "fifth day has three baseline days and must be scored");
        assert!(rolling_z_scores(&[], &[], &AnomalyConfig::default(), SPREAD).0.is_empty());
    }

    #[test]
    fn rolling_zscore_window_is_configurable_and_calendar_based() {
        // Values far in the past fall out of the window.
        let d = |n: u32| NaiveDate::from_ymd_opt(2026, 10, n).unwrap();
        let ds = vec![d(1), d(2), d(3), d(4), d(20)];
        let vs = vec![0.0, 1.0, 0.0, 1.0, 50.0];
        let (_, f) = rolling_z_scores(&ds, &vs, &AnomalyConfig::default(), SPREAD);
        assert!(!f[4], "baseline from 16 days ago is outside the 7-day window");
        let wide = AnomalyConfig { window_days: 30, ..AnomalyConfig::default() };
        let (_, f) = rolling_z_scores(&ds, &vs, &wide, SPREAD);
        assert!(f[4]);
    }

    #[test]
    fn calm_baseline_uses_the_noise_floor_instead_of_exploding() {
        // A perfectly flat week: no deviation, no score; a real jump still stands out.
        let (z, f) = rolling_z_scores(&days(8), &[5.0; 8], &AnomalyConfig::default(), 0.5);
        assert_eq!(z, vec![0.0; 8]);
        assert!(f.iter().all(|x| !x));
        let (z, f) = rolling_z_scores(&days(5), &[5.0, 5.0, 5.0, 5.0, 9.0], &AnomalyConfig::default(), 0.5);
        assert!(f[4] && (z[4] - 8.0).abs() < 1e-9);
        // A small wiggle in a calm period is below the floor, not a "z = 40" event.
        let (_, f) = rolling_z_scores(&days(5), &[5.0, 5.0, 5.0, 5.0, 5.3], &AnomalyConfig::default(), 0.5);
        assert!(!f[4]);
    }

    #[test]
    fn gross_outliers_do_not_poison_later_baselines_but_mild_shifts_adapt() {
        // A level shift from 10 to 11 (z just above the threshold with a 0.3 floor) is flagged
        // briefly, then becomes the new normal because mildly-flagged days stay in the baseline.
        let mut v = vec![10.0, 10.1, 9.9, 10.0, 10.1, 9.9];
        v.extend([11.0; 8]);
        let cfg = AnomalyConfig { z_threshold: 2.0, ..AnomalyConfig::default() };
        let (_, f) = rolling_z_scores(&days(14), &v, &cfg, 0.3);
        assert!(f[6], "the shift itself is flagged");
        assert!(!f[13], "a week later it is normal: {f:?}");
    }

    #[test]
    fn aggregate_marks_anomalous_day() {
        let mut rs = Vec::new();
        for day in 1..=12u32 {
            let v = if day == 8 { 25.0 } else { 10.0 + (day % 3) as f64 * 0.2 };
            rs.push(reading("Berlin", &format!("2026-10-{day:02}T12:00"), v));
        }
        rs.push(reading("Oslo", "2026-10-01T12:00", 1.0)); // separate series, untouched
        let d = aggregate(&rs, &AnomalyConfig::default());
        let flagged: Vec<_> = d.iter().filter(|x| x.anomaly).collect();
        assert_eq!(flagged.len(), 1);
        assert_eq!(flagged[0].day, "2026-10-08");
        assert!(flagged[0].zscore > 2.0);
        assert!(d.iter().filter(|x| x.city == "Oslo").all(|x| !x.anomaly));
    }
}
