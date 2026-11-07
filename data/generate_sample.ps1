# Regenerates data/sample_weather.csv (deterministic, seed 42).
# Run from the project root:  powershell -File data/generate_sample.ps1
$inv = [Globalization.CultureInfo]::InvariantCulture
$rng = New-Object System.Random 42
$cities = @(
  @{n='Berlin';   t=11; h=75; w=14},
  @{n='Tokyo';    t=19; h=65; w=11},
  @{n='New York'; t=14; h=60; w=16},
  @{n='Sydney';   t=18; h=62; w=19}
)
$start = [datetime]'2026-10-01'
$lines = New-Object System.Collections.Generic.List[string]
$lines.Add('city,timestamp,temperature,unit,humidity,wind_speed')
foreach ($c in $cities) {
  for ($d = 0; $d -lt 21; $d++) {
    $day = $start.AddDays($d)
    $trend = 1.5 * [math]::Sin($d / 4.0)
    for ($h = 0; $h -lt 24; $h++) {
      $ph = ($h - 9) / 24.0 * 2 * [math]::PI
      $temp = $c.t + $trend + 4 * [math]::Sin($ph) + ($rng.NextDouble() - 0.5) * 1.6
      $hum  = $c.h - 12 * [math]::Sin($ph) + ($rng.NextDouble() - 0.5) * 6
      $wind = $c.w + 3 * [math]::Sin($h / 24.0 * 2 * [math]::PI) + ($rng.NextDouble() - 0.5) * 5
      # injected anomalies
      if ($c.n -eq 'Berlin' -and $d -eq 13) { $temp += 9 }                      # heat spike
      if ($c.n -eq 'Tokyo' -and $d -eq 8)  { $temp -= 7; $hum += 20; $wind += 35 } # storm
      if ($c.n -eq 'Sydney' -and $d -eq 16) { $wind += 30 }                     # gale
      $hum = [math]::Min(99, [math]::Max(5, $hum))
      $wind = [math]::Max(0, $wind)
      $unit = 'C'
      if ($c.n -eq 'New York' -and $d -ge 2 -and $d -le 4) { $temp = $temp * 9 / 5 + 32; $unit = 'F' }
      $ts = $day.AddHours($h).ToString('yyyy-MM-ddTHH:mm', $inv)
      $lines.Add(('{0},{1},{2},{3},{4},{5}' -f $c.n, $ts, $temp.ToString('0.0', $inv), $unit, $hum.ToString('0', $inv), $wind.ToString('0.0', $inv)))
    }
  }
}
# a few exact duplicates (idempotent upsert handles them)
1..3 | ForEach-Object { $lines.Add($lines[$rng.Next(1, $lines.Count)]) }
# deliberately bad rows (8) scattered through the file
$bad = @(
  ',2026-10-05T10:00,12.0,C,70,10',
  'Berlin,not-a-date,12.0,C,70,10',
  'Tokyo,2026-10-06T10:00,abc,C,70,10',
  'Sydney,2026-10-07T10:00,999,C,70,10',
  'Berlin,2026-10-08T10:00,12.0,C,140,10',
  'New York,2026-10-09T10:00,,C,60,12',
  'Tokyo,2026-10-10T10:00,15.0,X,60,12',
  'Sydney,2026-10-11T10:00,18.0,C,60,-5'
)
foreach ($b in $bad) { $lines.Insert($rng.Next(1, $lines.Count), $b) }
[IO.File]::WriteAllLines((Join-Path (Get-Location) 'data/sample_weather.csv'), $lines)
"rows (excluding header): $($lines.Count - 1)"

