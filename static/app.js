"use strict";
// PulseETL dashboard: vanilla JS, charts drawn as inline SVG (no external dependencies).

const $ = (id) => document.getElementById(id);
const NS = "http://www.w3.org/2000/svg";
const state = { overview: null, city: null, metric: "temperature", series: null, running: false };

/* ---------- helpers ---------- */
function el(tag, attrs = {}, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v === false || v == null) continue;
    if (k === "text") n.textContent = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v === true ? "" : v);
  }
  for (const k of kids.flat()) if (k != null) n.append(k);
  return n;
}
function svg(tag, attrs = {}, ...kids) {
  const n = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
  for (const k of kids.flat()) if (k != null) n.append(k);
  return n;
}
const fmt = (v, d = 1) => (v == null || Number.isNaN(v) ? "–" : Number(v).toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d }));
const int = (v) => Number(v).toLocaleString();
const metricInfo = (name) => state.overview.metrics.find((m) => m.name === name);
const shortDay = (d) => new Date(d + "T00:00:00").toLocaleDateString(undefined, { month: "short", day: "numeric" });
const longDay = (d) => new Date(d + "T00:00:00").toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric", year: "numeric" });

function ago(iso) {
  if (!iso) return "–";
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return "just now";
  if (s < 3600) return Math.floor(s / 60) + " min ago";
  if (s < 86400) return Math.floor(s / 3600) + " h ago";
  return Math.floor(s / 86400) + " d ago";
}
function duration(r) {
  if (!r.finished_at) return r.status === "running" ? "running…" : "–";
  const s = (new Date(r.finished_at) - new Date(r.started_at)) / 1000;
  return s < 1 ? "<1 s" : s < 60 ? Math.round(s) + " s" : Math.floor(s / 60) + " m " + Math.round(s % 60) + " s";
}
function toast(msg) {
  const t = $("toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(toast.t);
  toast.t = setTimeout(() => t.classList.remove("show"), 4200);
}
async function api(path, opts) {
  const r = await fetch(path, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw Object.assign(new Error(body.error || r.statusText), { status: r.status });
  return body;
}
// Optional API token for POST /api/ingest (only needed when the server sets PULSE_API_TOKEN).
const tokenStore = {
  get() { try { return sessionStorage.getItem("pulse-token") || ""; } catch (e) { return ""; } },
  set(v) { try { v ? sessionStorage.setItem("pulse-token", v) : sessionStorage.removeItem("pulse-token"); } catch (e) { /* ignore */ } },
};
async function ingest() {
  const call = () => { const t = tokenStore.get(); return api("/api/ingest", { method: "POST", headers: t ? { Authorization: "Bearer " + t } : {} }); };
  try { return await call(); } catch (e) {
    if (e.status !== 401) throw e;
    const t = window.prompt("This server protects \"Run now\" with an API token. Enter it:");
    if (!t) throw e;
    tokenStore.set(t.trim());
    try { return await call(); } catch (e2) { if (e2.status === 401) tokenStore.set(""); throw e2; }
  }
}
const badge = (status) => el("span", { class: "badge " + status, text: status });

/* ---------- theme ---------- */
function applyTheme(t) {
  document.documentElement.setAttribute("data-theme", t);
  $("theme-icon").textContent = t === "dark" ? "☾" : "☀";
  $("theme").setAttribute("aria-pressed", String(t === "dark"));
  try { localStorage.setItem("pulse-theme", t); } catch (e) { /* ignore */ }
}
function currentTheme() {
  return document.documentElement.getAttribute("data-theme") || (matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
}
$("theme").addEventListener("click", () => { applyTheme(currentTheme() === "dark" ? "light" : "dark"); });
applyTheme(currentTheme());

/* ---------- chart ---------- */
function drawChart(host, data) {
  host.replaceChildren();
  const pts = data.points;
  if (!pts.length) { host.append(el("div", { class: "empty", text: "No data yet. Click “Run now” to ingest." })); return; }

  const W = Math.max(300, host.clientWidth), small = W < 560;
  const H = small ? 260 : 340;
  const m = { t: 14, r: small ? 12 : 20, b: 30, l: small ? 40 : 50 };
  const iw = W - m.l - m.r, ih = H - m.t - m.b;
  const lo0 = Math.min(...pts.map((p) => p.min)), hi0 = Math.max(...pts.map((p) => p.max));
  const rawStep = ((hi0 - lo0 || 1) * 1.1) / 5, mag = 10 ** Math.floor(Math.log10(rawStep));
  const step = [1, 2, 2.5, 5, 10].map((k) => k * mag).find((s) => s >= rawStep);
  const lo = Math.floor(lo0 / step) * step, hi = Math.max(Math.ceil(hi0 / step) * step, lo + step);
  const x = (i) => m.l + (pts.length === 1 ? iw / 2 : (i / (pts.length - 1)) * iw);
  const y = (v) => m.t + (1 - (v - lo) / (hi - lo)) * ih;
  const path = (key) => pts.map((p, i) => (i ? "L" : "M") + x(i).toFixed(1) + " " + y(p[key]).toFixed(1)).join("");

  const nAnom = pts.filter((p) => p.anomaly).length;
  const root = svg("svg", {
    viewBox: `0 0 ${W} ${H}`, role: "application", tabindex: "0",
    "aria-label": `${data.label} for ${data.city}, ${pts.length} days, ${nAnom} anomalies. Use left and right arrow keys to inspect each day.`,
  });

  const grid = svg("g", { class: "grid" }), axis = svg("g", { class: "ax" });
  for (let v = lo; v <= hi + step / 1e6; v += step) {
    const yy = y(v);
    grid.append(svg("line", { x1: m.l, x2: W - m.r, y1: yy, y2: yy }));
    axis.append(svg("text", { x: m.l - 8, y: yy + 4, "text-anchor": "end" }, document.createTextNode(fmt(v, step < 1 ? 1 : 0))));
  }
  const xt = Math.min(pts.length, small ? 4 : 8);
  for (let k = 0; k < xt; k++) {
    const i = xt === 1 ? 0 : Math.round((k / (xt - 1)) * (pts.length - 1));
    axis.append(svg("text", { x: x(i), y: H - 8, "text-anchor": k === 0 ? "start" : k === xt - 1 ? "end" : "middle" }, document.createTextNode(shortDay(pts[i].day))));
  }
  axis.append(svg("text", { x: m.l, y: m.t - 3, "text-anchor": "start" }, document.createTextNode(data.unit)));

  const band = pts.map((p, i) => (i ? "L" : "M") + x(i).toFixed(1) + " " + y(p.max).toFixed(1)).join("")
    + pts.map((p, i) => "L" + x(pts.length - 1 - i).toFixed(1) + " " + y(pts[pts.length - 1 - i].min).toFixed(1)).join("") + "Z";

  root.append(grid, axis, svg("path", { class: "band", d: pts.length > 1 ? band : "" }),
    svg("path", { class: "l-roll", d: path("rolling7") }), svg("path", { class: "l-avg", d: path("avg") }));
  pts.forEach((p, i) => {
    if (p.anomaly) root.append(svg("circle", { class: "pt-anom", cx: x(i), cy: y(p.avg), r: 6 }));
    else if (pts.length < 40) root.append(svg("circle", { class: "pt", cx: x(i), cy: y(p.avg), r: 2.5 }));
  });

  const cross = svg("line", { class: "cross", y1: m.t, y2: m.t + ih, visibility: "hidden" });
  const dot = svg("circle", { class: "pt", r: 5, visibility: "hidden", stroke: "var(--surface)", "stroke-width": 2 });
  root.append(cross, dot);
  const tip = el("div", { class: "tip", hidden: true });
  host.append(root, tip);

  let active = -1;
  function show(i) {
    active = Math.max(0, Math.min(pts.length - 1, i));
    const p = pts[active], px = x(active);
    cross.setAttribute("x1", px); cross.setAttribute("x2", px); cross.setAttribute("visibility", "visible");
    dot.setAttribute("cx", px); dot.setAttribute("cy", y(p.avg)); dot.setAttribute("visibility", "visible");
    const row = (k, v) => el("div", { class: "row" }, el("span", { text: k }), el("span", { text: v }));
    tip.replaceChildren(el("b", { text: longDay(p.day) }), row("Average", fmt(p.avg) + " " + data.unit),
      row("Min / max", `${fmt(p.min)} / ${fmt(p.max)}`), row("7-day avg", fmt(p.rolling7) + " " + data.unit),
      row("z-score", fmt(p.zscore, 2)), p.anomaly ? el("div", { class: "flag", text: "⚠ Anomaly" }) : null);
    tip.hidden = false;
    const scale = host.clientWidth / W, tw = tip.offsetWidth;
    let left = px * scale + 14;
    if (left + tw > host.clientWidth) left = px * scale - tw - 14;
    tip.style.left = Math.max(0, left) + "px";
    tip.style.top = Math.max(0, y(p.avg) * scale - tip.offsetHeight - 10) + "px";
    if (parseFloat(tip.style.top) < 4) tip.style.top = y(p.avg) * scale + 14 + "px";
  }
  function hide() { tip.hidden = true; cross.setAttribute("visibility", "hidden"); dot.setAttribute("visibility", "hidden"); active = -1; }
  const idxAt = (ev) => {
    const r = root.getBoundingClientRect();
    const vx = ((ev.clientX - r.left) / r.width) * W;
    return Math.round(((vx - m.l) / iw) * (pts.length - 1));
  };
  root.addEventListener("pointermove", (ev) => show(idxAt(ev)));
  root.addEventListener("pointerdown", (ev) => show(idxAt(ev)));
  root.addEventListener("pointerleave", (ev) => { if (ev.pointerType === "mouse" && document.activeElement !== root) hide(); });
  root.addEventListener("blur", hide);
  root.addEventListener("keydown", (ev) => {
    const step = { ArrowLeft: -1, ArrowRight: 1, Home: -9999, End: 9999 }[ev.key];
    if (step === undefined) return;
    ev.preventDefault();
    show(active < 0 ? (step < 0 ? pts.length - 1 : 0) : active + step);
  });
}

function drawTable(data) {
  const t = $("data-table");
  t.replaceChildren(
    el("thead", {}, el("tr", {}, ...["Day", "Min", "Max", "Avg", "7-day avg", "z-score", "Anomaly"].map((h) => el("th", { text: h })))),
    el("tbody", {}, ...data.points.map((p) => el("tr", {}, el("td", { text: p.day }), el("td", { text: fmt(p.min) }), el("td", { text: fmt(p.max) }),
      el("td", { text: fmt(p.avg) }), el("td", { text: fmt(p.rolling7) }), el("td", { text: fmt(p.zscore, 2) }), el("td", { text: p.anomaly ? "yes" : "no" })))),
  );
}

function spark(points) {
  const w = 120, h = 36;
  if (!points.length) return svg("svg", { viewBox: `0 0 ${w} ${h}`, "aria-hidden": "true", preserveAspectRatio: "none" });
  const vals = points.map((p) => p.avg), lo = Math.min(...vals), hi = Math.max(...vals), span = hi - lo || 1;
  const X = (i) => 3 + (i / Math.max(1, points.length - 1)) * (w - 6), Y = (v) => 4 + (1 - (v - lo) / span) * (h - 8);
  const s = svg("svg", { viewBox: `0 0 ${w} ${h}`, "aria-hidden": "true", preserveAspectRatio: "none" },
    svg("path", { d: points.map((p, i) => (i ? "L" : "M") + X(i).toFixed(1) + " " + Y(p.avg).toFixed(1)).join(""), "vector-effect": "non-scaling-stroke" }));
  points.forEach((p, i) => { if (p.anomaly) s.append(svg("circle", { cx: X(i), cy: Y(p.avg), r: 2.8 })); });
  return s;
}

/* ---------- sections ---------- */
function renderKpis() {
  const o = state.overview, info = metricInfo(state.metric);
  const pts = state.series ? state.series.points : [];
  const last = pts[pts.length - 1];
  const selAnom = pts.filter((p) => p.anomaly).length;
  const run = o.last_run;
  const diff = last ? last.avg - last.rolling7 : 0;
  const tile = (label, value, unit, sub) => el("div", { class: "kpi" }, el("div", { class: "label", text: label }),
    el("div", { class: "value" }, value, unit ? el("small", { text: unit }) : null), el("div", { class: "sub" }, sub));
  $("kpis").replaceChildren(
    tile(`Latest ${info.label.toLowerCase()}`, last ? fmt(last.avg) : "–", info.unit,
      last ? [el("span", { class: "delta " + (diff >= 0 ? "up" : "down"), text: (diff >= 0 ? "▲ " : "▼ ") + fmt(Math.abs(diff)) }), ` vs 7-day avg · ${shortDay(last.day)}`] : "no data"),
    tile("Anomalies flagged", int(o.totals.anomalies), "", `${selAnom} for ${state.city || "–"} · ${info.name}`),
    tile("Readings stored", int(o.totals.readings), "", `${o.totals.cities} cities · ${int(o.totals.daily_points)} daily points`),
    tile("Last run", run ? "#" + run.id : "–", "", run ? [badge(run.status), ` ${ago(run.finished_at || run.started_at)}`] : "never run"),
  );
}

function renderAnomalies() {
  const list = state.overview.recent_anomalies;
  $("anomalies").replaceChildren(...(list.length ? list.map((a) => {
    const info = metricInfo(a.metric);
    return el("li", {}, el("span", { class: "what" }, el("b", { text: a.city }), ` · ${info.label}`, el("small", { text: `${longDay(a.day)} · avg ${fmt(a.avg)} ${info.unit}` })),
      el("span", { class: "z", text: (a.zscore >= 0 ? "+" : "") + fmt(a.zscore, 2) + " σ" }));
  }) : [el("li", { text: "No anomalies detected." })]));
}

function renderRuns(runs) {
  $("runs-sub").textContent = runs.length ? `latest ${runs.length}` : "";
  const body = document.querySelector("#runs tbody");
  if (!runs.length) { body.replaceChildren(el("tr", {}, el("td", { colspan: 9, text: "No runs yet." }))); return; }
  const cells = [
    ["Run", (r) => "#" + r.id], ["Status", (r) => badge(r.status)], ["Source", (r) => r.source],
    ["Started", (r) => el("span", { title: r.started_at, text: ago(r.started_at) })], ["Duration", duration],
    ["In", (r) => int(r.rows_in), "num"], ["Loaded", (r) => int(r.rows_out), "num"], ["Rejected", (r) => int(r.rows_rejected), "num"],
    ["Notes", (r) => r.message || "–", "notes"],
  ];
  body.replaceChildren(...runs.map((r) => el("tr", {}, ...cells.map(([label, f, cls]) => el("td", { "data-label": label, class: cls }, f(r))))));
}

function renderCities(all) {
  const info = metricInfo(state.metric);
  $("cmp-sub").textContent = `${info.label} · daily average`;
  $("cities").replaceChildren(...all.map(({ city, data }) => {
    const pts = data.points, last = pts[pts.length - 1], n = pts.filter((p) => p.anomaly).length;
    return el("button", { class: "spark", type: "button", "aria-pressed": String(city === state.city),
      "aria-label": `${city}: ${last ? fmt(last.avg) + " " + info.unit : "no data"}, ${n} anomalies. Select`,
      onclick: () => { $("city").value = city; selectCity(city); } },
      el("div", { class: "name" }, el("span", { text: city }), n ? el("span", { class: "badge-n", text: n + " ⚠" }) : null),
      el("div", { class: "val", text: last ? `${fmt(last.avg)} ${info.unit}` : "–" }), spark(pts));
  }));
}

function renderHealth(h) {
  const p = $("health");
  if (h) { p.textContent = `API ok · ${h.source}`; p.className = "pill ok"; $("ver").textContent = h.version; }
  else { p.textContent = "API unreachable"; p.className = "pill bad"; }
}

/* ---------- data flow ---------- */
let seriesToken = 0;
async function loadSeries() {
  const token = ++seriesToken;
  if (!state.city) { state.series = null; drawChart($("chart"), { points: [] }); renderKpis(); return; }
  const q = (c) => api(`/api/series?city=${encodeURIComponent(c)}&metric=${state.metric}&limit=1000`);
  const [main, ...rest] = await Promise.all([q(state.city), ...state.overview.cities.map((c) => q(c).then((data) => ({ city: c, data })))]);
  if (token !== seriesToken) return;
  state.series = main;
  $("chart-title").textContent = `${main.label} · ${main.city}`;
  drawChart($("chart"), main);
  drawTable(main);
  renderCities(rest);
  renderKpis();
}

async function refresh(initial) {
  try {
    const [overview, runs, health] = await Promise.all([api("/api/metrics"), api("/api/runs?limit=12"), api("/api/health").catch(() => null)]);
    state.overview = overview;
    renderHealth(health);
    // selectors
    const sel = $("city");
    sel.replaceChildren(...overview.cities.map((c) => el("option", { value: c, text: c })));
    if (!overview.cities.includes(state.city)) state.city = overview.cities[0] || null;
    if (state.city) sel.value = state.city;
    if (initial || !$("metric").children.length) buildMetricSelector();
    renderAnomalies();
    renderRuns(runs);
    renderKpis();
    await loadSeries();
  } catch (e) {
    renderHealth(null);
    toast("Could not load data: " + e.message);
  }
}

function buildMetricSelector() {
  $("metric").replaceChildren(...state.overview.metrics.map((m) =>
    el("label", {}, el("input", { type: "radio", name: "metric", value: m.name, checked: m.name === state.metric,
      onchange: () => { state.metric = m.name; loadSeries(); } }), el("span", { text: m.label }))));
}
function selectCity(c) { state.city = c; loadSeries(); }
$("city").addEventListener("change", (e) => selectCity(e.target.value));

$("run").addEventListener("click", async () => {
  if (state.running) return;
  state.running = true;
  const b = $("run");
  b.setAttribute("aria-busy", "true"); b.disabled = true; $("run-label").textContent = "Running…";
  try {
    const r = await ingest();
    toast(`Run #${r.id} ${r.status}: ${int(r.rows_out)} rows loaded, ${int(r.rows_rejected)} rejected` + (r.failures.length ? `, ${r.failures.length} source(s) failed` : ""));
  } catch (e) {
    toast(e.status === 409 ? "A run is already in progress" : e.status === 401 ? "Run not authorised: wrong or missing API token" : "Run failed: " + e.message);
  } finally {
    state.running = false; b.removeAttribute("aria-busy"); b.disabled = false; $("run-label").textContent = "Run now";
    refresh(false);
  }
});

let rz;
new ResizeObserver(() => {
  clearTimeout(rz);
  rz = setTimeout(() => { if (state.series) drawChart($("chart"), state.series); }, 120);
}).observe($("chart"));

refresh(true);
setInterval(() => { if (!document.hidden && !state.running) refresh(false); }, 30000);
