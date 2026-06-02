const { useState, useEffect, useMemo, useRef } = React;

// ── helpers ───────────────────────────────────────────────────────────────────
const usd = (n, sign) => {
  if (typeof n !== "number") return "—";
  const a = Math.abs(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (sign) return (n >= 0 ? "+$" : "-$") + a;
  return (n < 0 ? "-$" : "$") + a;
};
const pct = (n) => (typeof n === "number" ? (n * 100).toFixed(1) + "%" : "—");
const api = {
  get: (p) => fetch(p).then((r) => r.json()),
  post: (p, b) => fetch(p, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b) }).then((r) => r.json()),
};
const COLORS = ["#4ade80", "#60a5fa", "#f472b6", "#fbbf24"];

// Param editor layout: groups → field specs.
const NUM = (step) => ({ kind: "num", step: step || 0.01 });
const GROUPS = [
  ["Probability", {
    ensemble_fraction_weight: NUM(), gaussian_cdf_weight: NUM(),
    std_floor_high: NUM(0.5), std_floor_low: NUM(0.5),
    prob_floor: NUM(), prob_ceiling: NUM(),
    prob_floor_sizing: NUM(), prob_ceiling_sizing: NUM(),
  }],
  ["Source blend", {
    "source_weights.nws": NUM(), "source_weights.ecmwf": NUM(),
    "source_weights.gfs": NUM(), "source_weights.gem": NUM(),
    use_dynamic_brier_weights: { kind: "bool" },
    agreement_tight: NUM(), majority_band: NUM(),
    outlier_threshold: NUM(), outlier_dampen: NUM(),
  }],
  ["Filters", {
    model_divergence_threshold: NUM(0.5), climatology_deviation_max: NUM(0.5),
    obs_window_hours: NUM(1), cold_day_margin: NUM(0.5), cold_day_nws_min: NUM(),
    yes_entry_floor: NUM(), rain_entry_floor: NUM(),
    entry_min_price: NUM(), entry_max_price: NUM(),
    same_day_high_cutoff_hour: NUM(1), same_day_low_cutoff_hour: NUM(1),
    conviction_threshold: NUM(), model_data_max_age_hours: NUM(0.5),
  }],
  ["Sizing", {
    kelly_fraction: NUM(), min_edge_threshold: NUM(), low_confidence_edge_override: NUM(),
    max_trade_size: NUM(5), initial_bankroll: NUM(50),
    bankroll_basis: { kind: "sel", opts: ["cash", "equity"] },
  }],
  ["Execution", {
    fill_mode: { kind: "sel", opts: ["taker", "maker"] },
    maker_post_at: { kind: "sel", opts: ["mid", "bid", "bid_plus"] },
    maker_offset: NUM(), taker_fee_coef: NUM(), maker_fee_coef: NUM(),
  }],
  ["Liquidity", { min_ask_size: NUM(1), min_volume_24h: NUM(10) }],
];

function getPath(obj, path) {
  return path.split(".").reduce((o, k) => (o == null ? o : o[k]), obj);
}
function setPath(obj, path, val) {
  const next = JSON.parse(JSON.stringify(obj));
  const parts = path.split(".");
  let cur = next;
  for (let i = 0; i < parts.length - 1; i++) cur = cur[parts[i]];
  cur[parts[parts.length - 1]] = val;
  return next;
}

// ── equity curve (inline SVG) ───────────────────────────────────────────────
function EquityChart({ series, initial }) {
  const W = 760, H = 280, PAD = 44;
  const all = series.flatMap((s) => s.curve.map((p) => p.equity).filter((v) => typeof v === "number"));
  if (!all.length) return <div style={{ color: "#6b7280", padding: 40 }}>No equity data.</div>;
  let lo = Math.min(initial, ...all), hi = Math.max(initial, ...all);
  if (lo === hi) { lo -= 1; hi += 1; }
  const pad = (hi - lo) * 0.08; lo -= pad; hi += pad;
  const maxLen = Math.max(...series.map((s) => s.curve.length));
  const x = (i, n) => PAD + (i / Math.max(1, n - 1)) * (W - PAD - 10);
  const y = (v) => H - PAD - ((v - lo) / (hi - lo)) * (H - PAD - 14);
  const baseY = y(initial);
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", background: "#0d1117", borderRadius: 10, border: "1px solid #1f2630" }}>
      {[0, 0.25, 0.5, 0.75, 1].map((f, i) => {
        const v = lo + f * (hi - lo);
        return (<g key={i}>
          <line x1={PAD} y1={y(v)} x2={W - 10} y2={y(v)} stroke="#1b2129" />
          <text x={6} y={y(v) + 4} fill="#6b7280" fontSize="10">{usd(v)}</text>
        </g>);
      })}
      <line x1={PAD} y1={baseY} x2={W - 10} y2={baseY} stroke="#374151" strokeDasharray="4 4" />
      {series.map((s, si) => {
        const pts = s.curve.filter((p) => typeof p.equity === "number")
          .map((p, i, arr) => `${x(i, arr.length)},${y(p.equity)}`).join(" ");
        return <polyline key={si} points={pts} fill="none" stroke={s.color} strokeWidth="2" />;
      })}
    </svg>
  );
}

function Stat({ label, value, tone }) {
  const color = tone === "pos" ? "#4ade80" : tone === "neg" ? "#f87171" : "#e6edf3";
  return (
    <div style={{ background: "#11151c", border: "1px solid #1f2630", borderRadius: 10, padding: "12px 14px", minWidth: 120 }}>
      <div style={{ fontSize: 11, color: "#8b949e", textTransform: "uppercase", letterSpacing: 0.4 }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 600, color, marginTop: 4 }}>{value}</div>
    </div>
  );
}

function MetricCards({ m }) {
  if (!m) return null;
  const pnlTone = m.total_pnl > 0 ? "pos" : m.total_pnl < 0 ? "neg" : null;
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginBottom: 14 }}>
      <Stat label="Total P&L" value={usd(m.total_pnl, true)} tone={pnlTone} />
      <Stat label="Final bankroll" value={usd(m.final_bankroll)} />
      <Stat label="ROI" value={pct(m.roi)} tone={pnlTone} />
      <Stat label="Hit rate" value={m.hit_rate == null ? "—" : pct(m.hit_rate)} />
      <Stat label="Resolved" value={`${m.n_resolved} (${m.wins}W/${m.losses}L)`} />
      <Stat label="Fill rate" value={m.fill_rate == null ? "—" : pct(m.fill_rate)} />
      <Stat label="Max DD" value={pct(m.max_drawdown)} />
      <Stat label="Brier" value={m.brier == null ? "—" : m.brier} />
      <Stat label="Signals/Filled" value={`${m.n_signals}/${m.n_filled}`} />
      <Stat label="Turnover" value={usd(m.turnover)} />
    </div>
  );
}

function Breakdown({ title, data }) {
  const keys = data ? Object.keys(data) : [];
  if (!keys.length) return null;
  return (
    <div style={{ flex: 1, minWidth: 220 }}>
      <h4 style={{ color: "#8b949e", fontSize: 12, margin: "6px 0", textTransform: "uppercase" }}>{title}</h4>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
        <tbody>
          {keys.map((k) => {
            const b = data[k];
            return (<tr key={k} style={{ borderBottom: "1px solid #1b2129" }}>
              <td style={{ padding: "4px 6px" }}>{k}</td>
              <td style={{ padding: "4px 6px", color: "#8b949e" }}>{b.wins}W/{b.losses}L</td>
              <td style={{ padding: "4px 6px", textAlign: "right", color: b.pnl >= 0 ? "#4ade80" : "#f87171" }}>{usd(b.pnl, true)}</td>
            </tr>);
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── main ────────────────────────────────────────────────────────────────────
function BacktestLab() {
  const [params, setParams] = useState(null);
  const [strategies, setStrategies] = useState(["default"]);
  const [strategy, setStrategy] = useState("default");
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [cities, setCities] = useState("");
  const [name, setName] = useState("");
  const [runs, setRuns] = useState([]);
  const [active, setActive] = useState(null);     // loaded run detail
  const [compareIds, setCompareIds] = useState([]); // run_ids overlaid
  const [compareData, setCompareData] = useState({});
  const [status, setStatus] = useState("");
  const pollRef = useRef(null);

  useEffect(() => {
    api.get("/api/backtest/params/defaults").then((d) => setParams(d.params));
    api.get("/api/backtest/strategies").then((d) => setStrategies(d.strategies || ["default"]));
    refreshRuns();
    // default date range = last 14 days
    const today = new Date();
    const past = new Date(today.getTime() - 13 * 86400000);
    setEnd(today.toISOString().slice(0, 10));
    setStart(past.toISOString().slice(0, 10));
  }, []);

  const refreshRuns = () => api.get("/api/backtest/runs?limit=40").then((d) => setRuns(d.runs || []));

  const runBacktest = async () => {
    setStatus("queued…");
    const body = {
      strategy, start, end, name: name || null,
      cities: cities.split(",").map((c) => c.trim()).filter(Boolean),
      params,
    };
    const res = await api.post("/api/backtest/run", body);
    if (res.error) { setStatus("error: " + res.error); return; }
    poll(res.run_id);
  };

  const poll = (runId) => {
    clearInterval(pollRef.current);
    const tick = async () => {
      const r = await api.get(`/api/backtest/runs/${runId}`);
      setStatus(r.status === "done" ? "" : r.status + "…");
      if (r.status === "done") { clearInterval(pollRef.current); setActive(r); refreshRuns(); }
      else if (r.status === "error") { clearInterval(pollRef.current); setStatus("error: " + (r.error || "?")); refreshRuns(); }
    };
    tick();
    pollRef.current = setInterval(tick, 1500);
  };

  const loadRun = async (runId) => {
    const r = await api.get(`/api/backtest/runs/${runId}`);
    setActive(r);
  };

  const toggleCompare = async (runId) => {
    let ids = compareIds.includes(runId) ? compareIds.filter((x) => x !== runId) : [...compareIds, runId].slice(-4);
    setCompareIds(ids);
    const data = { ...compareData };
    for (const id of ids) if (!data[id]) data[id] = await api.get(`/api/backtest/runs/${id}`);
    setCompareData(data);
  };

  const chartSeries = useMemo(() => {
    if (compareIds.length) {
      return compareIds.map((id, i) => ({
        curve: (compareData[id] && compareData[id].equity_curve) || [],
        color: COLORS[i % COLORS.length],
        label: id.slice(0, 6),
      }));
    }
    if (active && active.equity_curve) return [{ curve: active.equity_curve, color: COLORS[0], label: "run" }];
    return [];
  }, [active, compareIds, compareData]);

  const initialBankroll = (active && active.initial_bankroll) || (params && params.initial_bankroll) || 1000;

  const field = (path, spec) => {
    const v = getPath(params, path);
    const onNum = (e) => setParams(setPath(params, path, e.target.value === "" ? "" : Number(e.target.value)));
    const label = path.replace("source_weights.", "w·");
    const common = { background: "#0d1117", color: "#e6edf3", border: "1px solid #283039", borderRadius: 6, padding: "4px 6px", width: "100%", fontSize: 12 };
    let input;
    if (spec.kind === "bool")
      input = <input type="checkbox" checked={!!v} onChange={(e) => setParams(setPath(params, path, e.target.checked))} />;
    else if (spec.kind === "sel")
      input = <select value={v} onChange={(e) => setParams(setPath(params, path, e.target.value))} style={common}>{spec.opts.map((o) => <option key={o} value={o}>{o}</option>)}</select>;
    else
      input = <input type="number" step={spec.step} value={v} onChange={onNum} style={common} />;
    return (
      <label key={path} style={{ display: "flex", flexDirection: "column", gap: 2 }}>
        <span style={{ fontSize: 10.5, color: "#8b949e" }}>{label}</span>
        {input}
      </label>
    );
  };

  if (!params) return <div style={{ padding: 40 }}>Loading…</div>;

  return (
    <div style={{ maxWidth: 1280, margin: "0 auto", padding: 20 }}>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 16 }}>
        <h1 style={{ fontSize: 22 }}>🧪 Backtest Lab</h1>
        <a href="/" style={{ color: "#60a5fa", fontSize: 13, textDecoration: "none" }}>← Mission Control</a>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "340px 1fr", gap: 18, alignItems: "start" }}>
        {/* ── left: config + param editor ── */}
        <div style={{ background: "#11151c", border: "1px solid #1f2630", borderRadius: 12, padding: 14 }}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginBottom: 10 }}>
            <label style={{ gridColumn: "1 / 3", fontSize: 11, color: "#8b949e" }}>Strategy
              <select value={strategy} onChange={(e) => setStrategy(e.target.value)} style={{ width: "100%", background: "#0d1117", color: "#e6edf3", border: "1px solid #283039", borderRadius: 6, padding: "5px 6px", marginTop: 3 }}>
                {strategies.map((s) => <option key={s} value={s}>{s}</option>)}
              </select>
            </label>
            <label style={{ fontSize: 11, color: "#8b949e" }}>Start
              <input type="date" value={start} onChange={(e) => setStart(e.target.value)} style={{ width: "100%", background: "#0d1117", color: "#e6edf3", border: "1px solid #283039", borderRadius: 6, padding: "4px 6px", marginTop: 3 }} />
            </label>
            <label style={{ fontSize: 11, color: "#8b949e" }}>End
              <input type="date" value={end} onChange={(e) => setEnd(e.target.value)} style={{ width: "100%", background: "#0d1117", color: "#e6edf3", border: "1px solid #283039", borderRadius: 6, padding: "4px 6px", marginTop: 3 }} />
            </label>
            <label style={{ fontSize: 11, color: "#8b949e" }}>Cities (blank=all)
              <input value={cities} onChange={(e) => setCities(e.target.value)} placeholder="nyc,chicago" style={{ width: "100%", background: "#0d1117", color: "#e6edf3", border: "1px solid #283039", borderRadius: 6, padding: "4px 6px", marginTop: 3 }} />
            </label>
            <label style={{ fontSize: 11, color: "#8b949e" }}>Label
              <input value={name} onChange={(e) => setName(e.target.value)} placeholder="optional" style={{ width: "100%", background: "#0d1117", color: "#e6edf3", border: "1px solid #283039", borderRadius: 6, padding: "4px 6px", marginTop: 3 }} />
            </label>
          </div>

          <button onClick={runBacktest} style={{ width: "100%", background: "#1f6feb", color: "white", border: "none", borderRadius: 8, padding: "9px", fontWeight: 600, cursor: "pointer", marginBottom: 4 }}>
            Run backtest
          </button>
          {status && <div style={{ fontSize: 12, color: status.startsWith("error") ? "#f87171" : "#fbbf24", textAlign: "center", padding: "4px 0" }}>{status}</div>}

          <div style={{ marginTop: 8 }}>
            {GROUPS.map(([title, fields]) => (
              <details key={title} style={{ marginBottom: 6, borderTop: "1px solid #1b2129", paddingTop: 6 }}>
                <summary style={{ cursor: "pointer", fontSize: 12, color: "#c9d1d9", fontWeight: 600 }}>{title}</summary>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, marginTop: 8 }}>
                  {Object.entries(fields).map(([path, spec]) => field(path, spec))}
                </div>
              </details>
            ))}
          </div>
        </div>

        {/* ── right: results ── */}
        <div>
          {compareIds.length > 1 && (
            <div style={{ marginBottom: 8, fontSize: 12, color: "#8b949e" }}>
              Comparing: {compareIds.map((id, i) => <span key={id} style={{ color: COLORS[i % COLORS.length] }}>● {id.slice(0, 6)} </span>)}
            </div>
          )}
          <EquityChart series={chartSeries} initial={initialBankroll} />
          {active && !compareIds.length && (
            <div style={{ marginTop: 14 }}>
              <h3 style={{ fontSize: 14, marginBottom: 6 }}>{active.name || active.run_id.slice(0, 8)} · {active.strategy} · {active.start}→{active.end}</h3>
              <MetricCards m={active.metrics} />
              <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                <Breakdown title="By city" data={active.metrics && active.metrics.by_city} />
                <Breakdown title="By metric" data={active.metrics && active.metrics.by_metric} />
                <Breakdown title="By agreement" data={active.metrics && active.metrics.by_agreement} />
              </div>
              {active.metrics && active.metrics.bid_thin_fills > 0 && (
                <div style={{ fontSize: 11, color: "#fbbf24", marginTop: 8 }}>
                  ⚠ {active.metrics.bid_thin_fills} maker fill(s) had no captured bid (posted at ask) — Kalshi's bulk feed often omits yes_bid.
                </div>
              )}
              {active.ledger && <Ledger rows={active.ledger} />}
            </div>
          )}
        </div>
      </div>

      {/* ── saved runs ── */}
      <div style={{ marginTop: 22 }}>
        <h3 style={{ fontSize: 14, marginBottom: 8, color: "#c9d1d9" }}>Saved runs</h3>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12.5 }}>
          <thead><tr style={{ color: "#8b949e", textAlign: "left" }}>
            {["", "label", "strategy", "window", "fills", "P&L", "final", "status", "cmp"].map((h) => <th key={h} style={{ padding: "6px 8px" }}>{h}</th>)}
          </tr></thead>
          <tbody>
            {runs.map((r) => (
              <tr key={r.run_id} style={{ borderTop: "1px solid #1b2129", cursor: "pointer" }}>
                <td style={{ padding: "6px 8px" }}><button onClick={() => loadRun(r.run_id)} style={{ background: "#21262d", color: "#e6edf3", border: "1px solid #30363d", borderRadius: 5, padding: "2px 8px", cursor: "pointer" }}>load</button></td>
                <td style={{ padding: "6px 8px" }} onClick={() => loadRun(r.run_id)}>{r.name || r.run_id.slice(0, 8)}</td>
                <td style={{ padding: "6px 8px", color: "#8b949e" }}>{r.strategy}</td>
                <td style={{ padding: "6px 8px", color: "#8b949e" }}>{r.start}→{r.end}</td>
                <td style={{ padding: "6px 8px" }}>{r.n_trades}</td>
                <td style={{ padding: "6px 8px", color: (r.total_pnl || 0) >= 0 ? "#4ade80" : "#f87171" }}>{usd(r.total_pnl, true)}</td>
                <td style={{ padding: "6px 8px" }}>{usd(r.final_bankroll)}</td>
                <td style={{ padding: "6px 8px", color: r.status === "done" ? "#4ade80" : r.status === "error" ? "#f87171" : "#fbbf24" }}>{r.status}</td>
                <td style={{ padding: "6px 8px" }}><input type="checkbox" checked={compareIds.includes(r.run_id)} onChange={() => toggleCompare(r.run_id)} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Ledger({ rows }) {
  if (!rows || !rows.length) return null;
  return (
    <div style={{ marginTop: 14 }}>
      <h4 style={{ color: "#8b949e", fontSize: 12, margin: "6px 0", textTransform: "uppercase" }}>Trade ledger ({rows.length})</h4>
      <div style={{ maxHeight: 360, overflow: "auto", border: "1px solid #1f2630", borderRadius: 8 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
          <thead><tr style={{ color: "#8b949e", textAlign: "left", position: "sticky", top: 0, background: "#11151c" }}>
            {["ticker", "side", "edge", "entry", "n", "fill", "result", "P&L"].map((h) => <th key={h} style={{ padding: "5px 8px" }}>{h}</th>)}
          </tr></thead>
          <tbody>
            {rows.map((t, i) => (
              <tr key={i} style={{ borderTop: "1px solid #1b2129" }}>
                <td style={{ padding: "4px 8px" }}>{t.ticker}</td>
                <td style={{ padding: "4px 8px", textTransform: "uppercase", color: t.bet_side === "yes" ? "#4ade80" : "#f472b6" }}>{t.bet_side}</td>
                <td style={{ padding: "4px 8px" }}>{pct(t.edge)}</td>
                <td style={{ padding: "4px 8px" }}>{pct(t.entry_price)}</td>
                <td style={{ padding: "4px 8px" }}>{t.contracts}</td>
                <td style={{ padding: "4px 8px", color: t.filled ? "#8b949e" : "#f87171" }}>{t.filled ? t.fill_mode : "unfilled"}</td>
                <td style={{ padding: "4px 8px", color: t.result === "win" ? "#4ade80" : t.result === "loss" ? "#f87171" : "#8b949e" }}>{t.result || "—"}</td>
                <td style={{ padding: "4px 8px", color: (t.pnl || 0) >= 0 ? "#4ade80" : "#f87171" }}>{t.pnl == null ? "—" : usd(t.pnl, true)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
