const { useState, useEffect, useMemo, useRef } = React;

// ── Formatting helpers ────────────────────────────────────────────────────────
const usd = (n, showSign = false) => {
  if (typeof n !== "number") return "—";
  const abs = Math.abs(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (showSign) return (n >= 0 ? "+" : "-") + "$" + abs;
  return (n < 0 ? "-$" : "$") + abs;
};
const pct = (n) => (typeof n === "number" ? (n * 100).toFixed(1) + "%" : "—");

const CITY_SHORT = {
  nyc: "NYC", chicago: "CHI", miami: "MIA", los_angeles: "LAX",
  denver: "DEN", austin: "AUS", houston: "HOU", boston: "BOS",
  washington_dc: "DC", phoenix: "PHX", seattle: "SEA",
  san_francisco: "SFO", atlanta: "ATL", dallas: "DAL",
  las_vegas: "LAS", minneapolis: "MSP", new_orleans: "NOLA",
  oklahoma_city: "OKC", san_antonio: "SAT", philadelphia: "PHL",
};

function tradeLabel(t) {
  const city = CITY_SHORT[t.city] || t.city?.toUpperCase() || "?";
  const metric = t.metric === "high" ? "High" : t.metric === "low" ? "Low" : t.metric === "rain" ? "Rain" : (t.metric || "?");
  const dir = t.market_direction === "above" ? ">" : "<";
  const thresh = t.threshold_f != null ? `${t.threshold_f}°F` : "";
  const date = t.resolution_date ? fmtShortDate(t.resolution_date) : "";
  if (t.metric === "rain") return `${city} Rain — ${date}`;
  return `${city} ${metric} ${dir} ${thresh} — ${date}`;
}

function fmtShortDate(s) {
  const d = new Date(s + "T12:00:00");
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function fmtDate(isoStr) {
  if (!isoStr) return "—";
  const d = new Date(isoStr);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function getWeekStart(isoStr) {
  const d = new Date(isoStr);
  const day = d.getUTCDay(); // 0=Sun, 1=Mon
  const diff = day === 0 ? -6 : 1 - day;
  const mon = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + diff));
  return mon.toISOString().slice(0, 10);
}

// ── API ───────────────────────────────────────────────────────────────────────
const api = {
  get: (path) => fetch(path).then((r) => r.json()),
  post: (path, body) =>
    fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }).then((r) => r.json()),
};

// ── App root ──────────────────────────────────────────────────────────────────
function MissionControl() {
  const [view, setView] = useState("trades");
  const [trades, setTrades] = useState([]);
  const [paperTrades, setPaperTrades] = useState([]);
  const [bankroll, setBankroll] = useState(null);
  const [config, setConfig] = useState(null);
  const [cities, setCities] = useState(null);
  const [commits, setCommits] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.get("/api/trades").then((d) => setTrades(d.trades || [])),
      api.get("/api/paper-trades").then((d) => setPaperTrades(d.trades || [])),
      api.get("/api/bankroll").then((d) => setBankroll(d)),
      api.get("/api/config").then((d) => setConfig(d)),
      api.get("/api/cities").then((d) => setCities(d)),
      api.get("/api/git-commits").then((d) => setCommits(d.commits || [])),
    ])
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const toggleCity = async (key) => {
    const res = await api.post(`/api/cities/${key}/toggle`, {});
    setCities((prev) => ({ ...prev, [key]: { ...prev[key], enabled: res.enabled } }));
  };

  const saveConfig = async (updates) => {
    const res = await api.post("/api/config", updates);
    setConfig((prev) => ({ ...prev, ...updates }));
    return res;
  };

  if (loading) {
    return (
      <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh", background: "#f8fafc" }}>
        <div style={{ color: "#64748b", fontSize: 15 }}>Loading...</div>
      </div>
    );
  }

  const resolved = trades.filter((t) => t.resolved);
  const wins = resolved.filter((t) => t.result === "win");
  const totalPnl = resolved.reduce((s, t) => s + (t.pnl || 0), 0);
  const winRate = resolved.length ? wins.length / resolved.length : null;
  const current = bankroll?.current ?? config?.INITIAL_BANKROLL ?? 0;
  const isLive = config?.LIVE_TRADING;

  return (
    <div style={S.page}>
      {/* Header */}
      <header style={S.header}>
        <div style={S.headerLeft}>
          <div style={S.logoMark}>W</div>
          <div>
            <div style={S.appName}>Kalshi Weather Arb</div>
            <div style={S.appSub}>Mission Control</div>
          </div>
        </div>

        <div style={S.headerStats}>
          <HeaderStat label="Bankroll" value={usd(current)} color={C.blue} />
          <div style={S.statDivider} />
          <HeaderStat label="All-time P&L" value={usd(totalPnl, true)} color={totalPnl >= 0 ? C.green : C.red} />
          <div style={S.statDivider} />
          <HeaderStat label="Win rate" value={winRate !== null ? pct(winRate) : "—"} color={winRate !== null && winRate >= 0.5 ? C.green : C.amber} />
          <div style={S.statDivider} />
          <HeaderStat label="Total trades" value={trades.length} color={C.text} />
        </div>

        {isLive && (
          <div style={S.headerRight}>
            <span style={{ ...S.badge, background: "#fef2f2", color: C.red, border: `1px solid ${C.red}33` }}>
              Live
            </span>
          </div>
        )}
      </header>

      {/* Tabs */}
      <div style={S.tabBar}>
        {[["trades", "Trades"], ["bankroll", "Bankroll"], ["config", "Settings"]].map(([key, label]) => (
          <button key={key} onClick={() => setView(key)} style={{ ...S.tab, ...(view === key ? S.tabActive : {}) }}>
            {label}
          </button>
        ))}
      </div>

      {/* Content */}
      <main style={S.main}>
        {view === "trades" && <TradesView trades={trades} />}
        {view === "bankroll" && <BankrollView bankroll={bankroll} trades={trades} config={config} commits={commits} />}
        {view === "paper-trades" && <PaperTradesView trades={paperTrades} />}
        {view === "config" && <ConfigView config={config} cities={cities} saveConfig={saveConfig} toggleCity={toggleCity} />}
      </main>
    </div>
  );
}

function HeaderStat({ label, value, color }) {
  return (
    <div style={{ textAlign: "center" }}>
      <div style={{ fontSize: 11, color: C.muted, marginBottom: 2, textTransform: "uppercase", letterSpacing: "0.05em" }}>{label}</div>
      <div style={{ fontSize: 18, fontWeight: 700, color }}>{value}</div>
    </div>
  );
}

// ── Trades view ───────────────────────────────────────────────────────────────
function TradesView({ trades }) {
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return trades;
    return trades.filter((t) => {
      const label = tradeLabel(t).toLowerCase();
      return (
        label.includes(q) ||
        (t.ticker || "").toLowerCase().includes(q) ||
        (t.city || "").toLowerCase().includes(q) ||
        (t.result || "").includes(q) ||
        (t.side || "").includes(q)
      );
    });
  }, [trades, search]);

  const fResolved = filtered.filter((t) => t.resolved);
  const fWins = fResolved.filter((t) => t.result === "win");
  const fPnl = fResolved.reduce((s, t) => s + (t.pnl || 0), 0);

  return (
    <div>
      {/* Search + summary */}
      <div style={S.searchRow}>
        <div style={S.searchWrap}>
          <svg style={S.searchIcon} viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5">
            <circle cx="8.5" cy="8.5" r="5.5" /><path d="M15 15l-3-3" strokeLinecap="round" />
          </svg>
          <input
            style={S.searchInput}
            placeholder="Search by city, market, result..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          {search && (
            <button style={S.clearBtn} onClick={() => setSearch("")}>✕</button>
          )}
        </div>
        <div style={S.searchSummary}>
          <span style={{ color: C.muted }}>{filtered.length} of {trades.length} trades</span>
          {fResolved.length > 0 && (
            <>
              <span style={{ color: C.border, margin: "0 8px" }}>|</span>
              <span style={{ color: C.green, fontWeight: 600 }}>{fWins.length}W</span>
              <span style={{ color: C.muted, margin: "0 4px" }}>/</span>
              <span style={{ color: C.red, fontWeight: 600 }}>{fResolved.length - fWins.length}L</span>
              <span style={{ color: C.border, margin: "0 8px" }}>|</span>
              <span style={{ color: fPnl >= 0 ? C.green : C.red, fontWeight: 600 }}>{usd(fPnl, true)}</span>
            </>
          )}
        </div>
      </div>

      {/* Table */}
      <div style={S.card}>
        <table style={S.table}>
          <thead>
            <tr style={S.thead}>
              <th style={S.th}>Market</th>
              <th style={S.th}>Side</th>
              <th style={S.th}>Edge</th>
              <th style={S.th}>Model prob</th>
              <th style={S.th}>Outcome</th>
              <th style={S.th}>P&L</th>
              <th style={S.th}>Date</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td colSpan={7} style={{ padding: "48px 24px", textAlign: "center", color: C.muted, fontSize: 14 }}>
                  {trades.length === 0 ? "No trades placed yet." : "No trades match your search."}
                </td>
              </tr>
            ) : (
              filtered.map((t) => <TradeRow key={`${t.is_paper ? "p" : "l"}_${t.id}`} trade={t} />)
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TradeRow({ trade: t }) {
  const result = t.result;
  const pnlColor = (t.pnl || 0) > 0 ? C.green : (t.pnl || 0) < 0 ? C.red : C.muted;

  let outcomeChip;
  if (result === "win") {
    outcomeChip = <span style={{ ...S.chip, background: "#f0fdf4", color: C.green, border: `1px solid ${C.green}33` }}>Win</span>;
  } else if (result === "loss") {
    outcomeChip = <span style={{ ...S.chip, background: "#fef2f2", color: C.red, border: `1px solid ${C.red}33` }}>Loss</span>;
  } else {
    outcomeChip = <span style={{ ...S.chip, background: "#fffbeb", color: C.amber, border: `1px solid ${C.amber}33` }}>Pending</span>;
  }

  return (
    <tr style={S.tr}>
      <td style={S.td}>
        <div style={{ fontWeight: 500, color: C.text, fontSize: 13 }}>{tradeLabel(t)}</div>
        <div style={{ fontSize: 11, color: C.subtle, marginTop: 1 }}>{t.ticker}</div>
      </td>
      <td style={S.td}>
        <span style={{
          fontWeight: 600, fontSize: 12,
          color: t.side === "yes" ? C.green : C.amber,
        }}>
          {t.side?.toUpperCase() || "—"}
        </span>
      </td>
      <td style={{ ...S.td, color: (t.edge || 0) >= 0.1 ? C.green : C.text }}>
        {pct(t.edge)}
      </td>
      <td style={{ ...S.td, color: C.muted }}>{pct(t.model_prob)}</td>
      <td style={S.td}>{outcomeChip}</td>
      <td style={{ ...S.td, fontWeight: t.pnl != null ? 600 : 400, color: pnlColor }}>
        {t.pnl != null ? usd(t.pnl, true) : "—"}
      </td>
      <td style={{ ...S.td, color: C.muted, fontSize: 12 }}>{t.resolution_date || "—"}</td>
    </tr>
  );
}

// ── Paper Trades view ─────────────────────────────────────────────────────────
function PaperTradesView({ trades }) {
  const resolved = trades.filter((t) => t.resolved);
  const wins = resolved.filter((t) => t.result === "win");
  const totalPnl = resolved.reduce((s, t) => s + (t.pnl || 0), 0);

  return (
    <div>
      <div style={{ marginBottom: 16, display: "flex", alignItems: "center", gap: 24 }}>
        <div style={{ fontSize: 13, color: C.muted }}>{trades.length} paper trades — {resolved.length} settled</div>
        {resolved.length > 0 && (
          <>
            <span style={{ color: C.green, fontWeight: 600 }}>{wins.length}W</span>
            <span style={{ color: C.muted }}>/ {resolved.length - wins.length}L</span>
            <span style={{ color: totalPnl >= 0 ? C.green : C.red, fontWeight: 600 }}>{usd(totalPnl, true)}</span>
          </>
        )}
      </div>
      <div style={S.card}>
        <table style={S.table}>
          <thead>
            <tr style={S.thead}>
              <th style={S.th}>Market</th>
              <th style={S.th}>Side</th>
              <th style={S.th}>Edge</th>
              <th style={S.th}>Model prob</th>
              <th style={S.th}>Outcome</th>
              <th style={S.th}>P&L</th>
              <th style={S.th}>Date</th>
            </tr>
          </thead>
          <tbody>
            {trades.length === 0 ? (
              <tr>
                <td colSpan={7} style={{ padding: "48px 24px", textAlign: "center", color: C.muted, fontSize: 14 }}>
                  No paper trades found.
                </td>
              </tr>
            ) : (
              trades.map((t) => <TradeRow key={`p_${t.id}`} trade={t} />)
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Bankroll view ─────────────────────────────────────────────────────────────
function BankrollView({ bankroll, trades, config, commits }) {
  const resolved = trades.filter((t) => t.resolved);
  const wins = resolved.filter((t) => t.result === "win");
  const losses = resolved.filter((t) => t.result === "loss");
  const totalPnl = resolved.reduce((s, t) => s + (t.pnl || 0), 0);
  const winRate = resolved.length ? wins.length / resolved.length : null;
  const avgEdge = trades.length ? trades.reduce((s, t) => s + (t.edge || 0), 0) / trades.length : 0;
  const initial = bankroll?.initial ?? config?.INITIAL_BANKROLL ?? 1000;
  const current = bankroll?.current ?? initial;

  const stats = [
    { label: "Current bankroll", value: usd(current), color: C.blue, big: true },
    { label: "All-time P&L", value: usd(totalPnl, true), color: totalPnl >= 0 ? C.green : C.red, big: true },
    { label: "ROI", value: pct(totalPnl / initial), color: totalPnl >= 0 ? C.green : C.red },
    { label: "Win rate", value: winRate !== null ? pct(winRate) : "—", color: winRate !== null && winRate >= 0.5 ? C.green : C.amber },
    { label: "Wins", value: wins.length, color: C.green },
    { label: "Losses", value: losses.length, color: C.red },
    { label: "Total settled", value: resolved.length, color: C.text },
    { label: "Avg edge", value: pct(avgEdge), color: C.blue },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      {/* Stat grid */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 12 }}>
        {stats.map((s, i) => (
          <div key={i} style={S.statCard}>
            <div style={S.statLabel}>{s.label}</div>
            <div style={{ ...S.statValue, color: s.color, fontSize: s.big ? 24 : 20 }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Chart */}
      <div style={S.card}>
        <div style={S.cardTitle}>Bankroll over time</div>
        <BankrollChart points={bankroll?.points || []} initial={initial} />
      </div>

      {/* Weekly P&L */}
      <WeeklyPnlSection trades={trades} commits={commits} />

      {/* Recent settlements */}
      {resolved.length > 0 && (
        <div style={S.card}>
          <div style={S.cardTitle}>Recent settlements</div>
          <table style={S.table}>
            <thead>
              <tr style={S.thead}>
                <th style={S.th}>Date settled</th>
                <th style={S.th}>Market</th>
                <th style={S.th}>Side</th>
                <th style={S.th}>Edge</th>
                <th style={S.th}>P&L</th>
                <th style={S.th}>Result</th>
              </tr>
            </thead>
            <tbody>
              {[...resolved].reverse().slice(0, 30).map((t) => {
                const rc = t.result === "win" ? C.green : C.red;
                return (
                  <tr key={`${t.is_paper ? "p" : "l"}_${t.id}`} style={S.tr}>
                    <td style={{ ...S.td, color: C.muted, fontSize: 12 }}>{fmtDate(t.resolved_at)}</td>
                    <td style={S.td}>{tradeLabel(t)}</td>
                    <td style={{ ...S.td, color: t.side === "yes" ? C.green : C.amber, fontWeight: 600 }}>{t.side?.toUpperCase()}</td>
                    <td style={{ ...S.td, color: (t.edge || 0) >= 0.1 ? C.green : C.text }}>{pct(t.edge)}</td>
                    <td style={{ ...S.td, color: (t.pnl || 0) >= 0 ? C.green : C.red, fontWeight: 600 }}>{t.pnl != null ? usd(t.pnl, true) : "—"}</td>
                    <td style={S.td}>
                      <span style={{ color: rc, fontWeight: 600, fontSize: 12 }}>{t.result?.toUpperCase()}</span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Weekly P&L section ────────────────────────────────────────────────────────
function WeeklyPnlSection({ trades, commits }) {
  const [selectedWeek, setSelectedWeek] = useState(null);

  const weeks = useMemo(() => {
    const resolved = trades.filter((t) => t.resolved && t.pnl != null && t.resolved_at);
    const byWeek = {};
    for (const t of resolved) {
      const wk = getWeekStart(t.resolved_at);
      if (!byWeek[wk]) byWeek[wk] = { week: wk, trades: [], pnl: 0, wins: 0, losses: 0 };
      byWeek[wk].trades.push(t);
      byWeek[wk].pnl += t.pnl;
      if (t.result === "win") byWeek[wk].wins++;
      else if (t.result === "loss") byWeek[wk].losses++;
    }
    return Object.values(byWeek).sort((a, b) => a.week.localeCompare(b.week));
  }, [trades]);

  const commitsByWeek = useMemo(() => {
    const map = {};
    for (const c of commits) {
      const wk = getWeekStart(c.date + "T12:00:00Z");
      if (!map[wk]) map[wk] = [];
      map[wk].push(c);
    }
    return map;
  }, [commits]);

  const selectedWeekData = weeks.find((w) => w.week === selectedWeek) || null;
  const selectedCommits = selectedWeek ? (commitsByWeek[selectedWeek] || []) : [];

  return (
    <div style={S.card}>
      <div style={S.cardTitle}>Weekly P&L</div>
      <WeeklyBarChart
        weeks={weeks}
        commitsByWeek={commitsByWeek}
        selectedWeek={selectedWeek}
        onSelectWeek={(wk) => setSelectedWeek(wk === selectedWeek ? null : wk)}
      />
      {selectedWeekData && (
        <WeekDetail
          weekData={selectedWeekData}
          commits={selectedCommits}
          onClose={() => setSelectedWeek(null)}
        />
      )}
    </div>
  );
}

function WeeklyBarChart({ weeks, commitsByWeek, selectedWeek, onSelectWeek }) {
  const [tooltip, setTooltip] = useState(null);
  const containerRef = useRef(null);

  if (!weeks.length) {
    return <div style={{ color: C.muted, fontSize: 14, padding: "32px 0", textAlign: "center" }}>No settled trades yet.</div>;
  }

  const W = 900, H = 260, PL = 52, PR = 24, PT = 40, PB = 36;
  const innerW = W - PL - PR;
  const innerH = H - PT - PB;
  const zeroY = PT + innerH / 2;
  const maxAbs = Math.max(...weeks.map((w) => Math.abs(w.pnl)), 0.01);
  const n = weeks.length;
  const slotW = innerW / n;
  const barW = Math.max(8, Math.min(44, slotW - 6));
  const cx = (i) => PL + (i + 0.5) * slotW;
  const toBarH = (pnl) => (Math.abs(pnl) / maxAbs) * (innerH / 2);

  const handleMouseMove = (e) => {
    if (!containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    const relX = ((e.clientX - rect.left) / rect.width) * W;
    const i = Math.floor((relX - PL) / slotW);
    if (i >= 0 && i < n) {
      setTooltip({ i, px: e.clientX - rect.left, py: e.clientY - rect.top });
    } else {
      setTooltip(null);
    }
  };

  const hoveredWeek = tooltip != null ? weeks[tooltip.i] : null;

  return (
    <div ref={containerRef} style={{ position: "relative" }}
      onMouseMove={handleMouseMove} onMouseLeave={() => setTooltip(null)}>
      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: 260, display: "block", cursor: "pointer" }}>

        {/* Commit track separator */}
        <line x1={PL} y1={PT - 4} x2={W - PR} y2={PT - 4} stroke={C.border} strokeWidth="0.5" strokeDasharray="3 3" />
        <text x={PL - 6} y="22" fill={C.subtle} fontSize="9" textAnchor="end" fontFamily={font}>deploys</text>

        {/* Zero line */}
        <line x1={PL} y1={zeroY} x2={W - PR} y2={zeroY} stroke={C.border} strokeWidth="1" />

        {/* Y-axis labels */}
        <text x={PL - 6} y={PT + 4} fill={C.subtle} fontSize="10" textAnchor="end" fontFamily={font}>{`+$${maxAbs.toFixed(0)}`}</text>
        <text x={PL - 6} y={zeroY + 4} fill={C.subtle} fontSize="10" textAnchor="end" fontFamily={font}>$0</text>
        <text x={PL - 6} y={PT + innerH + 4} fill={C.subtle} fontSize="10" textAnchor="end" fontFamily={font}>{`-$${maxAbs.toFixed(0)}`}</text>

        {weeks.map((week, i) => {
          const barH = Math.max(2, toBarH(week.pnl));
          const bx = cx(i) - barW / 2;
          const by = week.pnl >= 0 ? zeroY - barH : zeroY;
          const color = week.pnl >= 0 ? C.green : C.red;
          const isSelected = week.week === selectedWeek;
          const isHovered = hoveredWeek?.week === week.week;
          const wkCommits = commitsByWeek[week.week] || [];

          return (
            <g key={week.week} onClick={() => onSelectWeek(week.week)}>
              {/* Bar */}
              <rect x={bx.toFixed(1)} y={by.toFixed(1)} width={barW} height={barH.toFixed(1)}
                fill={color} opacity={isSelected ? 1 : isHovered ? 0.85 : 0.6} rx="3" />
              {/* Selected ring */}
              {isSelected && (
                <rect x={(bx - 1.5).toFixed(1)} y={(by - 1.5).toFixed(1)}
                  width={barW + 3} height={barH + 3}
                  fill="none" stroke={color} strokeWidth="1.5" rx="4" />
              )}
              {/* Commit dot */}
              {wkCommits.length > 0 && (
                <g>
                  <circle cx={cx(i).toFixed(1)} cy="20" r="7" fill={C.blue} opacity="0.9" />
                  <text x={cx(i).toFixed(1)} y="24" fill="white" fontSize={wkCommits.length > 9 ? "6" : "8"}
                    textAnchor="middle" fontFamily={font} fontWeight="700">
                    {wkCommits.length}
                  </text>
                </g>
              )}
              {/* Week label */}
              {(n <= 14 || i % 2 === 0) && (
                <text x={cx(i).toFixed(1)} y={H - 6} fill={isSelected ? C.text : C.subtle}
                  fontSize="10" textAnchor="middle" fontFamily={font} fontWeight={isSelected ? "600" : "400"}>
                  {fmtShortDate(week.week)}
                </text>
              )}
            </g>
          );
        })}
      </svg>

      {/* Hover tooltip */}
      {tooltip && hoveredWeek && (
        <div style={{
          position: "absolute",
          left: Math.min(tooltip.px + 14, (containerRef.current?.offsetWidth || 999) - 170),
          top: Math.max(8, tooltip.py - 88),
          background: C.surface,
          border: `1px solid ${C.border}`,
          borderRadius: 10,
          padding: "12px 16px",
          fontSize: 13,
          pointerEvents: "none",
          boxShadow: "0 4px 20px rgba(0,0,0,0.1)",
          zIndex: 10,
          minWidth: 155,
        }}>
          <div style={{ fontWeight: 600, marginBottom: 6, color: C.text }}>
            Week of {fmtShortDate(hoveredWeek.week)}
          </div>
          <div style={{ fontSize: 18, fontWeight: 700, color: hoveredWeek.pnl >= 0 ? C.green : C.red, marginBottom: 6 }}>
            {usd(hoveredWeek.pnl, true)}
          </div>
          <div style={{ color: C.muted, marginBottom: 4 }}>
            <span style={{ color: C.green, fontWeight: 600 }}>{hoveredWeek.wins}W</span>
            {" / "}
            <span style={{ color: C.red, fontWeight: 600 }}>{hoveredWeek.losses}L</span>
            {" · "}{hoveredWeek.trades.length} trades
          </div>
          {(commitsByWeek[hoveredWeek.week] || []).length > 0 && (
            <div style={{ color: C.blue, fontSize: 12 }}>
              {(commitsByWeek[hoveredWeek.week] || []).length} deploy{(commitsByWeek[hoveredWeek.week] || []).length > 1 ? "s" : ""} this week
            </div>
          )}
          <div style={{ color: C.subtle, fontSize: 11, marginTop: 6 }}>Click to see details</div>
        </div>
      )}
    </div>
  );
}

function WeekDetail({ weekData, commits, onClose }) {
  const weekEnd = new Date(weekData.week + "T00:00:00Z");
  weekEnd.setUTCDate(weekEnd.getUTCDate() + 6);
  const weekEndStr = weekEnd.toLocaleDateString("en-US", { month: "short", day: "numeric" });

  return (
    <div style={{ marginTop: 24, borderTop: `2px solid ${C.border}`, paddingTop: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 18 }}>
        <div style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
          <span style={{ fontWeight: 700, fontSize: 16 }}>
            {fmtShortDate(weekData.week)} – {weekEndStr}
          </span>
          <span style={{ fontWeight: 700, fontSize: 18, color: weekData.pnl >= 0 ? C.green : C.red }}>
            {usd(weekData.pnl, true)}
          </span>
          <span style={{ color: C.muted, fontSize: 13 }}>
            {weekData.wins}W / {weekData.losses}L
          </span>
        </div>
        <button onClick={onClose} style={{
          background: "none", border: `1px solid ${C.border}`, borderRadius: 6,
          padding: "5px 14px", cursor: "pointer", color: C.muted, fontSize: 13, fontFamily: font,
        }}>
          Close ✕
        </button>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: commits.length ? "3fr 2fr" : "1fr", gap: 24, alignItems: "start" }}>
        {/* Trades table */}
        <div>
          <div style={{ fontSize: 11, fontWeight: 600, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 10 }}>
            Trades this week
          </div>
          <table style={S.table}>
            <thead>
              <tr style={S.thead}>
                <th style={S.th}>Market</th>
                <th style={S.th}>Side</th>
                <th style={S.th}>Edge</th>
                <th style={S.th}>P&L</th>
                <th style={S.th}>Result</th>
              </tr>
            </thead>
            <tbody>
              {weekData.trades.map((t) => (
                <tr key={`${t.is_paper ? "p" : "l"}_${t.id}`} style={S.tr}>
                  <td style={S.td}>{tradeLabel(t)}</td>
                  <td style={{ ...S.td, color: t.side === "yes" ? C.green : C.amber, fontWeight: 600 }}>{t.side?.toUpperCase()}</td>
                  <td style={{ ...S.td, color: (t.edge || 0) >= 0.1 ? C.green : C.text }}>{pct(t.edge)}</td>
                  <td style={{ ...S.td, color: (t.pnl || 0) >= 0 ? C.green : C.red, fontWeight: 600 }}>
                    {t.pnl != null ? usd(t.pnl, true) : "—"}
                  </td>
                  <td style={{ ...S.td, color: t.result === "win" ? C.green : C.red, fontWeight: 600, fontSize: 12 }}>
                    {t.result?.toUpperCase()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Commits */}
        {commits.length > 0 && (
          <div>
            <div style={{ fontSize: 11, fontWeight: 600, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 10 }}>
              Deploys this week
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {commits.map((c) => (
                <div key={c.hash} style={{
                  padding: "10px 14px",
                  background: "#f8fafc",
                  border: `1px solid ${C.border}`,
                  borderLeft: `3px solid ${C.blue}`,
                  borderRadius: "0 8px 8px 0",
                }}>
                  <div style={{ fontWeight: 500, fontSize: 13, color: C.text, lineHeight: 1.4 }}>{c.message}</div>
                  <div style={{ fontSize: 11, color: C.subtle, marginTop: 4 }}>
                    <code style={{ background: "#e2e8f0", padding: "1px 5px", borderRadius: 3, fontSize: 10 }}>{c.hash}</code>
                    {" · "}{fmtDate(c.date + "T12:00:00")}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function BankrollChart({ points, initial }) {
  if (!points || points.length < 2) {
    return (
      <div style={{ padding: "48px 0", textAlign: "center", color: C.muted, fontSize: 14 }}>
        No settled trades yet — chart will appear here once trades resolve.
      </div>
    );
  }

  const W = 900, H = 240, PL = 72, PR = 90, PT = 20, PB = 36;
  const innerW = W - PL - PR;
  const innerH = H - PT - PB;
  const n = points.length;

  const values = points.map((p) => p.bankroll);
  const rawMin = Math.min(...values);
  const rawMax = Math.max(...values);
  const pad = (rawMax - rawMin) * 0.12 || 20;
  const minVal = rawMin - pad;
  const maxVal = rawMax + pad;
  const valRange = maxVal - minVal;

  const toX = (i) => PL + (i / (n - 1)) * innerW;
  const toY = (v) => PT + (1 - (v - minVal) / valRange) * innerH;

  const pts = points.map((p, i) => ({ x: toX(i), y: toY(p.bankroll) }));
  const pathD = pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(" ");
  const fillD = `${pathD} L${pts[n - 1].x.toFixed(1)},${(PT + innerH).toFixed(1)} L${pts[0].x.toFixed(1)},${(PT + innerH).toFixed(1)} Z`;

  const current = points[n - 1].bankroll;
  const lineColor = current >= initial ? C.green : C.red;
  const fillColor = current >= initial ? "#16a34a" : "#dc2626";

  // Y-axis labels
  const yGrids = [0, 0.25, 0.5, 0.75, 1].map((frac) => ({
    y: toY(minVal + valRange * frac),
    label: "$" + (minVal + valRange * frac).toFixed(0),
  }));

  // Date range labels
  const datePts = points.filter((p) => p.t);
  const firstDate = datePts.length ? datePts[0].t.slice(0, 10) : "";
  const lastDate = datePts.length ? datePts[datePts.length - 1].t.slice(0, 10) : "";
  const currentY = toY(current);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: 240, display: "block" }}>
      <defs>
        <linearGradient id="chartFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={fillColor} stopOpacity="0.15" />
          <stop offset="100%" stopColor={fillColor} stopOpacity="0.02" />
        </linearGradient>
      </defs>

      {/* Grid lines */}
      {yGrids.map((g, i) => (
        <g key={i}>
          <line x1={PL} y1={g.y.toFixed(1)} x2={W - PR} y2={g.y.toFixed(1)} stroke="#e2e8f0" strokeWidth="1" />
          <text x={PL - 8} y={+g.y.toFixed(1) + 4} fill={C.subtle} fontSize="10" textAnchor="end" fontFamily={font}>{g.label}</text>
        </g>
      ))}

      {/* Zero line (initial bankroll) */}
      {initial >= minVal && initial <= maxVal && (
        <line x1={PL} y1={toY(initial).toFixed(1)} x2={W - PR} y2={toY(initial).toFixed(1)}
          stroke={C.border} strokeWidth="1" strokeDasharray="4 3" />
      )}

      {/* Fill + line */}
      <path d={fillD} fill="url(#chartFill)" />
      <path d={pathD} fill="none" stroke={lineColor} strokeWidth="2" strokeLinejoin="round" />

      {/* Dots (when sparse) */}
      {n <= 60 && pts.slice(1).map((p, i) => (
        <circle key={i} cx={p.x.toFixed(1)} cy={p.y.toFixed(1)} r="3" fill={lineColor} opacity="0.6" />
      ))}

      {/* Axes */}
      <line x1={PL} y1={PT} x2={PL} y2={PT + innerH} stroke="#e2e8f0" strokeWidth="1" />
      <line x1={PL} y1={PT + innerH} x2={W - PR} y2={PT + innerH} stroke="#e2e8f0" strokeWidth="1" />

      {/* Current value callout */}
      <line x1={(W - PR).toFixed(1)} y1={currentY.toFixed(1)} x2={(W - PR + 8).toFixed(1)} y2={currentY.toFixed(1)}
        stroke={lineColor} strokeWidth="1.5" />
      <text x={W - PR + 11} y={+currentY.toFixed(1) + 4} fill={lineColor} fontSize="11" fontFamily={font} fontWeight="700">
        {usd(current)}
      </text>

      {/* Date labels */}
      {firstDate && <text x={PL} y={H - 6} fill={C.subtle} fontSize="10" fontFamily={font}>{firstDate}</text>}
      {lastDate && <text x={W - PR} y={H - 6} fill={C.subtle} fontSize="10" textAnchor="end" fontFamily={font}>{lastDate}</text>}
    </svg>
  );
}

// ── Config / Switchboard ──────────────────────────────────────────────────────
function ConfigView({ config, cities, saveConfig, toggleCity }) {
  const [local, setLocal] = useState(config || {});
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [savedMsg, setSavedMsg] = useState("");

  const set = (key, val) => { setLocal((p) => ({ ...p, [key]: val })); setDirty(true); setSavedMsg(""); };

  const handleSave = async () => {
    setSaving(true);
    try {
      await saveConfig(local);
      setDirty(false);
      setSavedMsg("Saved — restart bot to apply changes");
      setTimeout(() => setSavedMsg(""), 6000);
    } catch (e) { alert("Save failed: " + e.message); }
    finally { setSaving(false); }
  };

  const ENV_FIELDS = [
    { key: "LIVE_TRADING",             label: "Live trading mode",         type: "bool",   danger: true, help: "Enables real-money orders on Kalshi" },
    { key: "INITIAL_BANKROLL",         label: "Initial bankroll ($)",       type: "number", step: 100, min: 0 },
    { key: "KELLY_FRACTION",           label: "Kelly fraction",             type: "number", step: 0.01, min: 0.01, max: 1 },
    { key: "MIN_EDGE_THRESHOLD",       label: "Min edge threshold",         type: "number", step: 0.01, min: 0, max: 0.5 },
    { key: "KALSHI_FEE_RATE",          label: "Kalshi fee rate",            type: "number", step: 0.01, min: 0, max: 0.2 },
    { key: "WEATHER_MAX_TRADE_SIZE",   label: "Max trade size ($)",         type: "number", step: 10, min: 0 },
    { key: "LIVE_MAX_TRADE_SIZE",      label: "Live max trade size ($)",    type: "number", step: 1, min: 0 },
    { key: "SCAN_INTERVAL_SECONDS",    label: "Scan interval (seconds)",    type: "number", step: 10, min: 10 },
    { key: "TRADING_HOURS_START",      label: "Trading start hour (ET)",    type: "number", step: 1, min: 0, max: 23 },
    { key: "TRADING_HOURS_END",        label: "Trading end hour (ET)",      type: "number", step: 1, min: 0, max: 23 },
    { key: "MIN_ASK_SIZE",             label: "Min ask size",               type: "number", step: 5, min: 0 },
    { key: "MIN_VOLUME_24H",           label: "Min 24h volume",             type: "number", step: 50, min: 0 },
    { key: "WEATHER_MIN_ENTRY_PRICE",  label: "Min entry price",            type: "number", step: 0.01, min: 0, max: 1 },
    { key: "WEATHER_MAX_ENTRY_PRICE",  label: "Max entry price",            type: "number", step: 0.01, min: 0, max: 1 },
    { key: "CITY_OVERRIDE",            label: "City override (blank = all)",type: "text",   placeholder: "nyc" },
  ];

  const enabledCount = cities ? Object.values(cities).filter((c) => c.enabled).length : 0;
  const totalCount = cities ? Object.keys(cities).length : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>

      {/* Environment variables */}
      <div style={S.card}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <div style={S.cardTitle}>Bot settings</div>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            {savedMsg && <span style={{ fontSize: 13, color: C.green }}>{savedMsg}</span>}
            {dirty && (
              <button style={{ ...S.btn, opacity: saving ? 0.6 : 1 }} onClick={handleSave} disabled={saving}>
                {saving ? "Saving..." : "Save changes"}
              </button>
            )}
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0 48px" }}>
          {ENV_FIELDS.map((f) => (
            <div key={f.key} style={S.envRow}>
              <div>
                <div style={S.envLabel}>{f.label}</div>
                {f.help && <div style={S.envHelp}>{f.help}</div>}
              </div>

              {f.type === "bool" ? (
                <button
                  style={{
                    ...S.toggle,
                    background: local[f.key] ? (f.danger ? C.red : C.green) : "#e2e8f0",
                    color: local[f.key] ? "#fff" : C.muted,
                  }}
                  onClick={() => {
                    if (f.danger && !local[f.key]) {
                      if (!confirm("⚠ This enables REAL MONEY trading on Kalshi.\nAre you absolutely sure?")) return;
                    }
                    set(f.key, !local[f.key]);
                  }}
                >
                  {local[f.key] ? "On" : "Off"}
                </button>
              ) : f.type === "text" ? (
                <input style={S.envInput} value={local[f.key] || ""} placeholder={f.placeholder || ""} onChange={(e) => set(f.key, e.target.value)} />
              ) : (
                <input type="number" step={f.step} min={f.min} max={f.max} style={S.envInput}
                  value={local[f.key] ?? ""} onChange={(e) => set(f.key, parseFloat(e.target.value) || 0)} />
              )}
            </div>
          ))}
        </div>

        <div style={{ marginTop: 16, paddingTop: 14, borderTop: `1px solid ${C.border}`, fontSize: 12, color: C.subtle }}>
          Changes are saved to .env — restart the bot process for them to take effect.
          City toggles below apply immediately on the next scan.
        </div>
      </div>

      {/* City toggles */}
      <div style={S.card}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <div style={S.cardTitle}>Active cities</div>
          <span style={{ fontSize: 13, color: C.muted }}>{enabledCount} of {totalCount} enabled</span>
        </div>

        {cities ? (
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: 8 }}>
            {Object.entries(cities).map(([key, city]) => {
              const on = city.enabled;
              return (
                <button key={key} onClick={() => toggleCity(key)} style={{
                  textAlign: "left",
                  padding: "10px 14px",
                  border: `1px solid ${on ? C.green + "44" : C.border}`,
                  borderRadius: 8,
                  background: on ? "#f0fdf4" : "#fafafa",
                  cursor: "pointer",
                  transition: "all 0.15s",
                  fontFamily: font,
                }}>
                  <div style={{ fontWeight: 600, fontSize: 13, color: on ? C.text : C.muted }}>{city.name}</div>
                  <div style={{ fontSize: 11, marginTop: 2, color: on ? C.green : C.subtle }}>
                    {on ? "Active" : "Inactive"}
                  </div>
                </button>
              );
            })}
          </div>
        ) : (
          <div style={{ color: C.muted }}>Loading...</div>
        )}
      </div>
    </div>
  );
}

// ── Design tokens ─────────────────────────────────────────────────────────────
const font = "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, sans-serif";

const C = {
  bg: "#f1f5f9",
  surface: "#ffffff",
  border: "#e2e8f0",
  text: "#0f172a",
  muted: "#64748b",
  subtle: "#94a3b8",
  green: "#16a34a",
  red: "#dc2626",
  amber: "#d97706",
  blue: "#2563eb",
};

const S = {
  page: { fontFamily: font, background: C.bg, minHeight: "100vh", color: C.text, fontSize: 14 },

  // Header
  header: {
    background: C.surface,
    borderBottom: `1px solid ${C.border}`,
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "14px 28px",
    gap: 24,
  },
  headerLeft: { display: "flex", alignItems: "center", gap: 12, flexShrink: 0 },
  logoMark: {
    width: 36, height: 36, borderRadius: 8,
    background: C.blue, color: "#fff",
    display: "flex", alignItems: "center", justifyContent: "center",
    fontWeight: 800, fontSize: 18,
  },
  appName: { fontWeight: 700, fontSize: 16, color: C.text },
  appSub: { fontSize: 11, color: C.muted, marginTop: 1 },
  headerStats: { display: "flex", alignItems: "center", gap: 24, flex: 1, justifyContent: "center" },
  statDivider: { width: 1, height: 32, background: C.border },
  headerRight: { flexShrink: 0 },

  // Tabs
  tabBar: {
    background: C.surface,
    borderBottom: `1px solid ${C.border}`,
    display: "flex",
    padding: "0 28px",
    gap: 0,
  },
  tab: {
    fontFamily: font,
    fontSize: 14,
    padding: "12px 20px",
    background: "none",
    border: "none",
    borderBottom: "2px solid transparent",
    color: C.muted,
    cursor: "pointer",
    fontWeight: 500,
    transition: "all 0.15s",
  },
  tabActive: { color: C.blue, borderBottomColor: C.blue },

  // Main
  main: { padding: "24px 28px", maxWidth: 1200, margin: "0 auto" },

  // Cards
  card: { background: C.surface, borderRadius: 10, border: `1px solid ${C.border}`, padding: 20, marginBottom: 0 },
  cardTitle: { fontWeight: 600, fontSize: 15, marginBottom: 16, color: C.text },

  // Stats
  statCard: { background: C.surface, borderRadius: 10, border: `1px solid ${C.border}`, padding: "14px 18px" },
  statLabel: { fontSize: 11, color: C.muted, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.05em" },
  statValue: { fontWeight: 700, fontSize: 20 },

  // Search
  searchRow: { display: "flex", alignItems: "center", gap: 16, marginBottom: 12 },
  searchWrap: { position: "relative", display: "flex", alignItems: "center" },
  searchIcon: { position: "absolute", left: 10, width: 16, height: 16, color: C.subtle, pointerEvents: "none" },
  searchInput: {
    fontFamily: font,
    fontSize: 13,
    background: C.surface,
    border: `1px solid ${C.border}`,
    borderRadius: 8,
    color: C.text,
    padding: "9px 36px 9px 34px",
    width: 320,
    outline: "none",
  },
  clearBtn: {
    position: "absolute", right: 10,
    background: "none", border: "none", color: C.subtle, cursor: "pointer",
    fontSize: 12, padding: "2px 4px",
  },
  searchSummary: { display: "flex", alignItems: "center", fontSize: 13 },

  // Table
  table: { width: "100%", borderCollapse: "collapse" },
  thead: { borderBottom: `1px solid ${C.border}` },
  th: {
    fontSize: 11, fontWeight: 600, color: C.muted,
    textAlign: "left", padding: "10px 16px",
    textTransform: "uppercase", letterSpacing: "0.05em",
  },
  tr: { borderBottom: `1px solid ${C.border}` },
  td: { padding: "12px 16px", fontSize: 13 },

  // Chip
  chip: { display: "inline-block", padding: "3px 10px", borderRadius: 20, fontSize: 12, fontWeight: 600 },

  // Badge (header)
  badge: { display: "inline-block", padding: "4px 10px", borderRadius: 20, fontSize: 12, fontWeight: 600 },

  // Config
  envRow: {
    display: "flex", alignItems: "flex-start", justifyContent: "space-between",
    gap: 16, padding: "12px 0", borderBottom: `1px solid ${C.border}`,
  },
  envLabel: { fontSize: 13, fontWeight: 500, color: C.text },
  envHelp: { fontSize: 11, color: C.subtle, marginTop: 2 },
  envInput: {
    fontFamily: font, fontSize: 13,
    background: "#f8fafc", border: `1px solid ${C.border}`,
    borderRadius: 6, color: C.text,
    padding: "6px 10px", width: 130, textAlign: "right", flexShrink: 0,
    outline: "none",
  },
  toggle: {
    fontFamily: font, fontSize: 12, fontWeight: 600,
    padding: "5px 16px", border: "none", borderRadius: 20,
    cursor: "pointer", flexShrink: 0, minWidth: 56, transition: "all 0.15s",
  },
  btn: {
    fontFamily: font, fontSize: 13, fontWeight: 600,
    padding: "8px 18px", background: C.blue, color: "#fff",
    border: "none", borderRadius: 8, cursor: "pointer",
  },
};

// ── CSS ───────────────────────────────────────────────────────────────────────
if (typeof document !== "undefined") {
  const el = document.createElement("style");
  el.textContent = `
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #f1f5f9; }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #f1f5f9; }
    ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 3px; }
    input[type=number] { -moz-appearance: textfield; }
    input[type=number]::-webkit-inner-spin-button { opacity: 0.3; }
    input:focus { border-color: #2563eb !important; outline: none; }
    button:hover { opacity: 0.85; }
    tr:hover td { background: #f8fafc; }
  `;
  document.head.appendChild(el);
}
