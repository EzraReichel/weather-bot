import { useState, useEffect, useCallback, useRef } from "react";

const STORAGE_KEY = "mission-control-data";

// ── helpers ──────────────────────────────────────────────────────
const fmt = (n) => (typeof n === "number" ? n.toFixed(2) : "–");
const pct = (n) => (typeof n === "number" ? (n * 100).toFixed(1) + "%" : "–");
const usd = (n) =>
  typeof n === "number"
    ? "$" + n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : "–";
const ts = () => new Date().toISOString().replace("T", " ").slice(0, 19);
const uid = () => Math.random().toString(36).slice(2, 10);

const STATUS = { PENDING: "PENDING", WON: "WON", LOST: "LOST" };
const SIDES = ["BUY YES", "BUY NO"];
const CITIES = ["NYC", "CHI", "MIA", "LAX", "DEN"];
const TYPES = ["KXHIGH", "KXLOW"];

// ── default state ────────────────────────────────────────────────
const defaultState = () => ({
  bankroll: 1000,
  kellyFraction: 0.15,
  feeRate: 0.07,
  minEdge: 0.08,
  bets: [],
  signals: [],
  log: [{ t: ts(), msg: "SYSTEM ONLINE ✓ Mission Control initialized" }],
});

// ── main app ─────────────────────────────────────────────────────
export default function MissionControl() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("dashboard");
  const [showNewBet, setShowNewBet] = useState(false);
  const [showNewSignal, setShowNewSignal] = useState(false);
  const [clockTime, setClockTime] = useState(new Date());
  const tickRef = useRef(null);

  useEffect(() => {
    (async () => {
      try {
        const result = await window.storage.get(STORAGE_KEY);
        if (result && result.value) {
          setData(JSON.parse(result.value));
        } else {
          setData(defaultState());
        }
      } catch {
        setData(defaultState());
      }
      setLoading(false);
    })();
  }, []);

  useEffect(() => {
    if (data && !loading) {
      window.storage.set(STORAGE_KEY, JSON.stringify(data)).catch(() => {});
    }
  }, [data, loading]);

  useEffect(() => {
    tickRef.current = setInterval(() => setClockTime(new Date()), 1000);
    return () => clearInterval(tickRef.current);
  }, []);

  const addLog = useCallback(
    (msg) => setData((d) => ({ ...d, log: [{ t: ts(), msg }, ...d.log].slice(0, 100) })),
    []
  );

  const addBet = useCallback(
    (bet) => {
      setData((d) => ({
        ...d,
        bets: [{ ...bet, id: uid(), status: STATUS.PENDING, createdAt: ts() }, ...d.bets],
      }));
      addLog(`BET PLACED → ${bet.ticker} ${bet.side} @ ${usd(bet.price)} × ${bet.contracts} contracts`);
    },
    [addLog]
  );

  const resolveBet = useCallback(
    (id, result) => {
      setData((d) => ({
        ...d,
        bets: d.bets.map((b) => (b.id === id ? { ...b, status: result, resolvedAt: ts() } : b)),
      }));
      addLog(`BET RESOLVED → ${id.slice(0, 6)} → ${result}`);
    },
    [addLog]
  );

  const addSignal = useCallback(
    (signal) => {
      setData((d) => ({
        ...d,
        signals: [{ ...signal, id: uid(), receivedAt: ts() }, ...d.signals].slice(0, 200),
      }));
      addLog(`SIGNAL RECEIVED → ${signal.ticker} edge ${pct(signal.edge)}`);
    },
    [addLog]
  );

  const updateConfig = useCallback(
    (key, val) => {
      setData((d) => ({ ...d, [key]: val }));
      addLog(`CONFIG UPDATED → ${key} = ${val}`);
    },
    [addLog]
  );

  const resetAll = useCallback(() => {
    setData(defaultState());
  }, []);

  if (loading || !data)
    return (
      <div style={styles.bootScreen}>
        <div style={styles.bootText}>INITIALIZING SYSTEMS...</div>
        <div style={styles.bootBar}>
          <div style={styles.bootFill} />
        </div>
      </div>
    );

  const pending = data.bets.filter((b) => b.status === STATUS.PENDING);
  const resolved = data.bets.filter((b) => b.status !== STATUS.PENDING);
  const won = resolved.filter((b) => b.status === STATUS.WON);
  const lost = resolved.filter((b) => b.status === STATUS.LOST);

  const totalDeployed = pending.reduce((s, b) => s + b.price * b.contracts, 0);
  const totalWon = won.reduce((s, b) => s + (1 - b.price) * b.contracts * (1 - data.feeRate), 0);
  const totalLost = lost.reduce((s, b) => s + b.price * b.contracts, 0);
  const pnl = totalWon - totalLost;
  const winRate = resolved.length > 0 ? won.length / resolved.length : 0;
  const avgEdge = data.bets.length > 0 ? data.bets.reduce((s, b) => s + (b.edge || 0), 0) / data.bets.length : 0;

  const kellySize = (edge, prob) => {
    if (!edge || !prob || prob <= 0 || prob >= 1) return 0;
    const odds = (1 - data.feeRate) / prob - 1;
    const kelly = (prob * odds - (1 - prob)) / odds;
    return Math.max(0, Math.min(kelly * data.kellyFraction * data.bankroll, data.bankroll * 0.05));
  };

  const clockStr = clockTime.toISOString().replace("T", "  ").slice(0, 21) + " UTC";

  return (
    <div style={styles.root}>
      <div style={styles.scanlines} />

      <header style={styles.header}>
        <div style={styles.headerLeft}>
          <div style={styles.logo}>◈</div>
          <div>
            <div style={styles.title}>KALSHI WEATHER ARB</div>
            <div style={styles.subtitle}>MISSION CONTROL v1.0</div>
          </div>
        </div>
        <div style={styles.clock}>{clockStr}</div>
        <div style={styles.headerRight}>
          <div style={{ ...styles.statusDot, background: pending.length > 0 ? "#0f0" : "#555" }} />
          <span style={styles.statusText}>
            {pending.length > 0 ? `${pending.length} ACTIVE` : "STANDBY"}
          </span>
        </div>
      </header>

      <nav style={styles.nav}>
        {["dashboard", "bets", "signals", "config"].map((v) => (
          <button
            key={v}
            onClick={() => setView(v)}
            style={{ ...styles.navBtn, ...(view === v ? styles.navActive : {}) }}
          >
            {v.toUpperCase()}
          </button>
        ))}
      </nav>

      <main style={styles.main}>
        {view === "dashboard" && (
          <Dashboard
            {...{ data, pending, resolved, won, lost, totalDeployed, totalWon, totalLost, pnl, winRate, avgEdge, kellySize, setShowNewBet, setShowNewSignal }}
          />
        )}
        {view === "bets" && <BetsView bets={data.bets} resolveBet={resolveBet} setShowNewBet={setShowNewBet} />}
        {view === "signals" && (
          <SignalsView signals={data.signals} addBet={addBet} kellySize={kellySize} bankroll={data.bankroll} setShowNewSignal={setShowNewSignal} />
        )}
        {view === "config" && <ConfigView data={data} updateConfig={updateConfig} resetAll={resetAll} />}
      </main>

      <footer style={styles.footer}>
        <span style={styles.footerLabel}>LOG ▸</span>
        <div style={styles.footerScroll}>
          {data.log.slice(0, 5).map((l, i) => (
            <span key={i} style={styles.logEntry}>
              <span style={styles.logTime}>{l.t.slice(11)}</span> {l.msg}
              {i < 4 ? "  ·  " : ""}
            </span>
          ))}
        </div>
      </footer>

      {showNewBet && <NewBetModal addBet={addBet} close={() => setShowNewBet(false)} kellySize={kellySize} bankroll={data.bankroll} />}
      {showNewSignal && <NewSignalModal addSignal={addSignal} close={() => setShowNewSignal(false)} />}
    </div>
  );
}

function Dashboard({ data, pending, resolved, won, lost, totalDeployed, totalWon, totalLost, pnl, winRate, avgEdge, kellySize, setShowNewBet, setShowNewSignal }) {
  const metrics = [
    { label: "BANKROLL", value: usd(data.bankroll), color: "#0ff" },
    { label: "DEPLOYED", value: usd(totalDeployed), color: "#ff0" },
    { label: "P&L", value: usd(pnl), color: pnl >= 0 ? "#0f0" : "#f33" },
    { label: "WIN RATE", value: pct(winRate), color: winRate >= 0.5 ? "#0f0" : "#f93" },
    { label: "AVG EDGE", value: pct(avgEdge), color: "#0ff" },
    { label: "ACTIVE BETS", value: pending.length, color: "#ff0" },
  ];

  const recentSignals = data.signals.slice(0, 5);
  const recentBets = pending.slice(0, 5);

  return (
    <div style={styles.dashGrid}>
      <div style={styles.metricsRow}>
        {metrics.map((m, i) => (
          <div key={i} style={{ ...styles.metricCard, animationDelay: `${i * 0.08}s` }}>
            <div style={styles.metricLabel}>{m.label}</div>
            <div style={{ ...styles.metricValue, color: m.color }}>{m.value}</div>
          </div>
        ))}
      </div>

      <div style={styles.dashCols}>
        <div style={styles.panel}>
          <div style={styles.panelHeader}>
            <span>RECENT SIGNALS</span>
            <button style={styles.smallBtn} onClick={() => setShowNewSignal(true)}>+ ADD</button>
          </div>
          {recentSignals.length === 0 ? (
            <div style={styles.empty}>No signals yet — waiting for bot alerts</div>
          ) : (
            recentSignals.map((s) => (
              <div key={s.id} style={styles.signalRow}>
                <div style={styles.signalTicker}>{s.ticker}</div>
                <div style={styles.signalDetail}>
                  <span>Model: {pct(s.modelProb)}</span>
                  <span>Mkt: {pct(s.marketPrice)}</span>
                  <span style={{ color: s.edge >= 0.1 ? "#0f0" : "#ff0" }}>Edge: {pct(s.edge)}</span>
                </div>
                <div style={styles.signalSide}>{s.side}</div>
              </div>
            ))
          )}
        </div>

        <div style={styles.panel}>
          <div style={styles.panelHeader}>
            <span>ACTIVE POSITIONS</span>
            <button style={styles.smallBtn} onClick={() => setShowNewBet(true)}>+ BET</button>
          </div>
          {recentBets.length === 0 ? (
            <div style={styles.empty}>No active bets</div>
          ) : (
            recentBets.map((b) => (
              <div key={b.id} style={styles.betRow}>
                <div>
                  <div style={styles.betTicker}>{b.ticker}</div>
                  <div style={styles.betDetail}>{b.side} @ {usd(b.price)} × {b.contracts}</div>
                </div>
                <div style={styles.betCost}>{usd(b.price * b.contracts)}</div>
              </div>
            ))
          )}
        </div>
      </div>

      <div style={styles.panel}>
        <div style={styles.panelHeader}>RESOLUTION HISTORY</div>
        <div style={styles.historyRow}>
          <div style={styles.histBox}>
            <div style={{ ...styles.histNum, color: "#0f0" }}>{won.length}</div>
            <div style={styles.histLabel}>WON</div>
          </div>
          <div style={styles.histBox}>
            <div style={{ ...styles.histNum, color: "#f33" }}>{lost.length}</div>
            <div style={styles.histLabel}>LOST</div>
          </div>
          <div style={styles.histBox}>
            <div style={{ ...styles.histNum, color: "#0ff" }}>{resolved.length}</div>
            <div style={styles.histLabel}>TOTAL</div>
          </div>
          <div style={styles.histBox}>
            <div style={{ ...styles.histNum, color: pnl >= 0 ? "#0f0" : "#f33" }}>{usd(pnl)}</div>
            <div style={styles.histLabel}>NET P&L</div>
          </div>
        </div>
      </div>
    </div>
  );
}

function BetsView({ bets, resolveBet, setShowNewBet }) {
  return (
    <div>
      <div style={styles.viewHeader}>
        <span>ALL BETS ({bets.length})</span>
        <button style={styles.actionBtn} onClick={() => setShowNewBet(true)}>+ NEW BET</button>
      </div>
      <div style={styles.tableWrap}>
        <table style={styles.table}>
          <thead>
            <tr>
              {["TICKER", "SIDE", "PRICE", "QTY", "COST", "EDGE", "STATUS", "ACTIONS"].map((h) => (
                <th key={h} style={styles.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {bets.map((b) => (
              <tr key={b.id} style={styles.tr}>
                <td style={styles.td}>{b.ticker}</td>
                <td style={{ ...styles.td, color: b.side === "BUY YES" ? "#0f0" : "#f93" }}>{b.side}</td>
                <td style={styles.td}>{usd(b.price)}</td>
                <td style={styles.td}>{b.contracts}</td>
                <td style={styles.td}>{usd(b.price * b.contracts)}</td>
                <td style={styles.td}>{pct(b.edge)}</td>
                <td style={{ ...styles.td, color: b.status === STATUS.WON ? "#0f0" : b.status === STATUS.LOST ? "#f33" : "#ff0" }}>
                  {b.status}
                </td>
                <td style={styles.td}>
                  {b.status === STATUS.PENDING && (
                    <>
                      <button style={styles.tinyBtn} onClick={() => resolveBet(b.id, STATUS.WON)}>WIN</button>
                      <button style={{ ...styles.tinyBtn, borderColor: "#f33", color: "#f33" }} onClick={() => resolveBet(b.id, STATUS.LOST)}>LOSS</button>
                    </>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SignalsView({ signals, addBet, kellySize, bankroll, setShowNewSignal }) {
  const actOn = (s) => {
    const size = kellySize(s.edge, s.modelProb);
    const contracts = Math.floor(size / s.marketPrice);
    if (contracts < 1) return;
    addBet({
      ticker: s.ticker,
      side: s.side,
      price: s.marketPrice,
      contracts,
      edge: s.edge,
      modelProb: s.modelProb,
      signalId: s.id,
    });
  };

  return (
    <div>
      <div style={styles.viewHeader}>
        <span>ALL SIGNALS ({signals.length})</span>
        <button style={styles.actionBtn} onClick={() => setShowNewSignal(true)}>+ ADD SIGNAL</button>
      </div>
      <div style={styles.tableWrap}>
        <table style={styles.table}>
          <thead>
            <tr>
              {["TIME", "TICKER", "MODEL", "MARKET", "EDGE", "SIDE", "KELLY $", "ACTION"].map((h) => (
                <th key={h} style={styles.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {signals.map((s) => {
              const k = kellySize(s.edge, s.modelProb);
              return (
                <tr key={s.id} style={styles.tr}>
                  <td style={styles.td}>{s.receivedAt?.slice(11) || "–"}</td>
                  <td style={styles.td}>{s.ticker}</td>
                  <td style={styles.td}>{pct(s.modelProb)}</td>
                  <td style={styles.td}>{pct(s.marketPrice)}</td>
                  <td style={{ ...styles.td, color: s.edge >= 0.1 ? "#0f0" : "#ff0" }}>{pct(s.edge)}</td>
                  <td style={{ ...styles.td, color: s.side === "BUY YES" ? "#0f0" : "#f93" }}>{s.side}</td>
                  <td style={styles.td}>{usd(k)}</td>
                  <td style={styles.td}>
                    <button style={styles.tinyBtn} onClick={() => actOn(s)}>BET</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ConfigView({ data, updateConfig, resetAll }) {
  const fields = [
    { key: "bankroll", label: "BANKROLL ($)", step: 100, min: 0 },
    { key: "kellyFraction", label: "KELLY FRACTION", step: 0.01, min: 0.01, max: 1 },
    { key: "feeRate", label: "KALSHI FEE RATE", step: 0.01, min: 0, max: 0.5 },
    { key: "minEdge", label: "MIN EDGE THRESHOLD", step: 0.01, min: 0, max: 0.5 },
  ];

  return (
    <div style={styles.configGrid}>
      <div style={styles.panel}>
        <div style={styles.panelHeader}>PARAMETERS</div>
        {fields.map((f) => (
          <div key={f.key} style={styles.configRow}>
            <label style={styles.configLabel}>{f.label}</label>
            <input
              type="number"
              step={f.step}
              min={f.min}
              max={f.max}
              value={data[f.key]}
              onChange={(e) => updateConfig(f.key, parseFloat(e.target.value) || 0)}
              style={styles.configInput}
            />
          </div>
        ))}
      </div>
      <div style={styles.panel}>
        <div style={styles.panelHeader}>KELLY CALCULATOR</div>
        <KellyCalc bankroll={data.bankroll} kellyFraction={data.kellyFraction} feeRate={data.feeRate} />
      </div>
      <div style={styles.panel}>
        <div style={styles.panelHeader}>SYSTEM</div>
        <button style={{ ...styles.actionBtn, borderColor: "#f33", color: "#f33", marginTop: 12 }} onClick={() => { if (confirm("Reset all data? This cannot be undone.")) resetAll(); }}>
          RESET ALL DATA
        </button>
      </div>
    </div>
  );
}

function KellyCalc({ bankroll, kellyFraction, feeRate }) {
  const [prob, setProb] = useState(0.8);
  const [market, setMarket] = useState(0.65);
  const edge = prob - market;
  const odds = (1 - feeRate) / market - 1;
  const rawKelly = (prob * odds - (1 - prob)) / odds;
  const fracKelly = rawKelly * kellyFraction;
  const size = Math.max(0, Math.min(fracKelly * bankroll, bankroll * 0.05));
  const contracts = Math.floor(size / market);

  return (
    <div>
      <div style={styles.configRow}>
        <label style={styles.configLabel}>MODEL PROB</label>
        <input type="range" min={0.01} max={0.99} step={0.01} value={prob} onChange={(e) => setProb(+e.target.value)} style={{ flex: 1 }} />
        <span style={styles.calcVal}>{pct(prob)}</span>
      </div>
      <div style={styles.configRow}>
        <label style={styles.configLabel}>MARKET PRICE</label>
        <input type="range" min={0.01} max={0.99} step={0.01} value={market} onChange={(e) => setMarket(+e.target.value)} style={{ flex: 1 }} />
        <span style={styles.calcVal}>{pct(market)}</span>
      </div>
      <div style={styles.calcResults}>
        <div style={styles.calcRow}><span>Edge</span><span style={{ color: edge > 0 ? "#0f0" : "#f33" }}>{pct(edge)}</span></div>
        <div style={styles.calcRow}><span>Raw Kelly</span><span>{pct(rawKelly)}</span></div>
        <div style={styles.calcRow}><span>Fractional Kelly</span><span>{pct(fracKelly)}</span></div>
        <div style={styles.calcRow}><span>Position Size</span><span style={{ color: "#0ff" }}>{usd(size)}</span></div>
        <div style={styles.calcRow}><span>Contracts</span><span style={{ color: "#ff0" }}>{contracts}</span></div>
      </div>
    </div>
  );
}

function NewBetModal({ addBet, close, kellySize, bankroll }) {
  const [ticker, setTicker] = useState("");
  const [side, setSide] = useState("BUY YES");
  const [price, setPrice] = useState(0.5);
  const [contracts, setContracts] = useState(10);
  const [edge, setEdge] = useState(0.1);
  const [modelProb, setModelProb] = useState(0.8);

  const submit = () => {
    if (!ticker) return;
    addBet({ ticker, side, price, contracts, edge, modelProb });
    close();
  };

  return (
    <div style={styles.overlay} onClick={close}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div style={styles.modalTitle}>NEW BET</div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>TICKER</label>
          <input style={styles.formInput} value={ticker} onChange={(e) => setTicker(e.target.value.toUpperCase())} placeholder="KXHIGHNY-26APR07-T75" />
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>SIDE</label>
          <select style={styles.formInput} value={side} onChange={(e) => setSide(e.target.value)}>
            {SIDES.map((s) => <option key={s}>{s}</option>)}
          </select>
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>PRICE ($)</label>
          <input type="number" step={0.01} min={0.01} max={0.99} style={styles.formInput} value={price} onChange={(e) => setPrice(+e.target.value)} />
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>CONTRACTS</label>
          <input type="number" step={1} min={1} style={styles.formInput} value={contracts} onChange={(e) => setContracts(+e.target.value)} />
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>EDGE</label>
          <input type="number" step={0.01} style={styles.formInput} value={edge} onChange={(e) => setEdge(+e.target.value)} />
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>MODEL PROB</label>
          <input type="number" step={0.01} style={styles.formInput} value={modelProb} onChange={(e) => setModelProb(+e.target.value)} />
        </div>
        <div style={styles.modalCost}>TOTAL COST: {usd(price * contracts)}</div>
        <div style={styles.modalActions}>
          <button style={styles.actionBtn} onClick={submit}>CONFIRM BET</button>
          <button style={{ ...styles.actionBtn, borderColor: "#555", color: "#888" }} onClick={close}>CANCEL</button>
        </div>
      </div>
    </div>
  );
}

function NewSignalModal({ addSignal, close }) {
  const [ticker, setTicker] = useState("");
  const [modelProb, setModelProb] = useState(0.85);
  const [marketPrice, setMarketPrice] = useState(0.65);
  const [side, setSide] = useState("BUY YES");
  const [confidence, setConfidence] = useState(0.8);
  const edge = modelProb - marketPrice;

  const submit = () => {
    if (!ticker) return;
    addSignal({ ticker, modelProb, marketPrice, side, confidence, edge });
    close();
  };

  return (
    <div style={styles.overlay} onClick={close}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div style={styles.modalTitle}>LOG SIGNAL</div>
        <p style={styles.modalDesc}>Enter signal details from your Discord bot notification</p>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>TICKER</label>
          <input style={styles.formInput} value={ticker} onChange={(e) => setTicker(e.target.value.toUpperCase())} placeholder="KXHIGHNY-26APR07-T75" />
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>MODEL PROBABILITY</label>
          <input type="number" step={0.01} min={0} max={1} style={styles.formInput} value={modelProb} onChange={(e) => setModelProb(+e.target.value)} />
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>MARKET PRICE</label>
          <input type="number" step={0.01} min={0} max={1} style={styles.formInput} value={marketPrice} onChange={(e) => setMarketPrice(+e.target.value)} />
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>SIDE</label>
          <select style={styles.formInput} value={side} onChange={(e) => setSide(e.target.value)}>
            {SIDES.map((s) => <option key={s}>{s}</option>)}
          </select>
        </div>
        <div style={styles.formRow}>
          <label style={styles.formLabel}>CONFIDENCE</label>
          <input type="number" step={0.01} min={0} max={1} style={styles.formInput} value={confidence} onChange={(e) => setConfidence(+e.target.value)} />
        </div>
        <div style={{ ...styles.modalCost, color: edge >= 0.08 ? "#0f0" : "#f93" }}>
          EDGE: {pct(edge)} {edge >= 0.08 ? "✓ ABOVE THRESHOLD" : "✗ BELOW THRESHOLD"}
        </div>
        <div style={styles.modalActions}>
          <button style={styles.actionBtn} onClick={submit}>LOG SIGNAL</button>
          <button style={{ ...styles.actionBtn, borderColor: "#555", color: "#888" }} onClick={close}>CANCEL</button>
        </div>
      </div>
    </div>
  );
}

const font = "'Share Tech Mono', 'Courier New', monospace";
const styles = {
  root: { fontFamily: font, background: "#0a0c10", color: "#c8d0d8", minHeight: "100vh", position: "relative", overflow: "hidden", fontSize: 13 },
  scanlines: { position: "fixed", inset: 0, background: "repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,255,200,0.015) 2px, rgba(0,255,200,0.015) 4px)", pointerEvents: "none", zIndex: 999 },
  header: { display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 20px", borderBottom: "1px solid #1a2030", background: "linear-gradient(180deg, #0d1018 0%, #0a0c10 100%)" },
  headerLeft: { display: "flex", alignItems: "center", gap: 12 },
  logo: { fontSize: 28, color: "#0ff", textShadow: "0 0 12px rgba(0,255,255,0.5)", lineHeight: 1 },
  title: { fontSize: 15, fontWeight: 700, color: "#fff", letterSpacing: 3, fontFamily: font },
  subtitle: { fontSize: 10, color: "#556", letterSpacing: 2, fontFamily: font },
  clock: { fontSize: 12, color: "#0ff", letterSpacing: 1.5, textShadow: "0 0 8px rgba(0,255,255,0.3)", fontFamily: font },
  headerRight: { display: "flex", alignItems: "center", gap: 8 },
  statusDot: { width: 8, height: 8, borderRadius: "50%", boxShadow: "0 0 6px currentColor" },
  statusText: { fontSize: 11, letterSpacing: 1.5, fontFamily: font },
  nav: { display: "flex", gap: 0, borderBottom: "1px solid #1a2030", background: "#0c0e14" },
  navBtn: { fontFamily: font, fontSize: 11, letterSpacing: 2, padding: "10px 24px", background: "none", border: "none", borderBottom: "2px solid transparent", color: "#556", cursor: "pointer", transition: "all 0.2s" },
  navActive: { color: "#0ff", borderBottomColor: "#0ff", textShadow: "0 0 8px rgba(0,255,255,0.3)" },
  main: { padding: 20, minHeight: "calc(100vh - 140px)", overflow: "auto" },
  footer: { position: "fixed", bottom: 0, left: 0, right: 0, display: "flex", alignItems: "center", gap: 12, padding: "6px 20px", background: "#0c0e14", borderTop: "1px solid #1a2030", overflow: "hidden", whiteSpace: "nowrap" },
  footerLabel: { fontSize: 10, color: "#0ff", letterSpacing: 2, flexShrink: 0, fontFamily: font },
  footerScroll: { fontSize: 10, color: "#445", overflow: "hidden", fontFamily: font },
  logEntry: { fontFamily: font },
  logTime: { color: "#335" },
  dashGrid: { display: "flex", flexDirection: "column", gap: 16 },
  metricsRow: { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))", gap: 12 },
  metricCard: { background: "linear-gradient(135deg, #111420 0%, #0d1018 100%)", border: "1px solid #1a2030", borderRadius: 4, padding: "14px 16px", animation: "fadeSlideIn 0.4s ease both" },
  metricLabel: { fontSize: 9, color: "#445", letterSpacing: 2, marginBottom: 6, fontFamily: font },
  metricValue: { fontSize: 22, fontWeight: 700, fontFamily: font },
  dashCols: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 },
  panel: { background: "#0d1018", border: "1px solid #1a2030", borderRadius: 4, padding: 16 },
  panelHeader: { display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 11, color: "#0ff", letterSpacing: 2, marginBottom: 14, paddingBottom: 8, borderBottom: "1px solid #1a2030", fontFamily: font },
  empty: { color: "#334", fontSize: 11, fontStyle: "italic", padding: "12px 0", fontFamily: font },
  signalRow: { padding: "8px 0", borderBottom: "1px solid #111820" },
  signalTicker: { fontSize: 12, color: "#fff", marginBottom: 4, fontFamily: font },
  signalDetail: { display: "flex", gap: 16, fontSize: 10, color: "#667", fontFamily: font },
  signalSide: { fontSize: 10, color: "#0ff", marginTop: 2, fontFamily: font },
  betRow: { display: "flex", justifyContent: "space-between", alignItems: "center", padding: "8px 0", borderBottom: "1px solid #111820" },
  betTicker: { fontSize: 12, color: "#fff", fontFamily: font },
  betDetail: { fontSize: 10, color: "#667", marginTop: 2, fontFamily: font },
  betCost: { fontSize: 14, color: "#ff0", fontFamily: font },
  historyRow: { display: "flex", justifyContent: "space-around", paddingTop: 8 },
  histBox: { textAlign: "center" },
  histNum: { fontSize: 28, fontWeight: 700, fontFamily: font },
  histLabel: { fontSize: 9, color: "#445", letterSpacing: 2, marginTop: 4, fontFamily: font },
  viewHeader: { display: "flex", justifyContent: "space-between", alignItems: "center", fontSize: 13, color: "#0ff", letterSpacing: 2, marginBottom: 16, fontFamily: font },
  tableWrap: { overflowX: "auto" },
  table: { width: "100%", borderCollapse: "collapse" },
  th: { fontSize: 9, color: "#445", letterSpacing: 2, textAlign: "left", padding: "8px 10px", borderBottom: "1px solid #1a2030", fontFamily: font },
  tr: { borderBottom: "1px solid #111820" },
  td: { padding: "8px 10px", fontSize: 11, fontFamily: font },
  smallBtn: { fontFamily: font, fontSize: 9, letterSpacing: 1, padding: "4px 10px", background: "none", border: "1px solid #0ff", color: "#0ff", borderRadius: 3, cursor: "pointer" },
  actionBtn: { fontFamily: font, fontSize: 11, letterSpacing: 2, padding: "8px 20px", background: "none", border: "1px solid #0ff", color: "#0ff", borderRadius: 3, cursor: "pointer" },
  tinyBtn: { fontFamily: font, fontSize: 9, padding: "3px 8px", background: "none", border: "1px solid #0ff", color: "#0ff", borderRadius: 2, cursor: "pointer", marginRight: 4 },
  configGrid: { display: "flex", flexDirection: "column", gap: 16 },
  configRow: { display: "flex", alignItems: "center", gap: 12, marginBottom: 10 },
  configLabel: { fontSize: 10, color: "#556", letterSpacing: 1.5, width: 160, flexShrink: 0, fontFamily: font },
  configInput: { fontFamily: font, fontSize: 13, background: "#111420", border: "1px solid #1a2030", color: "#fff", padding: "6px 10px", borderRadius: 3, width: 120 },
  calcVal: { fontSize: 12, color: "#0ff", width: 50, textAlign: "right", fontFamily: font },
  calcResults: { marginTop: 16, borderTop: "1px solid #1a2030", paddingTop: 12 },
  calcRow: { display: "flex", justifyContent: "space-between", padding: "4px 0", fontSize: 11, color: "#889", fontFamily: font },
  overlay: { position: "fixed", inset: 0, background: "rgba(0,0,0,0.8)", display: "flex", alignItems: "center", justifyContent: "center", zIndex: 1000 },
  modal: { background: "#0d1018", border: "1px solid #1a2030", borderRadius: 6, padding: 28, width: 400, maxWidth: "90vw", maxHeight: "90vh", overflow: "auto", boxShadow: "0 0 60px rgba(0,255,255,0.08)" },
  modalTitle: { fontSize: 14, color: "#0ff", letterSpacing: 3, marginBottom: 6, fontFamily: font },
  modalDesc: { fontSize: 10, color: "#556", marginBottom: 16, fontFamily: font },
  formRow: { display: "flex", flexDirection: "column", gap: 4, marginBottom: 12 },
  formLabel: { fontSize: 9, color: "#556", letterSpacing: 1.5, fontFamily: font },
  formInput: { fontFamily: font, fontSize: 13, background: "#111420", border: "1px solid #1a2030", color: "#fff", padding: "8px 10px", borderRadius: 3 },
  modalCost: { fontSize: 14, color: "#ff0", textAlign: "center", padding: "12px 0", borderTop: "1px solid #1a2030", marginTop: 8, fontFamily: font },
  modalActions: { display: "flex", gap: 12, marginTop: 16, justifyContent: "center" },
  bootScreen: { display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100vh", background: "#0a0c10", fontFamily: font },
  bootText: { fontSize: 12, color: "#0ff", letterSpacing: 4, marginBottom: 20 },
  bootBar: { width: 200, height: 2, background: "#1a2030", borderRadius: 1, overflow: "hidden" },
  bootFill: { width: "60%", height: "100%", background: "#0ff", animation: "bootPulse 1s ease infinite" },
};

if (typeof document !== "undefined") {
  const styleEl = document.createElement("style");
  styleEl.textContent = `
    @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
    @keyframes fadeSlideIn { from { opacity:0; transform:translateY(8px) } to { opacity:1; transform:translateY(0) } }
    @keyframes bootPulse { 0%,100% { opacity:0.4 } 50% { opacity:1 } }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    ::-webkit-scrollbar { width: 4px; height: 4px; }
    ::-webkit-scrollbar-track { background: #0a0c10; }
    ::-webkit-scrollbar-thumb { background: #1a2030; border-radius: 2px; }
    input[type=range] { accent-color: #0ff; }
    select option { background: #111420; color: #fff; }
    button:hover { opacity: 0.8; }
  `;
  document.head.appendChild(styleEl);
}
