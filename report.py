#!/usr/bin/env python3
"""
Paper trading backtest report.
Usage: python report.py
"""
from dotenv import load_dotenv
load_dotenv()

from backend.models.paper_trade import init_paper_db, PaperSessionLocal, PaperTrade
from backend.core.paper_trading import get_paper_stats, get_model_accuracy

SEP  = "─" * 62
SEP2 = "═" * 62

def fmt_result(t: PaperTrade) -> str:
    if not t.resolved:
        return "PENDING"
    icon = "✅" if t.result == "win" else "❌"
    kalshi = "YES" if (t.actual_temp or 0) >= 1.0 else "NO"
    return f"{icon} {t.result.upper()}  (Kalshi={kalshi}  side={t.side.upper()})"

def main():
    init_paper_db()
    s = get_paper_stats()

    print()
    print(SEP2)
    print("  PAPER TRADING BACKTEST REPORT")
    print(SEP2)

    # ── Overview ──────────────────────────────────────────────────────────
    print(f"\n  Total trades logged:   {s['total']}")
    print(f"  Resolved:              {s['resolved']}")
    print(f"  Pending settlement:    {s['unresolved']}")

    if s["resolved"] > 0:
        win_rate = s["wins"] / s["resolved"] * 100
        print(f"\n  W/L Record:            {s['wins']}W / {s['losses']}L  ({win_rate:.0f}% win rate)")
    else:
        print(f"\n  W/L Record:            no resolved trades yet")

    pnl_sign = "+" if s["total_pnl"] >= 0 else ""
    print(f"  Total P&L:             {pnl_sign}${s['total_pnl']:.2f}")
    print(f"  Avg edge at entry:     {s['avg_edge']:+.1%}")

    if s["brier"] is not None:
        print(f"  Brier score:           {s['brier']:.4f}  (0=perfect, 0.25=random, lower=better)")
    else:
        print(f"  Brier score:           n/a (need resolved trades)")

    # ── City breakdown ────────────────────────────────────────────────────
    if s["cities"]:
        print(f"\n{SEP}")
        print("  BY CITY")
        print(SEP)
        print(f"  {'City':<16} {'W':>4} {'L':>4}  {'P&L':>10}")
        print(f"  {'────':<16} {'─':>4} {'─':>4}  {'───':>10}")
        for city, c in sorted(s["cities"].items()):
            pnl_sign = "+" if c["pnl"] >= 0 else ""
            print(f"  {city:<16} {c['wins']:>4} {c['losses']:>4}  {pnl_sign}${c['pnl']:>8.2f}")

    # ── Agreement breakdown ───────────────────────────────────────────────────
    if s.get("agreement_levels"):
        print(f"\n{SEP}")
        print("  BY AGREEMENT LEVEL  (key: does LOW agreement actually lose more?)")
        print(SEP)
        print(f"  {'Level':<10} {'W':>4} {'L':>4}  {'P&L':>10}")
        print(f"  {'─────':<10} {'─':>4} {'─':>4}  {'───':>10}")
        for lvl in ["HIGH", "MEDIUM", "LOW"]:
            if lvl not in s["agreement_levels"]:
                continue
            a = s["agreement_levels"][lvl]
            sign = "+" if a["pnl"] >= 0 else ""
            print(f"  {lvl:<10} {a['wins']:>4} {a['losses']:>4}  {sign}${a['pnl']:>8.2f}")

    # ── Trade log ─────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print("  FULL TRADE LOG")
    print(SEP)

    all_trades = sorted(s["all_trades"], key=lambda t: t.created_at)
    if not all_trades:
        print("  No paper trades yet.")
    else:
        for t in all_trades:
            side_str = f"{t.side.upper()} ({t.market_direction} {t.threshold_f:.0f}°F)"
            pnl_str = f"P&L ${t.pnl:+.2f}" if t.pnl is not None else "P&L pending"
            print(f"\n  {t.ticker}")
            print(f"    City:       {t.city}  |  Metric: {t.metric.upper()}")
            print(f"    Side:       {side_str}")
            print(f"    Entry:      {t.contracts} contracts @ {t.entry_price:.2%}  (${t.kelly_size:.0f} Kelly)")
            agr = getattr(t, "agreement", "MEDIUM") or "MEDIUM"
            print(f"    Signal:     model={t.model_prob:.1%}  market={t.market_price:.1%}  edge={t.edge:+.1%}  conf={t.confidence:.0%}  agreement={agr}")
            print(f"    Forecast:   mean={t.forecast_mean:.1f}°F  std={t.forecast_std:.1f}°F")
            print(f"    Logged:     {t.created_at.strftime('%Y-%m-%d %H:%M')} UTC  |  Resolves: {t.resolution_date}")
            print(f"    Result:     {fmt_result(t)}  |  {pnl_str}")

    # ── Calibration detail ────────────────────────────────────────────────
    resolved = s["resolved_trades"]
    if resolved:
        print(f"\n{SEP}")
        print("  CALIBRATION  (model probability vs actual outcome)")
        print(SEP)
        print(f"  {'Ticker':<32} {'Model':>6} {'Outcome':>8} {'Sq Err':>8}")
        print(f"  {'──────':<32} {'─────':>6} {'───────':>8} {'──────':>8}")
        for t in sorted(resolved, key=lambda t: t.created_at):
            # actual_temp stores Kalshi result: 1.0=YES won, 0.0=NO won
            yes_won = 1.0 if (t.actual_temp or 0) >= 1.0 else 0.0
            sq_err = (t.model_prob - yes_won) ** 2
            outcome_str = "YES won" if yes_won == 1.0 else "NO won"
            print(f"  {t.ticker:<32} {t.model_prob:>6.1%} {outcome_str:>8}  {sq_err:>7.4f}")

        brier = sum(
            (t.model_prob - (1.0 if (t.actual_temp or 0) >= 1.0 else 0.0)) ** 2
            for t in resolved
        ) / len(resolved)
        print(f"\n  Brier Score: {brier:.4f}")
        print(f"  (0.00 = perfect calibration  |  0.25 = random guessing)")

    # ── Per-model city accuracy ───────────────────────────────────────────────
    accuracy = get_model_accuracy()
    if accuracy:
        print(f"\n{SEP}")
        print("  PER-MODEL CITY ACCURACY  (which model is best where?)")
        print(SEP)
        print(f"  {'Model':<8} {'City':<16} {'Metric':<6} {'N':>4} {'W':>4} {'L':>4}  {'Brier':>7}")
        print(f"  {'─────':<8} {'────':<16} {'──────':<6} {'─':>4} {'─':>4} {'─':>4}  {'─────':>7}")
        for r in accuracy:
            if r["n"] == 0:
                continue
            brier_str = f"{r['brier']:.4f}" if r["brier"] is not None else "  n/a"
            print(f"  {r['model']:<8} {r['city']:<16} {r['metric']:<6} {r['n']:>4} {r['wins']:>4} {r['losses']:>4}  {brier_str:>7}")

    print(f"\n{SEP2}\n")


if __name__ == "__main__":
    main()
