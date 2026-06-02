"""Summary metrics for a completed backtest run."""
from __future__ import annotations

from typing import Dict, List


def _max_drawdown(curve: List[dict]) -> float:
    peak = None
    max_dd = 0.0
    for pt in curve:
        v = pt.get("equity", pt.get("bankroll"))
        if v is None:
            continue
        if peak is None or v > peak:
            peak = v
        if peak and peak > 0:
            max_dd = max(max_dd, (peak - v) / peak)
    return round(max_dd, 4)


def compute_metrics(ledger: List[dict], equity_curve: List[dict],
                    initial_bankroll: float) -> Dict:
    """Headline stats + breakdowns from the trade ledger and equity curve."""
    filled = [t for t in ledger if t["filled"]]
    resolved = [t for t in filled if t["resolved"]]
    wins = [t for t in resolved if t["result"] == "win"]
    losses = [t for t in resolved if t["result"] == "loss"]
    total_pnl = round(sum(t["pnl"] for t in resolved if t["pnl"] is not None), 2)
    final = round(initial_bankroll + total_pnl, 2)

    # Brier on the model probability vs the YES outcome.
    brier_terms = []
    for t in resolved:
        if t.get("settlement") in ("yes", "no") and t.get("model_prob") is not None:
            yes_outcome = 1.0 if t["settlement"] == "yes" else 0.0
            brier_terms.append((t["model_prob"] - yes_outcome) ** 2)
    brier = round(sum(brier_terms) / len(brier_terms), 4) if brier_terms else None

    turnover = round(sum(t["size"] for t in filled), 2)

    def _breakdown(key: str) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for t in resolved:
            k = t.get(key) or "?"
            b = out.setdefault(k, {"wins": 0, "losses": 0, "pnl": 0.0, "n": 0})
            b["n"] += 1
            b["pnl"] = round(b["pnl"] + (t["pnl"] or 0.0), 2)
            if t["result"] == "win":
                b["wins"] += 1
            elif t["result"] == "loss":
                b["losses"] += 1
        return out

    return {
        "initial_bankroll": round(initial_bankroll, 2),
        "final_bankroll": final,
        "total_pnl": total_pnl,
        "roi": round(total_pnl / initial_bankroll, 4) if initial_bankroll else None,
        "n_signals": len(ledger),
        "n_filled": len(filled),
        "n_unfilled": len(ledger) - len(filled),
        "fill_rate": round(len(filled) / len(ledger), 4) if ledger else None,
        "n_resolved": len(resolved),
        "n_unsettled": len(filled) - len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "hit_rate": round(len(wins) / len(resolved), 4) if resolved else None,
        "brier": brier,
        "turnover": turnover,
        "max_drawdown": _max_drawdown(equity_curve),
        "by_city": _breakdown("city_key"),
        "by_metric": _breakdown("metric"),
        "by_agreement": _breakdown("agreement"),
        "bid_thin_fills": sum(1 for t in filled if t.get("bid_thin")),
    }
