"""
Strategy registry — the drop-in seam for new trading algorithms.

A strategy is just a callable ``(WeatherDay, StrategyParams, bankroll) -> Decision``.
The "default" strategy is the shared ``decide()`` core. A new algorithm registers
itself by name (a new module under weatherbot/backtest/strategies/ that calls
``register(...)`` at import); the runner loads it by name. The live worker never
imports this package, so new strategies can never affect the running bot.
"""
from __future__ import annotations

import importlib
import pkgutil
from typing import Callable, Dict, List

from weatherbot.backtest.decision import decide as _default_decide

_REGISTRY: Dict[str, Callable] = {"default": _default_decide}


def register(name: str, fn: Callable) -> None:
    _REGISTRY[name] = fn


def get_strategy(name: str) -> Callable:
    _ensure_loaded()
    if name not in _REGISTRY:
        raise KeyError(f"Unknown strategy '{name}'. Available: {sorted(_REGISTRY)}")
    return _REGISTRY[name]


def list_strategies() -> List[str]:
    _ensure_loaded()
    return sorted(_REGISTRY)


_loaded = False


def _ensure_loaded() -> None:
    """Import every module under strategies/ so they self-register."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    try:
        from weatherbot.backtest import strategies as pkg
        for mod in pkgutil.iter_modules(pkg.__path__):
            importlib.import_module(f"{pkg.__name__}.{mod.name}")
    except Exception:
        pass  # no strategies package / import error → just the default
