"""
Drop-in strategy modules. Each module registers one or more strategies by
calling weatherbot.backtest.registry.register(name, fn) at import time. The
registry imports every module here on first use.
"""
