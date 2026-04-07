# Kalshi Weather Arb Bot

Scans Kalshi weather temperature markets and alerts you on Discord when ensemble forecast probability diverges from market price. Uses a Gaussian CDF probability model calibrated for forecast lead time.

![Python](https://img.shields.io/badge/python-3.10+-blue) ![License](https://img.shields.io/badge/license-MIT-green)

**100% free to run.** All data sources are free. Only requires a Kalshi API key.

---

## How It Works

1. Fetches open markets from Kalshi's KXHIGH and KXLOW series (daily high/low temperature brackets across 5 cities)
2. Pulls 31-member GFS ensemble forecasts from the Open-Meteo API
3. Fits a Gaussian distribution to the ensemble and computes P(temp > threshold) via `scipy.stats.norm.cdf`
4. Inflates the standard deviation based on how far out the forecast is (more uncertainty = wider distribution)
5. Compares model probability to Kalshi's market price — if the gap exceeds the minimum profitable edge, fires a Discord alert

### Probability Engine

Raw ensemble fraction (28/31 members above threshold = 90%) is replaced by a calibrated Gaussian CDF:

```
P(high > threshold) = 1 - CDF(threshold, mean=ensemble_mean, std=adjusted_std)
```

Lead-time uncertainty correction:

| Hours until resolution | Std multiplier |
|------------------------|---------------|
| 0–12h | 1.0× |
| 12–24h | 1.1× |
| 24–48h | 1.3× |
| 48–72h | 1.5× |
| 72h+ | 1.8× |

If the Gaussian CDF and raw ensemble fraction disagree by more than 15%, the signal is flagged as low-confidence and requires 12% edge instead of 8%.

### Edge & Fees

Kalshi charges ~7% of profit. Minimum profitable edge = `fee_rate / (1 - fee_rate)` ≈ 7.5%. Default threshold is set to 8%, giving ~0.5% margin after fees.

Kelly sizing accounts for fees:

```
b_net = (1 - entry_price) / entry_price * (1 - fee_rate)
kelly_f = (b_net * p_win - p_lose) / b_net
position_size = kelly_f * KELLY_FRACTION * bankroll
```

---

## Discord Alerts

Alerts fire when a signal clears the edge threshold. Each alert includes:

- Market title and Kalshi ticker
- Model probability vs market price
- Edge %, recommended side (YES/NO), Kelly-sized position amount
- Ensemble mean/std, number of members, confidence score
- Low-confidence warning flag if CDF and raw fraction diverge

Daily summary posts at 23:55 UTC with total signals, trades taken, and P&L.

---

## Quick Start

```bash
git clone https://github.com/EzraReichel/weather-bot
cd weather-bot

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Fill in KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH, DISCORD_WEBHOOK_URL

python main.py
```

Health check available at `http://localhost:8080` once running.

---

## Configuration

All settings via environment variables (`.env` locally, Railway env vars in production):

| Variable | Default | Description |
|----------|---------|-------------|
| `KALSHI_API_KEY_ID` | — | Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_PATH` | — | Path to RSA private key PEM file |
| `KALSHI_PRIVATE_KEY_PEM` | — | Inline PEM string (use this on Railway) |
| `DISCORD_WEBHOOK_URL` | — | Discord webhook for alerts |
| `SIMULATION_MODE` | `true` | If true, logs signals but doesn't place real orders |
| `INITIAL_BANKROLL` | `1000` | Starting bankroll for Kelly sizing |
| `KELLY_FRACTION` | `0.15` | Fractional Kelly multiplier (15%) |
| `MIN_EDGE_THRESHOLD` | `0.08` | Minimum edge to alert (8%) |
| `KALSHI_FEE_RATE` | `0.07` | Kalshi fee rate (7% of profit) |
| `SCAN_INTERVAL_SECONDS` | `30` | How often to scan markets |
| `WEATHER_CITIES` | `nyc,chicago,miami,los_angeles,denver` | Cities to scan |
| `WEATHER_MAX_TRADE_SIZE` | `100` | Max Kelly position size ($) |
| `DATABASE_URL` | `sqlite:///./weatherbot.db` | Database connection string |
| `PORT` | `8080` | Health check port |

---

## Market Coverage

Scans 10 Kalshi series by default:

| Series | City | Metric |
|--------|------|--------|
| KXHIGHNY | New York | Daily high |
| KXHIGHCHI | Chicago | Daily high |
| KXHIGHMIA | Miami | Daily high |
| KXHIGHLAX | Los Angeles | Daily high |
| KXHIGHDEN | Denver | Daily high |
| KXLOWNY | New York | Daily low |
| KXLOWCHI | Chicago | Daily low |
| KXLOWMIA | Miami | Daily low |
| KXLOWLAX | Los Angeles | Daily low |
| KXLOWDEN | Denver | Daily low |

To add a new series, append a tuple to `WEATHER_SERIES` in [backend/data/kalshi_markets.py](backend/data/kalshi_markets.py):

```python
("KXHIGHATL", "atlanta", "high"),
```

---

## Deploy on Railway

1. Connect this repo in Railway
2. Set environment variables (use `KALSHI_PRIVATE_KEY_PEM` with the inline PEM string — no file needed)
3. Railway detects the `Procfile` and runs `python main.py` as a worker
4. Health check at `PORT` (Railway monitors it to confirm the service is alive)

---

## Project Structure

```
weather-bot/
├── main.py                          # Entry point — starts scheduler + health check
├── Procfile                         # worker: python main.py
├── requirements.txt
├── backend/
│   ├── config.py                    # All settings
│   ├── core/
│   │   ├── probability.py           # Gaussian CDF engine + Kelly sizing
│   │   ├── weather_signals.py       # Signal generation (scan → probability → edge)
│   │   ├── scheduler.py             # APScheduler jobs (scan, settlement, daily summary)
│   │   └── settlement.py            # Trade settlement via NWS observed temps
│   ├── data/
│   │   ├── kalshi_client.py         # Kalshi REST client (RSA-PSS auth)
│   │   ├── kalshi_markets.py        # Fetches KXHIGH + KXLOW series
│   │   ├── weather.py               # Open-Meteo ensemble + NWS observations
│   │   └── weather_markets.py       # WeatherMarket dataclass
│   ├── models/
│   │   └── database.py              # SQLAlchemy models (signals, trades, bot state)
│   └── notifications/
│       └── discord.py               # Discord webhook alerts
└── .env.example
```

---

## Data Sources

| Source | Used For | Auth |
|--------|----------|------|
| [Open-Meteo Ensemble API](https://ensemble-api.open-meteo.com) | 31-member GFS temperature forecasts | None |
| [NWS API](https://api.weather.gov) | Observed temperatures for settlement | None |
| [Kalshi API](https://kalshi.com) | Market prices (KXHIGH/KXLOW series) | RSA key |

---

## Disclaimer

Simulation mode is on by default. This does not place real trades unless you wire up order execution. Prediction markets involve risk of loss.

## License

MIT
