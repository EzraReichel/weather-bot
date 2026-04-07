#!/bin/bash
set -e

echo "🔒 Starting Kalshi Weather Arb Bot (DRY RUN MODE)"
echo ""

# Check .env exists
if [ ! -f .env ]; then
  echo "❌ .env not found — copy .env.example to .env and fill in your keys"
  exit 1
fi

# Check Kalshi creds are filled in
if grep -q "your_kalshi_api_key_here" .env; then
  echo "⚠️  KALSHI_API_KEY_ID is still a placeholder in .env"
  echo "   Edit .env and add your real Kalshi API key before running."
  echo ""
fi

# Check PEM key exists if PATH is set
KEY_PATH=$(grep "^KALSHI_PRIVATE_KEY_PATH=" .env 2>/dev/null | cut -d= -f2- | tr -d '"' | tr -d "'")
if [ -n "$KEY_PATH" ] && [ "$KEY_PATH" != "" ] && [ ! -f "$KEY_PATH" ]; then
  echo "⚠️  KALSHI_PRIVATE_KEY_PATH=$KEY_PATH — file not found"
  echo "   Save your RSA private key to $KEY_PATH or set KALSHI_PRIVATE_KEY_PEM instead."
  echo ""
fi

echo "Loading .env..."
set -a; source .env; set +a

echo "DRY_RUN=$DRY_RUN"
echo "SCAN_INTERVAL_SECONDS=${SCAN_INTERVAL_SECONDS:-30}s"
echo "MIN_EDGE_THRESHOLD=${MIN_EDGE_THRESHOLD:-0.08}"
echo "PORT=${PORT:-8080}"
echo ""
echo "Mission Control UI → http://localhost:${PORT:-8080}"
echo "Health check       → http://localhost:${PORT:-8080}/health"
echo ""

python main.py
