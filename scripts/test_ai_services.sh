#!/usr/bin/env bash
# Test script for ai-services endpoints (http://localhost:8003)
# Requires DEBUG=True in .env — no auth token needed.

BASE="http://localhost:8003/api/v1"
BOLD="\033[1m"
GREEN="\033[32m"
CYAN="\033[36m"
RESET="\033[0m"

divider() { echo -e "\n${BOLD}─────────────────────────────────────${RESET}"; }
header()  { echo -e "${CYAN}${BOLD}▶ $1${RESET}"; }

# ─── 1. Health ───────────────────────────────────────────────────────────────
divider
header "Health check"
curl -s "$BASE/health" | python3 -m json.tool

# ─── 2. Sentiment — single text ──────────────────────────────────────────────
divider
header "Sentiment — single positive text"
curl -s -X POST "$BASE/sentiment/analyze" \
  -H "Content-Type: application/json" \
  -d '{"text": "Apple beats earnings expectations, stock surges 5%"}' \
  | python3 -m json.tool

# ─── 3. Sentiment — single negative text ─────────────────────────────────────
divider
header "Sentiment — single negative text"
curl -s -X POST "$BASE/sentiment/analyze" \
  -H "Content-Type: application/json" \
  -d '{"text": "Fed raises interest rates aggressively, markets plunge"}' \
  | python3 -m json.tool

# ─── 4. Sentiment — batch ────────────────────────────────────────────────────
divider
header "Sentiment — batch (3 texts)"
curl -s -X POST "$BASE/sentiment/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "texts": [
      "Tesla reports record deliveries, shares jump",
      "Inflation data worse than expected, recession fears grow",
      "OPEC holds production steady amid global uncertainty"
    ]
  }' | python3 -m json.tool

# ─── 5. NER — entity extraction ──────────────────────────────────────────────
divider
header "NER — entity extraction"
curl -s -X POST "$BASE/sentiment/entities" \
  -H "Content-Type: application/json" \
  -d '{"text": "Goldman Sachs upgraded Apple and Microsoft ahead of earnings season"}' \
  | python3 -m json.tool

# ─── 6. Events — Claude event extraction ─────────────────────────────────────
divider
header "Events — extract financial event from text"
curl -s -X POST "$BASE/events/extract" \
  -H "Content-Type: application/json" \
  -d '{
    "text": "The Federal Reserve announced a 25 basis point rate cut on July 2nd 2026, citing slowing inflation and labor market concerns.",
    "source_id": "test-001"
  }' | python3 -m json.tool

# ─── 7. Embeddings — add a document ──────────────────────────────────────────
divider
header "Embeddings — store a document"
curl -s -X POST "$BASE/embeddings/add" \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": "news-001",
    "text": "Apple reported record Q3 revenue of $85 billion driven by iPhone and services growth",
    "metadata": {"source": "reuters", "symbol": "AAPL"}
  }' | python3 -m json.tool

# ─── 8. Embeddings — semantic search ─────────────────────────────────────────
divider
header "Embeddings — semantic search"
curl -s -G "$BASE/embeddings/search" \
  --data-urlencode "q=Apple revenue growth" \
  --data-urlencode "n=3" \
  | python3 -m json.tool

# ─── 9. Prediction — single symbol signal ────────────────────────────────────
divider
header "Prediction — XGBoost signal for AAPL"
curl -s "$BASE/prediction/signals/AAPL" | python3 -m json.tool

# ─── 10. Prediction — batch signals ──────────────────────────────────────────
divider
header "Prediction — batch signals"
curl -s -X POST "$BASE/prediction/batch" \
  -H "Content-Type: application/json" \
  -d '{"symbols": ["AAPL", "MSFT", "TSLA", "NVDA"]}' \
  | python3 -m json.tool

# ─── 11. Prediction — SHAP feature importance ────────────────────────────────
divider
header "Prediction — SHAP values for AAPL"
curl -s "$BASE/prediction/shap/AAPL" | python3 -m json.tool

# ─── 12. Risk — single symbol metrics ────────────────────────────────────────
divider
header "Risk — VaR / CVaR / Sharpe for AAPL"
curl -s "$BASE/risk/metrics/AAPL" | python3 -m json.tool

# ─── 13. Risk — portfolio risk ───────────────────────────────────────────────
divider
header "Risk — portfolio (AAPL 40%, MSFT 35%, TSLA 25%)"
curl -s -X POST "$BASE/risk/portfolio" \
  -H "Content-Type: application/json" \
  -d '{
    "positions": [
      {"symbol": "AAPL", "weight": 0.40},
      {"symbol": "MSFT", "weight": 0.35},
      {"symbol": "TSLA", "weight": 0.25}
    ]
  }' | python3 -m json.tool

# ─── 14. Risk — backtest ─────────────────────────────────────────────────────
divider
header "Risk — buy-and-hold backtest for NVDA"
curl -s -X POST "$BASE/risk/backtest" \
  -H "Content-Type: application/json" \
  -d '{"symbol": "NVDA", "strategy": "buy_and_hold"}' \
  | python3 -m json.tool

divider
echo -e "${GREEN}${BOLD}Done.${RESET}"
