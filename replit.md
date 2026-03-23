# The Pulse - Macro Regime Trading Dashboard

## Overview
A real-time macro regime trading dashboard that aggregates data from multiple sources to calculate a directional market bias (Bullish/Bearish/Neutral) with confidence scoring.

## Architecture
- **Flask** web server on port 5000
- **5 Data Pillars** with weighted scoring:
  - Economic Calendar (40%) - Forex Factory red folder events
  - Geopolitical (28%) - Financial Juice & Unbiased Network with NLP sentiment
  - Institutional (20%) - CFTC COT report (weekly, Fridays)
  - Macro Sentiment (10%) - VIX, VXN, CNN Fear & Greed Index
  - News Sentiment (2%) - AI synthesis of geopolitical headlines

## Project Structure
```
config.py              - Configuration (weights, URLs, thresholds)
main.py                - Orchestrator (scheduler + Flask startup)
pipelines/             - Data fetching pipelines per pillar
processors/            - Data formatting, bias calculation, snapshots
ui/                    - Flask routes + dashboard HTML template
utils/                 - Logger, cache, error handler
data/snapshots/        - JSON snapshot storage
```

## Key Dependencies
- flask, schedule, yfinance, requests, beautifulsoup4
- transformers (v4.37.0), torch (CPU-only), numpy (<2)
- pytz

## Running
`python main.py` starts the scheduler and Flask dashboard on port 5000.
Auto-refreshes every 5 minutes. Snapshots are saved to `data/snapshots/`.
