import os

# The Pulse Configuration

# API Keys — set these as environment variables in Railway (and optionally locally via .env)
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
THENEWS_API_KEY = os.environ.get("THENEWS_API_KEY", "")
GROK_API_KEY = os.environ.get("GROK_API_KEY", "")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
SECRET_KEY = os.environ.get("SECRET_KEY", "")

# Timezone
TIMEZONE = "US/Eastern"

# Refresh Rate
REFRESH_INTERVAL_MINUTES = 5

# Pillar Weights — single source of truth (Finding 2+6, Pierre 2026-10-26:
# quiet-week EC 15 in the engine, bar stays 0.30)
PILLAR_WEIGHTS = {
    "economic_calendar": 30,
    "geopolitical": 25,
    "institutional": 12,
    "macro_sentiment": 20
}

# COT pillar score cap, previously hardcoded in bias_calculator.py
COT_SCORE_CAP = 0.6

# Quiet-week EC weight, previously only in recommendation._WEEK_WEIGHTS
QUIET_WEEK_EC_WEIGHT = 15

# Sentiment Model
SENTIMENT_MODEL = "distilbert-base-uncased-finetuned-sst-2-english"
