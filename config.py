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

# Pillar Weights
PILLAR_WEIGHTS = {
    "economic_calendar": 30,
    "geopolitical": 25,
    "institutional": 25,
    "macro_sentiment": 20
}

# Sentiment Model
SENTIMENT_MODEL = "distilbert-base-uncased-finetuned-sst-2-english"
