import os

# The Pulse Configuration

# API Keys — set these as environment variables in Railway (and optionally locally via .env)
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
THENEWS_API_KEY = os.environ.get("THENEWS_API_KEY", "")

# Endpoints
CFTC_COT_URL = "https://www.cftc.gov/dea/futures/financial_lf.htm"

# Timezone
TIMEZONE = "US/Eastern"

# Refresh Rate
REFRESH_INTERVAL_MINUTES = 5
GEO_REFRESH_INTERVAL_MINUTES = 3

# Pillar Weights
PILLAR_WEIGHTS = {
    "economic_calendar": 40,
    "geopolitical": 30,
    "institutional": 20,
    "macro_sentiment": 10
}

# Stale Data Thresholds (minutes)
STALE_THRESHOLDS = {
    "economic_calendar": 1440,
    "macro_sentiment": 30,
    "geopolitical": 120
}

# Bias Score Range
MIN_BIAS_SCORE = -2.0
MAX_BIAS_SCORE = 2.0

# COT Update Day
COT_UPDATE_DAY = "Friday"

# Sentiment Model
SENTIMENT_MODEL = "distilbert-base-uncased-finetuned-sst-2-english"

# Cache Settings
CACHE_DIR = "/data"
