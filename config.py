# The Pulse Configuration

# API Keys and Endpoints
YAHOO_FINANCE_API = "yfinance"
CNN_FEAR_GREED_URL = "https://www.cnn.com/markets/fear-and-greed"
CFTC_COT_URL = "https://www.cftc.gov/dea/futures/financial_lf.htm"
FINANCIAL_JUICE_URL = "https://www.financialjuice.com/home"
UNBIASED_NETWORK_URL = "https://www.unbiasednetwork.com/episodes"

# Timezone
TIMEZONE = "US/Eastern"

# Refresh Rate
REFRESH_INTERVAL_MINUTES = 5

# Pillar Weights
PILLAR_WEIGHTS = {
    "economic_calendar": 40,
    "geopolitical": 28,
    "institutional": 20,
    "macro_sentiment": 10,
    "news_sentiment": 2
}

# Stale Data Thresholds (minutes)
STALE_THRESHOLDS = {
    "economic_calendar": 1440,
    "macro_sentiment": 30,
    "geopolitical": 120,
    "news_sentiment": 60
}

# Bias Score Range
MIN_BIAS_SCORE = -2.0
MAX_BIAS_SCORE = 2.0

# COT Update Day
COT_UPDATE_DAY = "Friday"

# Sentiment Model
SENTIMENT_MODEL = "distilbert-base-uncased-finetuned-sst-2-english"

# Cache Settings
CACHE_DIR = "./data"
COT_CACHE_FILE = "./data/weekly_cot.json"
