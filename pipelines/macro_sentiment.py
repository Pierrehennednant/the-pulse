import requests
from datetime import datetime
import pytz
from config import TIMEZONE, FRED_API_KEY
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class MacroSentimentPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "macro_sentiment"

    def fetch_vix(self):
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=VIXCLS&api_key={FRED_API_KEY}&sort_order=desc&limit=10&file_type=json"
            response = requests.get(url, timeout=10)
            data = response.json()
            observations = [o for o in data.get('observations', []) if o['value'] != '.']
            if not observations:
                raise ValueError("No VIX data from FRED")
            current = round(float(observations[0]['value']), 2)
            previous = round(float(observations[1]['value']), 2) if len(observations) >= 2 else current
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            return {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 20 else 'neutral' if current > 15 else 'bullish'
            }
        except Exception as e:
            error_handler.handle(e, "VIX")
            return None

    def fetch_vxn(self):
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=VXNCLS&api_key={FRED_API_KEY}&sort_order=desc&limit=10&file_type=json"
            response = requests.get(url, timeout=10)
            data = response.json()
            observations = [o for o in data.get('observations', []) if o['value'] != '.']
            if not observations:
                raise ValueError("No VXN data from FRED")
            current = round(float(observations[0]['value']), 2)
            previous = round(float(observations[1]['value']), 2) if len(observations) >= 2 else current
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            return {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 25 else 'neutral' if current > 18 else 'bullish'
            }
        except Exception as e:
            error_handler.handle(e, "VXN")
            return None

    def fetch_fear_greed(self):
        try:
            url = "https://api.alternative.me/fng/?limit=1"
            response = requests.get(url, timeout=10)
            data = response.json()
            score = int(data['data'][0]['value'])
            rating = data['data'][0]['value_classification']
            return {
                'score': score,
                'rating': rating,
                'signal': 'bearish' if score < 40 else 'bullish' if score > 60 else 'neutral',
                'source': 'Alternative.me'
            }
        except Exception as e:
            error_handler.handle(e, "Fear & Greed")
            return None

    def calculate_score(self, vix, vxn, fear_greed):
        score = 0.0
        count = 0
        signal_map = {'bearish': -1, 'neutral': 0, 'bullish': 1}
        if vix:
            score += signal_map.get(vix['signal'], 0)
            count += 1
        if vxn:
            score += signal_map.get(vxn['signal'], 0)
            count += 1
        if fear_greed:
            score += signal_map.get(fear_greed['signal'], 0)
            count += 1
        return round(score / count if count > 0 else 0, 2)

    def fetch(self):
        try:
            vix = self.fetch_vix()
            vxn = self.fetch_vxn()
            fear_greed = self.fetch_fear_greed()
            score = self.calculate_score(vix, vxn, fear_greed)
            result = {
                'pillar': 'macro_sentiment',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'vix': vix,
                'vxn': vxn,
                'fear_greed': fear_greed,
                'pillar_score': score,
                'status': 'live'
            }
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ Macro Sentiment updated | Score: {score}")
            return result
        except Exception as e:
            error_handler.handle(e, "Macro Sentiment")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return cached['data']
            return None

macro_sentiment_pipeline = MacroSentimentPipeline()
