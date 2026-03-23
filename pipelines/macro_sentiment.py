import yfinance as yf
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
from config import TIMEZONE, STALE_THRESHOLDS
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class MacroSentimentPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "macro_sentiment"
    
    def fetch_vix(self):
        try:
            vix = yf.Ticker("^VIX")
            data = vix.history(period="2d")
            current = round(float(data['Close'].iloc[-1]), 2)
            previous = round(float(data['Close'].iloc[-2]), 2)
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2)
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
            vxn = yf.Ticker("^VXN")
            data = vxn.history(period="2d")
            current = round(float(data['Close'].iloc[-1]), 2)
            previous = round(float(data['Close'].iloc[-2]), 2)
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2)
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
            url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            data = response.json()
            score = round(float(data['fear_and_greed']['score']), 1)
            rating = data['fear_and_greed']['rating']
            return {
                'score': score,
                'rating': rating,
                'signal': 'bearish' if score < 40 else 'bullish' if score > 60 else 'neutral'
            }
        except Exception as e:
            error_handler.handle(e, "Fear & Greed")
            return None
    
    def calculate_score(self, vix, vxn, fear_greed):
        score = 0.0
        count = 0
        signal_map = {'bearish': -1, 'neutral': 0, 'bullish': 1}
        
        if vix:
            score += signal_map.get(vix['signal'], 0) * -1
            count += 1
        if vxn:
            score += signal_map.get(vxn['signal'], 0) * -1
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
