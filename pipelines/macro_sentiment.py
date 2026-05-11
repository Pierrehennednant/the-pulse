import json
import os
from datetime import datetime
import pytz
import fear_greed
from config import TIMEZONE, FRED_API_KEY
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

VIX_CACHE_FILE = '/data/vix_cache.json'
FG_CACHE_FILE = '/data/fear_greed_cache.json'

class MacroSentimentPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "macro_sentiment"

    def _save_vix_cache(self, vix_data):
        try:
            atomic_write_json(VIX_CACHE_FILE, {
                'timestamp': datetime.now(self.timezone).isoformat(),
                'vix': vix_data
            })
        except Exception as e:
            pulse_logger.log(f"⚠️ VIX cache write failed: {e}", level="WARNING")

    def _load_vix_cache(self):
        try:
            if os.path.exists(VIX_CACHE_FILE):
                with open(VIX_CACHE_FILE, 'r') as f:
                    return json.load(f).get('vix')
        except Exception:
            pass
        return None

    def fetch_vix(self):
        if not FRED_API_KEY:
            pulse_logger.log("⚠️ FRED_API_KEY not set — skipping VIX fetch", level="WARNING")
            cached = self._load_vix_cache()
            if cached:
                pulse_logger.log("⚠️ VIX — using cached value (FRED key missing)", level="WARNING")
                return cached
            pulse_logger.log("⚠️ VIX — no cache available, defaulting to 20.0", level="WARNING")
            return {'value': 20.0, 'previous': 20.0, 'change': 0, 'change_pct': 0, 'signal': 'neutral'}
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=VIXCLS&api_key={FRED_API_KEY}&sort_order=desc&limit=10&file_type=json"
            response = fetch_with_retry(url, timeout=10)
            data = response.json()
            observations = [o for o in data.get('observations', []) if o['value'] != '.']
            if not observations:
                raise ValueError("No VIX data from FRED")
            current = round(float(observations[0]['value']), 2)
            previous = round(float(observations[1]['value']), 2) if len(observations) >= 2 else current
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            result = {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 20 else 'neutral' if current > 15 else 'bullish'
            }
            self._save_vix_cache(result)
            return result
        except Exception as e:
            error_handler.handle(e, "VIX")
            cached = self._load_vix_cache()
            if cached:
                pulse_logger.log("⚠️ VIX — FRED fetch failed, using cached value", level="WARNING")
                return cached
            pulse_logger.log("⚠️ VIX — FRED fetch failed and no cache, defaulting to 20.0", level="WARNING")
            return {'value': 20.0, 'previous': 20.0, 'change': 0, 'change_pct': 0, 'signal': 'neutral'}

    def fetch_vxn(self):
        if not FRED_API_KEY:
            pulse_logger.log("⚠️ FRED_API_KEY not set — skipping VXN fetch", level="WARNING")
            return None
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=VXNCLS&api_key={FRED_API_KEY}&sort_order=desc&limit=10&file_type=json"
            response = fetch_with_retry(url, timeout=10)
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

    def _save_fg_cache(self, fg_data):
        try:
            atomic_write_json(FG_CACHE_FILE, {
                'timestamp': datetime.now(self.timezone).isoformat(),
                'fear_greed': fg_data
            })
        except Exception as e:
            pulse_logger.log(f"⚠️ Fear & Greed cache write failed: {e}", level="WARNING")

    def _load_fg_cache(self):
        try:
            if os.path.exists(FG_CACHE_FILE):
                with open(FG_CACHE_FILE, 'r') as f:
                    return json.load(f).get('fear_greed')
        except Exception:
            pass
        return None

    def fetch_fear_greed(self):
        try:
            fg = fear_greed.get()
            score = int(fg['score'])
            rating = fg['rating']
            result = {
                'score': score,
                'rating': rating,
                'signal': 'bearish' if score < 40 else 'bullish' if score > 60 else 'neutral',
                'source': 'CNN'
            }
            self._save_fg_cache(result)
            return result
        except Exception as e:
            error_handler.handle(e, "Fear & Greed")
            cached = self._load_fg_cache()
            if cached:
                pulse_logger.log("⚠️ Fear & Greed — CNN fetch failed, using cached value", level="WARNING")
                return cached
            pulse_logger.log("⚠️ Fear & Greed — CNN fetch failed and no cache available", level="WARNING")
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
