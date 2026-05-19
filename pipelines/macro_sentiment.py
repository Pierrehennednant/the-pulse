import json
import math
import os
from datetime import datetime, timezone
import pytz
import yfinance as yf
import fear_greed
from config import TIMEZONE, FRED_API_KEY
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

VIX_CACHE_FILE = '/data/vix_cache.json'
VXN_CACHE_FILE = '/data/vxn_cache.json'
FG_CACHE_FILE = '/data/fear_greed_cache.json'

_YF_STALE_MINUTES = 30

class MacroSentimentPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "macro_sentiment"

    # --- VIX file cache ---

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

    # --- VXN file cache ---

    def _save_vxn_cache(self, vxn_data):
        try:
            atomic_write_json(VXN_CACHE_FILE, {
                'timestamp': datetime.now(self.timezone).isoformat(),
                'vxn': vxn_data
            })
        except Exception as e:
            pulse_logger.log(f"⚠️ VXN cache write failed: {e}", level="WARNING")

    def _load_vxn_cache(self):
        try:
            if os.path.exists(VXN_CACHE_FILE):
                with open(VXN_CACHE_FILE, 'r') as f:
                    return json.load(f).get('vxn')
        except Exception:
            pass
        return None

    # --- yfinance tier ---

    def _fetch_yfinance_index(self, symbol, stale_minutes=_YF_STALE_MINUTES):
        """Fetch index from yfinance. Returns (current, previous) or raises."""
        hist = yf.Ticker(symbol).history(period='5d', interval='1d')
        if hist.empty or len(hist) < 1:
            raise ValueError(f"yfinance returned empty history for {symbol}")
        current = float(hist['Close'].iloc[-1])
        if math.isnan(current):
            raise ValueError(f"yfinance returned NaN for {symbol}")
        # Staleness check on last bar's timestamp
        last_ts = hist.index[-1]
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize('UTC')
        else:
            last_ts = last_ts.tz_convert('UTC')
        age_minutes = (datetime.now(timezone.utc) - last_ts.to_pydatetime()).total_seconds() / 60
        if age_minutes > stale_minutes:
            raise ValueError(f"yfinance {symbol} data is {age_minutes:.0f}m old (>{stale_minutes}m threshold)")
        previous = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else current
        if math.isnan(previous):
            previous = current
        return round(current, 2), round(previous, 2)

    # --- FRED tier ---

    def _fetch_fred_index(self, series_id):
        """Fetch index from FRED. Returns (current, previous) or raises."""
        if not FRED_API_KEY:
            raise ValueError("FRED_API_KEY not set")
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={FRED_API_KEY}"
            f"&sort_order=desc&limit=10&file_type=json"
        )
        response = fetch_with_retry(url, timeout=10)
        data = response.json()
        observations = [o for o in data.get('observations', []) if o['value'] != '.']
        if not observations:
            raise ValueError(f"No {series_id} data from FRED")
        current = round(float(observations[0]['value']), 2)
        previous = round(float(observations[1]['value']), 2) if len(observations) >= 2 else current
        return current, previous

    # --- VIX public fetch ---

    def fetch_vix(self):
        # Tier 1: yfinance
        try:
            current, previous = self._fetch_yfinance_index('^VIX')
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            result = {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 20 else 'neutral' if current > 15 else 'bullish',
                'source': 'yahoo'
            }
            self._save_vix_cache(result)
            return result
        except Exception as e:
            pulse_logger.log(f"⚠️ VIX — yfinance fetch failed ({e}), falling back to FRED", level="WARNING")

        # Tier 2: FRED
        try:
            current, previous = self._fetch_fred_index('VIXCLS')
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            result = {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 20 else 'neutral' if current > 15 else 'bullish',
                'source': 'fred'
            }
            self._save_vix_cache(result)
            return result
        except Exception as e:
            error_handler.handle(e, "VIX FRED")

        # Tier 3: file cache
        cached = self._load_vix_cache()
        if cached:
            pulse_logger.log("⚠️ VIX — both sources failed, using file cache", level="WARNING")
            cached['source'] = 'cache'
            return cached

        # Tier 4: default
        pulse_logger.log("⚠️ VIX — all sources failed, defaulting to 20.0", level="WARNING")
        return {'value': 20.0, 'previous': 20.0, 'change': 0, 'change_pct': 0, 'signal': 'neutral', 'source': 'default'}

    # --- VXN public fetch ---

    def fetch_vxn(self):
        # Tier 1: yfinance
        try:
            current, previous = self._fetch_yfinance_index('^VXN')
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            result = {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 25 else 'neutral' if current > 18 else 'bullish',
                'source': 'yahoo'
            }
            self._save_vxn_cache(result)
            return result
        except Exception as e:
            pulse_logger.log(f"⚠️ VXN — yfinance fetch failed ({e}), falling back to FRED", level="WARNING")

        # Tier 2: FRED
        try:
            current, previous = self._fetch_fred_index('VXNCLS')
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            result = {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 25 else 'neutral' if current > 18 else 'bullish',
                'source': 'fred'
            }
            self._save_vxn_cache(result)
            return result
        except Exception as e:
            error_handler.handle(e, "VXN FRED")

        # Tier 3: file cache
        cached = self._load_vxn_cache()
        if cached:
            pulse_logger.log("⚠️ VXN — both sources failed, using file cache", level="WARNING")
            cached['source'] = 'cache'
            return cached

        # Tier 4: default
        pulse_logger.log("⚠️ VXN — all sources failed, defaulting to 20.0", level="WARNING")
        return {'value': 20.0, 'previous': 20.0, 'change': 0, 'change_pct': 0, 'signal': 'neutral', 'source': 'default'}

    # --- Fear & Greed ---

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
