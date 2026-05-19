import json
import os
from datetime import datetime, timezone
import pytz
import fear_greed
from config import TIMEZONE, FRED_API_KEY, POLYGON_API_KEY
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

VIX_CACHE_FILE = '/data/vix_cache.json'
VXN_CACHE_FILE = '/data/vxn_cache.json'
FG_CACHE_FILE = '/data/fear_greed_cache.json'

_POLYGON_STALE_MINUTES = 30

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

    # --- Polygon.io tier ---

    def _fetch_polygon_index(self, ticker, stale_minutes=_POLYGON_STALE_MINUTES):
        """Fetch index snapshot from Polygon.io. Returns (current, previous) or raises."""
        if not POLYGON_API_KEY:
            raise ValueError("POLYGON_API_KEY not set")
        url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&apiKey={POLYGON_API_KEY}"
        response = fetch_with_retry(url, timeout=10)
        data = response.json()
        results = data.get('results', [])
        if not results:
            raise ValueError(f"No Polygon snapshot results for {ticker}")
        snap = results[0]
        session = snap.get('session', {})
        current = round(float(session['close']), 2)
        previous = round(float(session.get('previous_close', current)), 2)
        # Staleness check — last_updated is nanoseconds epoch
        last_updated_ns = snap.get('last_updated')
        if last_updated_ns:
            last_updated_s = last_updated_ns / 1e9
            age_minutes = (datetime.now(timezone.utc).timestamp() - last_updated_s) / 60
            if age_minutes > stale_minutes:
                raise ValueError(f"Polygon {ticker} data is {age_minutes:.0f}m old (>{stale_minutes}m threshold)")
        return current, previous

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
        # Tier 1: Polygon.io
        try:
            current, previous = self._fetch_polygon_index('I:VIX')
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            result = {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 20 else 'neutral' if current > 15 else 'bullish',
                'source': 'polygon'
            }
            self._save_vix_cache(result)
            return result
        except Exception as e:
            pulse_logger.log(f"⚠️ VIX — Polygon fetch failed ({e}), falling back to FRED", level="WARNING")

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
            cached['source'] = cached.get('source', 'cache')
            cached['source'] = 'cache'
            return cached

        # Tier 4: default
        pulse_logger.log("⚠️ VIX — all sources failed, defaulting to 20.0", level="WARNING")
        return {'value': 20.0, 'previous': 20.0, 'change': 0, 'change_pct': 0, 'signal': 'neutral', 'source': 'default'}

    # --- VXN public fetch ---

    def fetch_vxn(self):
        # Tier 1: Polygon.io
        try:
            current, previous = self._fetch_polygon_index('I:VXN')
            change = round(current - previous, 2)
            change_pct = round((change / previous) * 100, 2) if previous else 0
            result = {
                'value': current,
                'previous': previous,
                'change': change,
                'change_pct': change_pct,
                'signal': 'bearish' if current > 25 else 'neutral' if current > 18 else 'bullish',
                'source': 'polygon'
            }
            self._save_vxn_cache(result)
            return result
        except Exception as e:
            pulse_logger.log(f"⚠️ VXN — Polygon fetch failed ({e}), falling back to FRED", level="WARNING")

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
