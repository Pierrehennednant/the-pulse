import json
import os
import time
from datetime import datetime, timedelta, timezone
import pytz
import fear_greed
import yfinance as yf
from config import TIMEZONE, FRED_API_KEY
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

VIX_CACHE_FILE = '/data/vix_cache.json'
VXN_CACHE_FILE = '/data/vxn_cache.json'
FG_CACHE_FILE = '/data/fear_greed_cache.json'
VIX_SESSION_FILE = '/data/vix_intraday_session.json'
VXN_SESSION_FILE = '/data/vxn_intraday_session.json'

# Scheduled intraday pull slots (ET) — main.py registers one scheduled job per slot.
INTRADAY_SLOTS = ["09:40", "09:45", "09:55", "10:30"]
# 09:40 and 10:30 always attempt a live yfinance pull. 09:45 and 09:55 are
# conditional — they only attempt yfinance if no same-day slot has yet
# captured a real live value; otherwise they skip the live call and just
# carry the existing value forward via the session-cache fallback path.
ALWAYS_LIVE_SLOTS = {"09:40", "10:30"}
VALID_RANGE = (5.0, 100.0)  # sane bounds for both VIX and VXN

class MacroSentimentPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "macro_sentiment"

    # -- FRED cache (Level-3 fallback: prior-day close) --------------------

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
                    return json.load(f)  # returns {'timestamp': ..., 'vix': {...}}
        except Exception:
            pass
        return None

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
                    return json.load(f)  # returns {'timestamp': ..., 'vxn': {...}}
        except Exception:
            pass
        return None

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

    @staticmethod
    def _vix_signal(value):
        return (
            'strongly_bullish' if value < 15.0 else
            'mildly_bullish' if value < 17.0 else
            'neutral' if value < 20.0 else
            'mildly_bearish' if value < 25.0 else
            'strongly_bearish'
        )

    @staticmethod
    def _vxn_signal(value):
        return (
            'strongly_bullish' if value < 18.0 else
            'mildly_bullish' if value < 20.0 else
            'neutral' if value < 25.0 else
            'mildly_bearish' if value < 28.0 else
            'strongly_bearish'
        )

    def daily_fred_refresh(self):
        """Runs once daily (4:35 PM ET, scheduled from main.py) purely to keep the
        Level-3 FRED fallback cache fresh. Independent of the intraday yfinance
        pulls — without this, vix_cache.json/vxn_cache.json would never update
        again once yfinance became the primary source, and the prior-day-close
        fallback would silently rot."""
        for symbol, series_id, saver, signal_fn in (
            ('VIX', 'VIXCLS', self._save_vix_cache, self._vix_signal),
            ('VXN', 'VXNCLS', self._save_vxn_cache, self._vxn_signal),
        ):
            try:
                current, previous = self._fetch_fred_index(series_id)
                change = round(current - previous, 2)
                change_pct = round((change / previous) * 100, 2) if previous else 0
                result = {
                    'value': current,
                    'previous': previous,
                    'change': change,
                    'change_pct': change_pct,
                    'signal': signal_fn(current),
                    'source': 'fred'
                }
                saver(result)
                pulse_logger.log(f"✓ {symbol} — daily FRED refresh: {current} (prev: {previous}, chg: {change:+.2f})")
            except Exception as e:
                pulse_logger.log(f"⚠️ {symbol} — daily FRED refresh failed: {e}", level="WARNING")

    # -- Intraday session cache (Level-1/2: today's yfinance pulls) --------

    def _current_trading_day_str(self):
        """Date string of the current/most recent trading day. Weekends
        roll back to Friday — VIX/VXN don't trade Sat/Sun, so no new
        session should start (or the prior day's frozen value get wiped)
        until markets reopen Monday."""
        now = datetime.now(self.timezone)
        if now.weekday() == 5:      # Saturday
            now -= timedelta(days=1)
        elif now.weekday() == 6:    # Sunday
            now -= timedelta(days=2)
        return now.strftime('%Y-%m-%d')

    def _empty_session(self):
        return {
            'date': self._current_trading_day_str(),
            'slots': {s: None for s in INTRADAY_SLOTS},
            'consecutive_failures': 0,
            'intraday_warning': False,
            'frozen': False,
            'frozen_value': None,
            'frozen_at': None,
        }

    def _load_session(self, path):
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    data = json.load(f)
                if data.get('date') == self._current_trading_day_str():
                    return data
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load intraday session {path}: {e}", level="WARNING")
        return self._empty_session()

    def _save_session(self, path, data):
        try:
            atomic_write_json(path, data)
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to save intraday session {path}: {e}", level="WARNING")

    def _yfinance_pull(self, ticker_symbol, label):
        """Attempt a yfinance pull with 2 retries (3s, then 5s backoff).
        Returns a validated float in VALID_RANGE, or None if all 3 attempts fail."""
        delays = [0, 3, 5]
        for attempt, delay in enumerate(delays, start=1):
            if delay:
                time.sleep(delay)
            try:
                t = yf.Ticker(ticker_symbol)
                value = t.fast_info['last_price']
                if value is None:
                    hist = t.history(period='1d')
                    if not hist.empty:
                        value = float(hist['Close'].iloc[-1])
                if value is None:
                    raise ValueError("no price returned")
                value = float(value)
                if value != value:  # NaN check
                    raise ValueError("NaN value")
                if not (VALID_RANGE[0] <= value <= VALID_RANGE[1]):
                    raise ValueError(f"value {value} outside sane range {VALID_RANGE}")
                return round(value, 2)
            except Exception as e:
                pulse_logger.log(f"⚠️ {label} — yfinance attempt {attempt}/3 failed: {e}", level="WARNING")
        return None

    def _resolve_slot_fallback(self, session, current_slot_idx):
        """Most recent successful/fallback same-day slot before current_slot_idx."""
        for i in range(current_slot_idx - 1, -1, -1):
            slot = INTRADAY_SLOTS[i]
            entry = session['slots'].get(slot)
            if entry:
                return slot, entry
        return None, None

    def _apply_fallback(self, session, slot_idx, symbol, fred_cache_loader, slot, reason, is_skip=False):
        """Resolve this slot's value via Level-2 (same-day session-cache lookback)
        then Level-3 (FRED prior-day close) fallback, and log the outcome.
        Shared by both the real-failure path and the conditional-skip path in
        run_scheduled_pull() — the resolution logic is identical, only the
        leading `reason` phrase and log symbol differ.

        is_skip=True means this is the deliberate conditional-skip case (a prior
        slot already captured a live value today) — a success-path outcome, not
        a failure, so it logs with "↺" rather than "⚠️". is_skip=False means a
        real yfinance attempt failed, which is a genuine fallback and keeps "⚠️"."""
        symbol_emoji = "↺" if is_skip else "⚠️"
        fb_slot, fb_entry = self._resolve_slot_fallback(session, slot_idx)
        if fb_entry:
            session['slots'][slot] = {
                'value': fb_entry['value'],
                'timestamp': fb_entry['timestamp'],
                'source': f'session-cache ({fb_slot})'
            }
            pulse_logger.log(
                f"{symbol_emoji} {symbol} — {slot} {reason}, using {fb_slot} cached data "
                f"({fb_entry['value']}, from {fb_entry['timestamp']})"
            )
            return True
        fred_file = fred_cache_loader()
        fred_key = 'vix' if symbol == 'VIX' else 'vxn'
        if fred_file and fred_file.get(fred_key, {}).get('value') is not None:
            fred_val = fred_file[fred_key]['value']
            fred_ts = fred_file.get('timestamp', '')
            session['slots'][slot] = {
                'value': fred_val,
                'timestamp': fred_ts,
                'source': 'fred-prior-day'
            }
            # Always ⚠️ here — reaching FRED means no live value exists today at
            # all, a genuine fallback regardless of which path (skip or real
            # failure) got us here.
            pulse_logger.log(
                f"⚠️ {symbol} — {slot} {reason}, no live data today, using FRED prior-day close "
                f"({fred_val}, from {fred_ts})"
            )
            return True
        pulse_logger.log(f"⚠️ {symbol} — {slot} {reason} and no fallback available", level="WARNING")
        return False

    def run_scheduled_pull(self, slot):
        """Called by the 4 scheduled jobs (9:40/9:45/9:55/10:30 ET, registered in
        main.py).

        9:40 and 10:30 always attempt a live yfinance pull. 9:45 and 9:55 are
        conditional: each only calls yfinance if no same-day slot has yet
        captured a real live value (i.e. the prior attempt(s) failed) — if a
        live value already exists today, the slot skips the API call entirely
        and just carries that value forward via the session-cache fallback.

        Falls back per the documented hierarchy (session-cache -> FRED
        prior-day close), tracks consecutive REAL failures for the UI warning
        (skipped slots never count as failures), and freezes the day's
        reading after the 10:30 slot."""
        if slot not in INTRADAY_SLOTS:
            pulse_logger.log(f"⚠️ Unknown intraday slot '{slot}' — skipping", level="WARNING")
            return
        if datetime.now(self.timezone).weekday() >= 5:
            pulse_logger.log(f"↺ {slot} intraday pull skipped — market closed (weekend)")
            return
        slot_idx = INTRADAY_SLOTS.index(slot)
        always_live = slot in ALWAYS_LIVE_SLOTS

        for symbol, ticker, session_file, fred_cache_loader in (
            ('VIX', '^VIX', VIX_SESSION_FILE, self._load_vix_cache),
            ('VXN', '^VXN', VXN_SESSION_FILE, self._load_vxn_cache),
        ):
            session = self._load_session(session_file)
            already_live_today = any(
                entry and entry.get('source') == 'yfinance'
                for entry in session['slots'].values()
            )
            attempt_live = always_live or not already_live_today

            if not attempt_live:
                # Conditional slot, prior slot already captured a real live
                # value today — skip the yfinance call, no effect on
                # consecutive_failures (this is not a failure).
                self._apply_fallback(session, slot_idx, symbol, fred_cache_loader, slot,
                                      reason="skipped (live value already captured earlier today)",
                                      is_skip=True)
            else:
                value = self._yfinance_pull(ticker, f"{symbol} {slot}")
                if value is not None:
                    session['slots'][slot] = {
                        'value': value,
                        'timestamp': datetime.now(self.timezone).isoformat(),
                        'source': 'yfinance'
                    }
                    session['consecutive_failures'] = 0
                    pulse_logger.log(f"✓ {symbol} — yfinance {slot} pull: {value}")
                else:
                    session['consecutive_failures'] = session.get('consecutive_failures', 0) + 1
                    self._apply_fallback(session, slot_idx, symbol, fred_cache_loader, slot,
                                          reason="pull failed")

            session['intraday_warning'] = session.get('consecutive_failures', 0) >= 2

            if slot == '10:30':
                resolved = session['slots'].get('10:30')
                if resolved:
                    session['frozen'] = True
                    session['frozen_value'] = resolved['value']
                    session['frozen_at'] = datetime.now(self.timezone).isoformat()
                    pulse_logger.log(
                        f"🧊 {symbol} — frozen for the day at {resolved['value']} "
                        f"(source: {resolved['source']})"
                    )

            self._save_session(session_file, session)

    def _read_intraday_value(self, symbol, session_file, fred_cache_loader, signal_fn):
        """Passive read for the normal 5-minute refresh cycle — never calls
        yfinance or FRED live. Reports today's frozen value, or the most recent
        successful/fallback slot so far, or (before any slot has run) FRED's
        prior-day close."""
        session = self._load_session(session_file)
        fred_file = fred_cache_loader()
        fred_key = 'vix' if symbol == 'VIX' else 'vxn'
        previous = fred_file.get(fred_key, {}).get('value') if fred_file else None

        if session.get('frozen'):
            value = session['frozen_value']
            source = 'frozen'
        else:
            value = None
            source = None
            for slot in reversed(INTRADAY_SLOTS):
                entry = session['slots'].get(slot)
                if entry:
                    value = entry['value']
                    source = entry['source']
                    break
            if value is None:
                if previous is not None:
                    value = previous
                    source = 'fred-prior-day'
                else:
                    value = 20.0
                    source = 'default'

        is_stale = False
        if source == 'fred-prior-day' and fred_file:
            ts = fred_file.get('timestamp', '')
            if ts:
                try:
                    cached_dt = datetime.fromisoformat(ts)
                    if cached_dt.tzinfo is None:
                        cached_dt = cached_dt.replace(tzinfo=self.timezone)
                    age_days = (datetime.now(self.timezone) - cached_dt).total_seconds() / 86400
                    is_stale = age_days >= 3
                except Exception:
                    pass

        change = round(value - previous, 2) if previous is not None else 0
        change_pct = round((change / previous) * 100, 2) if previous else 0

        return {
            'value': value,
            'previous': previous if previous is not None else value,
            'change': change,
            'change_pct': change_pct,
            'signal': signal_fn(value),
            'source': source,
            'stale': is_stale,
            'intraday_warning': session.get('intraday_warning', False),
        }

    def fetch_vix(self):
        return self._read_intraday_value('VIX', VIX_SESSION_FILE, self._load_vix_cache, self._vix_signal)

    def fetch_vxn(self):
        return self._read_intraday_value('VXN', VXN_SESSION_FILE, self._load_vxn_cache, self._vxn_signal)

    # -- Fear & Greed --------------------------------------------------------

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
            score = int(round(fg['score']))
            rating = fg['rating']
            pc = fg.get('indicators', {}).get('put_call_options', {})
            result = {
                'score': score,
                'rating': rating,
                'signal': (
                    'strongly_bearish' if score < 25 else
                    'mildly_bearish'   if score < 45 else
                    'neutral'          if score < 55 else
                    'mildly_bullish'   if score < 75 else
                    'strongly_bullish'
                ),
                'source': 'CNN',
                'put_call_score': int(round(pc['score'])) if pc.get('score') is not None else None,
                'put_call_rating': pc.get('rating'),
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
        signal_map = {
            'strongly_bullish': 1.0,
            'mildly_bullish': 0.5,
            'neutral': 0.0,
            'mildly_bearish': -0.5,
            'strongly_bearish': -1.0,
        }
        if vix:
            if vix.get('stale'):
                pulse_logger.log("📉 VIX excluded from macro score — stale data (≥3 days old)")
            else:
                score += signal_map.get(vix['signal'], 0)
                count += 1
        if vxn:
            if vxn.get('stale'):
                pulse_logger.log("📉 VXN excluded from macro score — stale data (≥3 days old)")
            else:
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
                'intraday_warning': bool(vix.get('intraday_warning') or vxn.get('intraday_warning')),
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
