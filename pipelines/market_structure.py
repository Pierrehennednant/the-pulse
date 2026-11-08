import json
import os
from datetime import datetime, time, timedelta

import pytz
import yfinance as yf

from config import TIMEZONE
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger

STRUCTURE_CACHE_FILE = '/data/structure_cache.json'
# The dashboard's own refresh cycle is 5 minutes; a 4-minute server-side
# cache keeps Yahoo hits at roughly one fetch per cycle no matter how
# often the page is opened, while never serving a read older than a cycle.
CACHE_TTL_SECONDS = 240

SESSION_START = time(18, 0)   # Globex session opens 18:00 ET


class MarketStructurePipeline:
    """Descriptive market-structure readouts for NQ and ES futures:
    nearest 4H fair value gaps above/below price, prior day high/low with
    price's position relative to that range, and — on Mondays — the
    weekend gap as levels (top/bottom, filled or still open).
    Levels and gaps only — no scoring, no thresholds, nothing here feeds
    the bias calculation."""

    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)

    # -- Cache --------------------------------------------------------------

    def _load_cache(self):
        try:
            if os.path.exists(STRUCTURE_CACHE_FILE):
                with open(STRUCTURE_CACHE_FILE) as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def _save_cache(self, data):
        try:
            atomic_write_json(STRUCTURE_CACHE_FILE, data)
        except Exception as e:
            pulse_logger.log(f"⚠️ Structure cache write failed: {e}", level="WARNING")

    def _cache_is_fresh(self, cached):
        if not cached or not cached.get('timestamp'):
            return False
        try:
            ts = datetime.fromisoformat(cached['timestamp'])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=self.timezone)
            age = (datetime.now(self.timezone) - ts).total_seconds()
            return 0 <= age < CACHE_TTL_SECONDS
        except Exception:
            return False

    # -- Yahoo bars -----------------------------------------------------------

    def _fetch_bars(self, symbol, period, interval):
        """Intraday bars as a list of (datetime ET, open, high, low, close),
        oldest first. Reuses the codebase's yfinance access pattern
        (yf.Ticker(...).history, as in macro_sentiment). Returns [] on any
        failure — callers treat missing bars as unavailable, never as zero."""
        try:
            df = yf.Ticker(symbol).history(period=period, interval=interval)
            if df is None or df.empty:
                return []
            idx = df.index
            if idx.tz is None:
                idx = idx.tz_localize('America/New_York')
            idx = idx.tz_convert(self.timezone)
            bars = []
            for ts, (o, h, l, c) in zip(idx, df[['Open', 'High', 'Low', 'Close']].values.tolist()):
                o, h, l, c = float(o), float(h), float(l), float(c)
                if o != o or h != h or l != l or c != c:  # NaN guard
                    continue
                bars.append((ts.to_pydatetime(), o, h, l, c))
            bars.sort(key=lambda b: b[0])
            return bars
        except Exception as e:
            pulse_logger.log(f"⚠️ Structure — yfinance {symbol} {interval} fetch failed: {e}", level="WARNING")
            return []

    # -- Prior day levels + weekend gap -----------------------------------------

    def _session_start(self, now):
        """Start of the current Globex session: today 18:00 ET if it has
        begun, otherwise yesterday 18:00 ET. On a Monday this is Sunday
        18:00 — the weekend session open."""
        if now.hour >= 18:
            start_date = now.date()
        else:
            start_date = now.date() - timedelta(days=1)
        return self.timezone.localize(datetime.combine(start_date, SESSION_START))

    @staticmethod
    def _prior_day_position(last_price, pd_high, pd_low):
        """Where price sits relative to the prior day's range, in words.
        Descriptive only — the four plain-language positions."""
        if last_price > pd_high:
            return 'Above prior day high'
        if last_price < pd_low:
            return 'Below prior day low'
        if pd_high > pd_low and (last_price - pd_low) / (pd_high - pd_low) >= 0.5:
            return 'Upper half of prior day range'
        return 'Lower half of prior day range'

    def _symbol_levels(self, symbol):
        """Prior day high/low + price position, and (Mondays) the weekend
        gap levels, from 5-minute bars."""
        bars = self._fetch_bars(symbol, period='5d', interval='5m')
        if not bars:
            return None
        now = datetime.now(self.timezone)
        start = self._session_start(now)
        last_price = bars[-1][4]

        result = {
            'symbol': symbol,
            'last_price': round(last_price, 2),
            'prior_day': None,
            'weekend_gap': None,
        }

        # Prior day: most recent ET date (starting from the session's
        # start date, walking back) that has bars before the 18:00 Globex
        # open — that date's pre-18:00 high/low. On a Monday this walks
        # Sunday → Saturday → Friday on its own.
        by_date = {}
        for b in bars:
            if b[0].time() < SESSION_START:
                by_date.setdefault(b[0].date(), []).append(b)
        for d in sorted(by_date.keys(), reverse=True):
            if d <= start.date():
                day_bars = by_date[d]
                pd_high = max(b[2] for b in day_bars)
                pd_low = min(b[3] for b in day_bars)
                result['prior_day'] = {
                    'date': d.isoformat(),
                    'high': round(pd_high, 2),
                    'low': round(pd_low, 2),
                    'position': self._prior_day_position(last_price, pd_high, pd_low),
                }
                break

        # Weekend gap — Mondays only, as structure levels: the band
        # between Friday's close (last close before the Sunday 18:00 open)
        # and the Sunday open, and whether price has traded back through
        # the Friday close since the open (filled) or not (still open).
        if now.weekday() == 0:
            before = [b for b in bars if b[0] < start]
            session = [b for b in bars if b[0] >= start]
            if before and session:
                friday_close = before[-1][4]
                sunday_open = session[0][1]
                if abs(sunday_open - friday_close) >= 0.25:
                    if sunday_open > friday_close:
                        filled = any(b[3] <= friday_close for b in session)
                    else:
                        filled = any(b[2] >= friday_close for b in session)
                    result['weekend_gap'] = {
                        'top': round(max(friday_close, sunday_open), 2),
                        'bottom': round(min(friday_close, sunday_open), 2),
                        'size_pts': round(abs(sunday_open - friday_close), 2),
                        'filled': filled,
                    }
        return result

    # -- 4H fair value gaps ------------------------------------------------------

    @staticmethod
    def _four_h_key(dt_et):
        """Bucket an ET datetime into its 4H candle: (cycle date, block).
        The cycle starts at 18:00 ET wall-clock, so the key is DST-safe
        (the once-a-year repeated 01:00 hour lands in the same block)."""
        minutes = dt_et.hour * 60 + dt_et.minute
        anchor = 18 * 60
        since_anchor = (minutes - anchor) % (24 * 60)
        block = since_anchor // 240
        cycle_date = dt_et.date() if minutes >= anchor else dt_et.date() - timedelta(days=1)
        return (cycle_date, block)

    def _build_4h_candles(self, bars):
        candles = []
        current = None
        for dt_et, o, h, l, c in bars:
            key = self._four_h_key(dt_et)
            if current is None or current['key'] != key:
                current = {'key': key, 'open': o, 'high': h, 'low': l, 'close': c}
                candles.append(current)
            else:
                current['high'] = max(current['high'], h)
                current['low'] = min(current['low'], l)
                current['close'] = c
        return candles

    def _fvg_4h(self, symbol, last_price):
        """Nearest 4H fair value gaps around price for one symbol, from
        1-hour bars aggregated into 4H candles anchored at 18:00 ET.
        Standard 3-candle definition: a gap exists where candle i+1's
        range does not overlap candle i-1's."""
        # NOTE: yfinance returns an empty frame for futures 1h bars with
        # period='1mo'; period='60d' works (verified 2026-11-08).
        bars = self._fetch_bars(symbol, period='60d', interval='1h')
        if len(bars) < 12 or last_price is None:
            return None
        cutoff = bars[-1][0] - timedelta(days=21)
        candles = self._build_4h_candles([b for b in bars if b[0] >= cutoff])
        if len(candles) < 3:
            return None
        gaps = []
        for i in range(1, len(candles) - 1):
            left, right = candles[i - 1], candles[i + 1]
            if right['low'] > left['high']:        # bullish FVG
                gaps.append({'bottom': left['high'], 'top': right['low']})
            elif right['high'] < left['low']:      # bearish FVG
                gaps.append({'bottom': right['high'], 'top': left['low']})
        above = below = inside = None
        for g in gaps:
            entry = {
                'top': round(g['top'], 2),
                'bottom': round(g['bottom'], 2),
                'size_pts': round(g['top'] - g['bottom'], 2),
            }
            if g['bottom'] <= last_price <= g['top']:
                inside = entry
            elif g['bottom'] > last_price:
                if above is None or g['bottom'] < above['bottom']:
                    above = entry
            else:  # g['top'] < last_price
                if below is None or g['top'] > below['top']:
                    below = entry
        return {'above': above, 'below': below, 'inside': inside}

    # -- Entry point ------------------------------------------------------------

    def fetch(self, force=False):
        cached = self._load_cache()
        if not force and self._cache_is_fresh(cached):
            return cached
        try:
            nq = self._symbol_levels('NQ=F')
            es = self._symbol_levels('ES=F')
            if nq is None and es is None:
                raise ValueError("no bars for NQ or ES")
            if nq:
                nq['fvg_4h'] = self._fvg_4h('NQ=F', nq['last_price'])
            if es:
                es['fvg_4h'] = self._fvg_4h('ES=F', es['last_price'])
            data = {
                'status': 'live',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'nq': nq,
                'es': es,
            }
            self._save_cache(data)
            fvg = (nq or {}).get('fvg_4h') or {}
            pulse_logger.log(
                f"✓ Structure updated | NQ 4H FVG above: {fvg.get('above')} | below: {fvg.get('below')}")
            return data
        except Exception as e:
            pulse_logger.log(f"⚠️ Structure — fetch failed: {e}, serving cache", level="WARNING")
            if cached:
                cached = dict(cached)
                cached['status'] = 'stale'
                return cached
            return {'status': 'unavailable', 'nq': None, 'es': None}


market_structure_pipeline = MarketStructurePipeline()
