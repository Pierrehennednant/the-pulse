import json
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pytz
from config import TIMEZONE
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.logger import pulse_logger
from utils.error_handler import error_handler

_MAX_COT_POSITION = 5_000_000  # upper bound per long/short field; flags CFTC layout changes
_MAX_MONDAY_RECHECK_ATTEMPTS = 3  # failed Monday re-checks allowed once the cache is confirmed newest
# CFTC Public Reporting (Socrata) — Traders in Financial Futures, Futures Only.
# Same report the legacy financial_lf.htm page renders; codes are the "Consolidated"
# sections the scrape matched first (S&P 500 Consolidated 13874+, NASDAQ-100 Consolidated 20974+).
_COT_API_URL = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
_COT_API_CODES = {'S&P 500': '13874+', 'NASDAQ': '20974+'}
_REPORT_DATE_RE = re.compile(r'Positions\s+as\s+of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})', re.IGNORECASE)

class InstitutionalPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.permanent_file = "/data/permanent_cot.json"
        self.cot_url = "https://www.cftc.gov/dea/futures/financial_lf.htm"
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def _load(self):
        try:
            with open(self.permanent_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load institutional permanent file: {e}", level="WARNING")
            return {}

    def _save(self, data):
        atomic_write_json(self.permanent_file, data)

    def is_friday(self):
        return datetime.now(pytz.timezone(TIMEZONE)).weekday() == 4

    def parse_positions(self, text, instrument):
        try:
            idx = text.upper().find(instrument.upper())
            if idx == -1:
                return None
            section = text[idx:idx+800]
            lines = section.split('\n')
            positions_line = None
            for i, line in enumerate(lines):
                if line.strip() == 'Positions':
                    if i + 1 < len(lines):
                        positions_line = lines[i + 1]
                    break
            if not positions_line:
                return None
            nums = [int(x.replace(',', '')) for x in re.findall(r'[\d,]+', positions_line)]
            if len(nums) < 14:
                pulse_logger.log(
                    f"❌ COT sanity check failed for {instrument}: expected ≥14 columns, got {len(nums)}",
                    level="ERROR"
                )
                return None
            positions = {
                'asset_mgr_long': nums[3],
                'asset_mgr_short': nums[4],
                'leveraged_long': nums[6],
                'leveraged_short': nums[7],
            }
            return self._build_positions(positions, instrument)
        except Exception as e:
            error_handler.handle(e, f"COT Parser {instrument}")
            return None

    def _build_positions(self, positions, instrument):
        try:
            for field, val in positions.items():
                if val < 0 or val > _MAX_COT_POSITION:
                    pulse_logger.log(
                        f"❌ COT sanity check failed for {instrument}: {field}={val:,} outside [0, {_MAX_COT_POSITION:,}]",
                        level="ERROR"
                    )
                    return None
            asset_mgr_net = positions.get('asset_mgr_long', 0) - positions.get('asset_mgr_short', 0)
            leveraged_net = positions.get('leveraged_long', 0) - positions.get('leveraged_short', 0)
            combined_long = positions.get('asset_mgr_long', 0) + positions.get('leveraged_long', 0)
            combined_short = positions.get('asset_mgr_short', 0) + positions.get('leveraged_short', 0)
            combined_net = combined_long - combined_short
            total = combined_long + combined_short
            if total == 0:
                pulse_logger.log(
                    f"❌ COT sanity check failed for {instrument}: combined open interest is zero — likely a parse column mismatch",
                    level="ERROR"
                )
                return None
            net_pct = round((combined_net / total * 100), 2)
            direction = 'bullish' if combined_net > 0 else 'bearish'
            score = 1.0 if net_pct > 10 else -1.0 if net_pct < -10 else 0.5 if net_pct > 0 else -0.5
            return {
                'asset_mgr_long': positions.get('asset_mgr_long', 0),
                'asset_mgr_short': positions.get('asset_mgr_short', 0),
                'asset_mgr_net': asset_mgr_net,
                'leveraged_long': positions.get('leveraged_long', 0),
                'leveraged_short': positions.get('leveraged_short', 0),
                'leveraged_net': leveraged_net,
                'combined_net': combined_net,
                'net_pct': net_pct,
                'direction': direction,
                'score': score
            }
        except Exception as e:
            error_handler.handle(e, f"COT Parser {instrument}")
            return None

    def parse_report_date(self, text):
        """Return the CFTC 'Positions as of <Month D, YYYY>' date as YYYY-MM-DD, or None."""
        try:
            m = _REPORT_DATE_RE.search(text or '')
            if not m:
                return None
            return datetime.strptime(' '.join(m.group(1).split()), '%B %d, %Y').date().isoformat()
        except Exception:
            return None

    def _log_bad_response(self, response, reason):
        """Log what CFTC actually sent back so a block/challenge page is visible."""
        try:
            h = response.headers or {}
            keys = ['server', 'content-type', 'content-length', 'cf-ray', 'cf-mitigated',
                    'x-cache', 'via', 'retry-after', 'location']
            hdrs = ' '.join(f"{k}={h.get(k)}" for k in keys if h.get(k) is not None)
            body = response.content[:2000].decode('utf-8', errors='replace') if response.content else ''
            snippet = ' '.join(body.split())[:300]
            pulse_logger.log(
                f"⚠️ COT response detail ({reason}) | status={response.status_code} | "
                f"bytes={len(response.content or b'')} | {hdrs or 'no headers'} | body={snippet!r}",
                level="WARNING"
            )
        except Exception as e:
            pulse_logger.log(f"⚠️ COT response detail unavailable: {e}", level="WARNING")

    def _log_api_exception(self, exc):
        pulse_logger.log(
            f"⚠️ COT response detail (API request error) | status=none | {type(exc).__name__}: {str(exc)[:300]}",
            level="WARNING"
        )

    def fetch_cot_api(self):
        """Pull ES/NQ TFF futures-only positions from CFTC's public Socrata API.
        Returns (nq_data, es_data, report_date) or (None, None, None)."""
        try:
            codes = ",".join(f"'{c}'" for c in _COT_API_CODES.values())
            params = {
                '$where': f"cftc_contract_market_code in({codes})",
                '$order': 'report_date_as_yyyy_mm_dd DESC',
                '$limit': 8,
            }
            try:
                response = fetch_with_retry(_COT_API_URL, params=params,
                                            headers={**self.headers, 'Accept': 'application/json'}, timeout=10)
            except Exception as e:
                self._log_api_exception(e)
                raise
            if response.status_code != 200:
                self._log_bad_response(response, "API non-200")
                raise ValueError(f"CFTC API returned HTTP {response.status_code}")
            try:
                rows = response.json()
            except Exception:
                self._log_bad_response(response, "API non-JSON")
                raise ValueError("CFTC API returned non-JSON body")
            if not isinstance(rows, list) or not rows:
                self._log_bad_response(response, "API empty")
                raise ValueError("CFTC API returned no rows")
            latest = max(str(r.get('report_date_as_yyyy_mm_dd', '')) for r in rows)
            by_code = {r.get('cftc_contract_market_code'): r for r in rows
                       if str(r.get('report_date_as_yyyy_mm_dd', '')) == latest}
            out = {}
            for instrument, code in _COT_API_CODES.items():
                r = by_code.get(code)
                if not r:
                    pulse_logger.log(f"❌ COT API: no {instrument} ({code}) row for {latest[:10]}", level="ERROR")
                    out[instrument] = None
                    continue
                positions = {
                    'asset_mgr_long': int(float(r['asset_mgr_positions_long'])),
                    'asset_mgr_short': int(float(r['asset_mgr_positions_short'])),
                    'leveraged_long': int(float(r['lev_money_positions_long'])),
                    'leveraged_short': int(float(r['lev_money_positions_short'])),
                }
                out[instrument] = self._build_positions(positions, instrument)
            report_date = latest[:10] if re.match(r'\d{4}-\d{2}-\d{2}', latest) else None
            if out['NASDAQ'] is None and out['S&P 500'] is None:
                return None, None, None
            return out['NASDAQ'], out['S&P 500'], report_date
        except Exception as e:
            error_handler.handle(e, "COT API")
            return None, None, None

    def fetch_cot(self):
        self.last_report_date = None
        nq_data, es_data, report_date = self.fetch_cot_api()
        if nq_data is not None or es_data is not None:
            self.last_report_date = report_date
            return nq_data, es_data
        pulse_logger.log("⚠️ Institutional — CFTC API failed, trying legacy page scrape", level="WARNING")
        return self.fetch_cot_scrape()

    def fetch_cot_scrape(self):
        self.last_report_date = None
        try:
            response = fetch_with_retry(self.cot_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            pre = soup.find('pre')
            if not pre:
                self._log_bad_response(response, "no <pre>")
                raise ValueError("No pre tag found in CFTC page")
            text = pre.get_text()
            self.last_report_date = self.parse_report_date(text)
            nq_data = self.parse_positions(text, 'NASDAQ')
            es_data = self.parse_positions(text, 'S&P 500')
            return nq_data, es_data
        except Exception as e:
            error_handler.handle(e, "COT Fetcher")
            return None, None

    def _latest_release(self, now):
        """Most recent scheduled release at or before `now`: (Friday 3:30 PM ET datetime,
        expected Tuesday as-of date). Pure schedule — no holiday assumptions; a delayed
        release simply fails the report-date match below and stays stale."""
        days_back = (now.weekday() - 4) % 7
        friday = now - timedelta(days=days_back)
        release = friday.replace(hour=15, minute=30, second=0, microsecond=0)
        if release > now:
            release = release - timedelta(days=7)
        return release, (release - timedelta(days=3)).date()

    def _cache_confirmed_newest(self, existing, now):
        """True only if the cached report date equals the expected latest Tuesday as-of
        date AND the data was fetched after that release. Caches without a parsed
        report_date (pre-fix) are never confirmed."""
        try:
            report_date = existing.get('report_date')
            ts = existing.get('data_timestamp') or existing.get('timestamp', '')
            if not report_date or not ts:
                return False
            fetched = datetime.fromisoformat(ts)
            if fetched.tzinfo is None:
                fetched = self.timezone.localize(fetched)
            release, expected = self._latest_release(now)
            return report_date == expected.isoformat() and fetched >= release
        except Exception:
            return False

    def _cache_is_stale(self, existing):
        """True if the cached COT data is older than 3 days — used to detect
        holiday-delayed releases where CFTC publishes Monday instead of Friday."""
        ts = existing.get('timestamp', '')
        if not ts:
            return True
        try:
            cached_dt = datetime.fromisoformat(ts)
            if cached_dt.tzinfo is None:
                cached_dt = cached_dt.replace(tzinfo=pytz.timezone(TIMEZONE))
            age_days = (datetime.now(pytz.timezone(TIMEZONE)) - cached_dt).total_seconds() / 86400
            return age_days >= 3
        except Exception:
            return False

    def _cache_from_friday(self, existing):
        """True if the cached COT data was written on a Friday.
        On Mondays, a Friday-stamped cache always triggers a re-fetch — the normal
        Friday release (3:30 PM EST) is only ~2.7 days old by Monday 9 AM, which
        would fail the >=3 day stale check even though new data may be available."""
        ts = existing.get('timestamp', '')
        if not ts:
            return False
        try:
            cached_dt = datetime.fromisoformat(ts)
            if cached_dt.tzinfo is None:
                cached_dt = cached_dt.replace(tzinfo=pytz.timezone(TIMEZONE))
            return cached_dt.weekday() == 4  # Friday
        except Exception:
            return False

    def fetch(self):
        try:
            existing = self._load()
            now = datetime.now(pytz.timezone(TIMEZONE))
            _from_friday = existing and self._cache_from_friday(existing)
            _stale = existing and self._cache_is_stale(existing)
            # Bounded retry: after a failed re-check on a confirmed-newest cache, retry
            # only until _MAX_MONDAY_RECHECK_ATTEMPTS failures today, not every refresh.
            _attempts = existing.get('monday_recheck_failures', 0) if existing else 0
            _retry_today = (existing and existing.get('monday_recheck_date') == now.date().isoformat()
                            and 0 < _attempts < _MAX_MONDAY_RECHECK_ATTEMPTS)
            is_monday_stale = now.weekday() == 0 and (_from_friday or _stale or _retry_today)
            if is_monday_stale:
                reason = ("cache from Friday" if _from_friday else "cache >3 days old" if _stale
                          else f"bounded retry {_attempts + 1}/{_MAX_MONDAY_RECHECK_ATTEMPTS}")
                pulse_logger.log(f"🔄 Institutional — Monday fetch triggered ({reason}), checking CFTC for latest release")

            if not self.is_friday() and not is_monday_stale and existing:
                if not existing.get('pillar_score'):
                    nq = existing.get('nq_futures') or {}
                    es = existing.get('es_futures') or {}
                    scores = [s for s in [nq.get('score'), es.get('score')] if s]
                    existing['pillar_score'] = round(sum(scores) / len(scores), 2) if scores else 0
                pulse_logger.log(f"↺ Institutional — using weekly COT cache | Score: {existing['pillar_score']}")
                return existing

            nq_data, es_data = self.fetch_cot()

            # If CFTC returned nothing, do not overwrite the permanent file with blank data.
            # Return whatever we had (as stale) so the dashboard keeps showing real positions.
            if nq_data is None and es_data is None:
                if existing and now.weekday() == 0 and is_monday_stale and self._cache_confirmed_newest(existing, now):
                    # Friday's report is confirmed as the newest release: the failed
                    # re-check cannot be hiding newer data. Serve it fresh. The verification
                    # time becomes `timestamp` (so it is not Friday-stamped); the original
                    # fetch time is kept in `data_timestamp`.
                    failures = (_attempts if existing.get('monday_recheck_date') == now.date().isoformat() else 0) + 1
                    existing.setdefault('data_timestamp', existing.get('timestamp'))
                    existing['timestamp'] = now.isoformat()
                    existing['status'] = 'live'
                    existing['monday_recheck_date'] = now.date().isoformat()
                    existing['monday_recheck_failures'] = failures
                    self._save(existing)
                    more = ("no further retries today" if failures >= _MAX_MONDAY_RECHECK_ATTEMPTS
                            else f"will retry ({failures}/{_MAX_MONDAY_RECHECK_ATTEMPTS})")
                    pulse_logger.log(
                        f"✅ Institutional — CFTC re-check failed but cached report (as of {existing['report_date']}, "
                        f"fetched {existing['data_timestamp']}) is the newest release; keeping fresh | {more}",
                        level="WARNING"
                    )
                    return existing
                if existing:
                    existing['status'] = 'stale'
                    pulse_logger.log("⚠️ Institutional — CFTC fetch failed, returning existing data as stale", level="WARNING")
                    return existing
                pulse_logger.log("⚠️ Institutional — CFTC fetch failed, no existing data", level="WARNING")
                return None

            prev_nq_net = None
            prev_es_net = None
            if existing:
                prev_nq = existing.get('nq_futures', {})
                prev_es = existing.get('es_futures', {})
                prev_nq_net = prev_nq.get('combined_net')
                prev_es_net = prev_es.get('combined_net')

            if nq_data and prev_nq_net is not None:
                nq_data['prev_net'] = prev_nq_net
                nq_data['wow_change'] = nq_data['combined_net'] - prev_nq_net
                nq_data['wow_direction'] = 'increasing' if nq_data['wow_change'] > 0 else 'decreasing'

            if es_data and prev_es_net is not None:
                es_data['prev_net'] = prev_es_net
                es_data['wow_change'] = es_data['combined_net'] - prev_es_net
                es_data['wow_direction'] = 'increasing' if es_data['wow_change'] > 0 else 'decreasing'

            scores = []
            if nq_data and nq_data.get('score'):
                scores.append(nq_data['score'])
            if es_data and es_data.get('score'):
                scores.append(es_data['score'])
            pillar_score = round(sum(scores) / len(scores), 2) if scores else 0

            result = {
                'pillar': 'institutional',
                'timestamp': datetime.now(pytz.timezone(TIMEZONE)).isoformat(),
                'report_date': getattr(self, 'last_report_date', None),
                'nq_futures': nq_data or {'direction': 'unknown', 'score': 0},
                'es_futures': es_data or {'direction': 'unknown', 'score': 0},
                'pillar_score': pillar_score,
                'update_frequency': 'Weekly (Fridays)',
                'next_update': 'Next Friday',
                'status': 'live'
            }
            self._save(result)
            pulse_logger.log(f"✓ Institutional COT updated | as of {result['report_date'] or 'unknown'} | NQ: {nq_data['direction'] if nq_data else 'unknown'} | ES: {es_data['direction'] if es_data else 'unknown'} | Score: {pillar_score}")
            return result
        except Exception as e:
            error_handler.handle(e, "Institutional")
            existing = self._load()
            if existing:
                existing['status'] = 'stale'
                return existing
            return None

institutional_pipeline = InstitutionalPipeline()
