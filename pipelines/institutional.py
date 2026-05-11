import json
import os
import re
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.logger import pulse_logger
from utils.error_handler import error_handler

_MAX_COT_POSITION = 5_000_000  # upper bound per long/short field; flags CFTC layout changes

class InstitutionalPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.permanent_file = "/data/permanent_cot.json"
        self.cot_url = "https://www.cftc.gov/dea/futures/financial_lf.htm"
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self._ensure_exists()

    def _ensure_exists(self):
        if not os.path.exists('/data'):
            os.makedirs('/data')
        if not os.path.exists(self.permanent_file):
            atomic_write_json(self.permanent_file, {})

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

    def _append_history(self, result):
        history_file = '/data/cot_history.json'
        try:
            try:
                with open(history_file, 'r') as f:
                    history = json.load(f)
            except Exception:
                history = []
            nq = result.get('nq_futures') or {}
            es = result.get('es_futures') or {}
            entry = {
                'timestamp': result.get('timestamp', ''),
                'nq_net_pct': nq.get('net_pct', 0),
                'nq_direction': nq.get('direction', 'unknown'),
                'es_net_pct': es.get('net_pct', 0),
                'es_direction': es.get('direction', 'unknown')
            }
            history.append(entry)
            history = history[-6:]
            atomic_write_json(history_file, history)
        except Exception as e:
            pulse_logger.log(f"⚠️ COT history append failed: {e}", level="WARNING")

    def fetch_cot(self):
        try:
            response = fetch_with_retry(self.cot_url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            pre = soup.find('pre')
            if not pre:
                raise ValueError("No pre tag found in CFTC page")
            text = pre.get_text()
            nq_data = self.parse_positions(text, 'NASDAQ')
            es_data = self.parse_positions(text, 'S&P 500')
            return nq_data, es_data
        except Exception as e:
            error_handler.handle(e, "COT Fetcher")
            return None, None

    def fetch(self):
        try:
            existing = self._load()
            if not self.is_friday() and existing:
                pulse_logger.log("↺ Institutional — using weekly COT cache (updates Fridays)")
                return existing

            nq_data, es_data = self.fetch_cot()

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
                'nq_futures': nq_data or {'direction': 'unknown', 'score': 0},
                'es_futures': es_data or {'direction': 'unknown', 'score': 0},
                'pillar_score': pillar_score,
                'update_frequency': 'Weekly (Fridays)',
                'next_update': 'Next Friday',
                'status': 'live'
            }
            self._save(result)
            self._append_history(result)
            pulse_logger.log(f"✓ Institutional COT updated | NQ: {nq_data['direction'] if nq_data else 'unknown'} | ES: {es_data['direction'] if es_data else 'unknown'} | Score: {pillar_score}")
            return result
        except Exception as e:
            error_handler.handle(e, "Institutional")
            existing = self._load()
            if existing:
                existing['status'] = 'stale'
                return existing
            return None

institutional_pipeline = InstitutionalPipeline()
