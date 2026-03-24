import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class InstitutionalPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "institutional"
        self.cot_url = "https://www.cftc.gov/dea/futures/financial_lf.htm"
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def is_friday(self):
        return datetime.now(pytz.timezone(TIMEZONE)).weekday() == 4

    def parse_positions(self, text, instrument):
        try:
            import re
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
                return None
            positions = {
                'asset_mgr_long': nums[3],
                'asset_mgr_short': nums[4],
                'leveraged_long': nums[6],
                'leveraged_short': nums[7],
            }
            asset_mgr_net = positions.get('asset_mgr_long', 0) - positions.get('asset_mgr_short', 0)
            leveraged_net = positions.get('leveraged_long', 0) - positions.get('leveraged_short', 0)
            combined_long = positions.get('asset_mgr_long', 0) + positions.get('leveraged_long', 0)
            combined_short = positions.get('asset_mgr_short', 0) + positions.get('leveraged_short', 0)
            combined_net = combined_long - combined_short
            total = combined_long + combined_short
            net_pct = round((combined_net / total * 100), 2) if total > 0 else 0
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

    def fetch_cot(self):
        try:
            response = requests.get(self.cot_url, headers=self.headers, timeout=15)
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
            existing = cache.load(self.cache_key)
            if not self.is_friday() and existing:
                pulse_logger.log("↺ Institutional — using weekly COT cache (updates Fridays)")
                return existing['data']

            nq_data, es_data = self.fetch_cot()

            prev_nq_net = None
            prev_es_net = None
            if existing and existing.get('data'):
                prev = existing['data']
                prev_nq = prev.get('nq_futures', {})
                prev_es = prev.get('es_futures', {})
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
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ Institutional COT updated | NQ: {nq_data['direction'] if nq_data else 'unknown'} | ES: {es_data['direction'] if es_data else 'unknown'} | Score: {pillar_score}")
            return result
        except Exception as e:
            error_handler.handle(e, "Institutional")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return cached['data']
            return None

institutional_pipeline = InstitutionalPipeline()
