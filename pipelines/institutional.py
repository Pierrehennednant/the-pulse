import requests
from datetime import datetime
import pytz
from config import TIMEZONE, COT_CACHE_FILE
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

    def fetch_cot(self):
        try:
            from bs4 import BeautifulSoup
            response = requests.get(self.cot_url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(response.content, 'html.parser')
            tables = soup.find_all('table')
            nq_data = None
            es_data = None
            for table in tables:
                text = table.get_text()
                if 'NASDAQ' in text.upper() or 'E-MINI' in text.upper():
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 4:
                            row_text = row.get_text().upper()
                            if 'NASDAQ' in row_text:
                                try:
                                    nq_data = {
                                        'long': int(cells[1].get_text(strip=True).replace(',', '')),
                                        'short': int(cells[2].get_text(strip=True).replace(',', ''))
                                    }
                                except:
                                    pass
                            if 'S&P 500' in row_text or 'ES' in row_text:
                                try:
                                    es_data = {
                                        'long': int(cells[1].get_text(strip=True).replace(',', '')),
                                        'short': int(cells[2].get_text(strip=True).replace(',', ''))
                                    }
                                except:
                                    pass
            return nq_data, es_data
        except Exception as e:
            error_handler.handle(e, "COT Fetcher")
            return None, None

    def calculate_positioning(self, data):
        if not data:
            return {'direction': 'unknown', 'score': 0}
        net = data['long'] - data['short']
        total = data['long'] + data['short']
        net_pct = (net / total * 100) if total > 0 else 0
        return {
            'long': data['long'],
            'short': data['short'],
            'net': net,
            'net_pct': round(net_pct, 2),
            'direction': 'bullish' if net > 0 else 'bearish',
            'score': 1.0 if net_pct > 10 else -1.0 if net_pct < -10 else 0.5 if net_pct > 0 else -0.5
        }

    def fetch(self):
        try:
            existing = cache.load(self.cache_key)
            if not self.is_friday() and existing:
                pulse_logger.log("↺ Institutional — using weekly COT cache (updates Fridays)")
                return existing['data']
            nq_raw, es_raw = self.fetch_cot()
            nq_positioning = self.calculate_positioning(nq_raw)
            es_positioning = self.calculate_positioning(es_raw)
            scores = []
            if nq_positioning.get('score'):
                scores.append(nq_positioning['score'])
            if es_positioning.get('score'):
                scores.append(es_positioning['score'])
            pillar_score = round(sum(scores) / len(scores), 2) if scores else 0
            result = {
                'pillar': 'institutional',
                'timestamp': datetime.now(pytz.timezone(TIMEZONE)).isoformat(),
                'nq_futures': nq_positioning,
                'es_futures': es_positioning,
                'pillar_score': pillar_score,
                'update_frequency': 'Weekly (Fridays)',
                'next_update': 'Next Friday',
                'status': 'live'
            }
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ Institutional COT updated | NQ: {nq_positioning['direction']} | Score: {pillar_score}")
            return result
        except Exception as e:
            error_handler.handle(e, "Institutional")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return cached['data']
            return None

institutional_pipeline = InstitutionalPipeline()
