import requests
from datetime import datetime
import pytz
from config import TIMEZONE, STALE_THRESHOLDS
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class EconomicCalendarPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "economic_calendar"
        self.url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    def is_market_moving(self, event):
        if event.get('country', '').upper() != 'USD':
            return False
        impact = event.get('impact', '').lower()
        if impact not in ['high', 'medium']:
            return False
        return True

    def convert_to_est(self, date_str):
        try:
            dt = datetime.fromisoformat(date_str)
            est = dt.astimezone(self.timezone)
            return est.strftime('%I:%M %p EST')
        except:
            return 'TBD'

    def get_market_implication(self, title, actual, forecast, previous):
        if not actual or actual == '':
            return 'pending', 'unknown', f'{title} not yet released'
        try:
            actual_val = float(actual.replace('%', '').replace('K', '').replace('M', '').replace('B', '').replace('T', ''))
            forecast_val = float(forecast.replace('%', '').replace('K', '').replace('M', '').replace('B', '').replace('T', '')) if forecast else None
            previous_val = float(previous.replace('%', '').replace('K', '').replace('M', '').replace('B', '').replace('T', '')) if previous else None
        except:
            return 'pending', 'unknown', f'{title} — cannot parse values'

        if forecast_val is not None:
            if actual_val > forecast_val:
                return 'beat', 'bullish', f'{title} beat forecast ({actual} vs {forecast})'
            elif actual_val < forecast_val:
                return 'miss', 'bearish', f'{title} missed forecast ({actual} vs {forecast})'
            else:
                return 'inline', 'neutral', f'{title} inline with forecast ({actual})'
        elif previous_val is not None:
            if actual_val > previous_val:
                return 'improved', 'bullish', f'{title} improved from previous ({actual} vs {previous})'
            elif actual_val < previous_val:
                return 'declined', 'bearish', f'{title} declined from previous ({actual} vs {previous})'
            else:
                return 'unchanged', 'neutral', f'{title} unchanged from previous ({actual})'
        return 'pending', 'unknown', f'{title} — no comparison available'

    def calculate_score(self, events):
        if not events:
            return 0.0
        
        score = 0.0
        count = 0
        impact_map = {'bullish': 1, 'bearish': -1, 'neutral': 0}
        
        for event in events:
            if event.get('market_impact') in impact_map:
                score += impact_map[event['market_impact']]
                count += 1
        
        pending_count = sum(1 for e in events if e['result'] == 'pending')
        if pending_count > 0:
            score -= 0.3 * pending_count
        
        return round(score / max(count, 1), 2)
    
    def fetch(self):
        try:
            response = requests.get(self.url, headers=self.headers, timeout=15)
            raw_events = response.json()
            events = []
            for event in raw_events:
                if not self.is_market_moving(event):
                    continue
                title = event.get('title', '')
                actual = event.get('actual', '')
                forecast = event.get('forecast', '')
                previous = event.get('previous', '')
                date_str = event.get('date', '')
                result, market_impact, reason = self.get_market_implication(title, actual, forecast, previous)
                events.append({
                    'title': title,
                    'time_est': self.convert_to_est(date_str),
                    'forecast': forecast or 'N/A',
                    'previous': previous or 'N/A',
                    'actual': actual or 'Pending',
                    'impact': event.get('impact', ''),
                    'result': result,
                    'market_impact': market_impact,
                    'reason': reason
                })
            score = self.calculate_score(events)
            warnings = []
            for event in events:
                if event['result'] == 'pending':
                    warnings.append(f"⚠️ {event['title']} at {event['time_est']} — {event['reason']}")
            result_data = {
                'pillar': 'economic_calendar',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'events': events,
                'pillar_score': score,
                'warnings': warnings,
                'status': 'live'
            }
            cache.save(self.cache_key, result_data)
            pulse_logger.log(f"✓ Economic Calendar updated | {len(events)} USD high/medium impact events | Score: {score}")
            return result_data
        except Exception as e:
            error_handler.handle(e, "Economic Calendar")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return cached['data']
            return None

economic_calendar_pipeline = EconomicCalendarPipeline()
