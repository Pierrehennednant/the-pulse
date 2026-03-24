from datetime import datetime
import pytz
from config import TIMEZONE
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class WeeklySummaryPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "weekly_summary"

    def is_monday(self):
        return datetime.now(pytz.timezone(TIMEZONE)).weekday() == 0

    def generate_summary(self, formatted_data):
        try:
            econ = formatted_data.get('economic', {})
            inst = formatted_data.get('institutional', {})
            macro = formatted_data.get('macro', {})
            geo = formatted_data.get('geopolitical', {})
            bias = formatted_data.get('bias', {})

            events = econ.get('events', [])
            pending = [e for e in events if e.get('result') == 'pending']
            event_names = [e['title'] for e in pending[:3]]
            econ_summary = f"This week has {len(pending)} key USD events" + (f" including {', '.join(event_names)}" if event_names else "") + "."

            nq = inst.get('nq_futures', {})
            es = inst.get('es_futures', {})
            inst_summary = f"Big money is net {nq.get('direction', 'unknown')} on NQ ({nq.get('net_pct', 0)}%) and {es.get('direction', 'unknown')} on ES ({es.get('net_pct', 0)}%)."

            vix = macro.get('vix', {})
            fg = macro.get('fear_greed', {})
            macro_summary = f"VIX at {vix.get('value', '--')} with Fear & Greed at {fg.get('score', '--')} ({fg.get('rating', 'N/A')})."

            flags = geo.get('active_flags', [])
            if flags:
                top_flags = [f['title'][:60] for f in flags[:2]]
                geo_summary = f"Active geopolitical risks: {' | '.join(top_flags)}."
            else:
                geo_summary = "No major geopolitical flags active this week."

            bias_direction = bias.get('bias', 'Neutral') if bias else 'Neutral'
            confidence = bias.get('confidence', 0) if bias else 0
            confidence_label = bias.get('confidence_label', '') if bias else ''

            overall = f"Overall regime entering this week: {bias_direction} with {confidence}% confidence ({confidence_label}). "

            if confidence < 40:
                overall += "Regime is unclear — reduce size and wait for confirmation."
            elif bias_direction == 'Bearish':
                overall += "Lean short on rallies, avoid aggressive longs until conditions improve."
            elif bias_direction == 'Bullish':
                overall += "Lean long on dips, institutions are supportive."
            else:
                overall += "Mixed signals — trade selectively and manage risk carefully."

            return {
                'generated_at': datetime.now(self.timezone).isoformat(),
                'overall': overall,
                'economic': econ_summary,
                'institutional': inst_summary,
                'macro': macro_summary,
                'geopolitical': geo_summary,
                'week_of': datetime.now(self.timezone).strftime('%B %d, %Y')
            }
        except Exception as e:
            error_handler.handle(e, "Weekly Summary Generator")
            return None

    def fetch(self, formatted_data=None, bias=None):
        try:
            existing = cache.load(self.cache_key)
            if not self.is_monday() and existing:
                pulse_logger.log("↺ Weekly Summary — using cached summary (updates Mondays)")
                return existing['data']

            if formatted_data is None:
                return existing['data'] if existing else None

            if bias:
                formatted_data['bias'] = bias

            summary = self.generate_summary(formatted_data)
            result = {
                'pillar': 'weekly_summary',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'summary': summary,
                'status': 'live'
            }
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ Weekly Summary generated for week of {summary['week_of'] if summary else 'N/A'}")
            return result
        except Exception as e:
            error_handler.handle(e, "Weekly Summary")
            cached = cache.load(self.cache_key)
            if cached:
                return cached['data']
            return None

weekly_summary_pipeline = WeeklySummaryPipeline()
