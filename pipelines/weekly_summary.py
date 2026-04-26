import json
import os
import re
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class WeeklySummaryPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.permanent_file = "/data/permanent_weekly_summary.json"
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
            pulse_logger.log(f"⚠️ Failed to load weekly summary permanent file: {e}", level="WARNING")
            return {}

    def _save(self, data):
        atomic_write_json(self.permanent_file, data)

    def is_friday(self):
        return datetime.now(pytz.timezone(TIMEZONE)).weekday() == 4

    def truncate_to_sentences(self, text, max_sentences=2):
        """Cut text at sentence boundary, max 2 sentences."""
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        return ' '.join(sentences[:max_sentences])

    def generate_summary(self, formatted_data):
        try:
            econ = formatted_data.get('economic', {})
            inst = formatted_data.get('institutional', {})
            macro = formatted_data.get('macro', {})
            geo = formatted_data.get('geopolitical', {})
            bias = formatted_data.get('bias', {})

            # Economic — day by day, score what has actually landed
            events = econ.get('events', [])
            pending = [e for e in events if e.get('result') in ['pending', 'speech']]
            scored = [e for e in events if e.get('result') not in ['pending', 'speech', 'unknown']]
            beats = [e for e in scored if e.get('market_impact') == 'bullish']
            misses = [e for e in scored if e.get('market_impact') == 'bearish']
            total = len(events)

            if len(scored) == 0:
                event_names = [e['title'] for e in pending[:3]]
                econ_summary = f"No actuals in yet. {total} USD events this week" + (f" — watch {', '.join(event_names)}" if event_names else "") + "."
            else:
                lean = 'bullish' if len(beats) > len(misses) else 'bearish' if len(misses) > len(beats) else 'neutral'
                econ_summary = f"{len(scored)} of {total} actuals in — {len(beats)} beats, {len(misses)} misses. Economic data leaning {lean} so far."
                if pending:
                    next_events = [e['title'] for e in pending[:2]]
                    econ_summary += f" Still watching: {', '.join(next_events)}."

            # Institutional
            nq = inst.get('nq_futures', {})
            es = inst.get('es_futures', {})
            inst_summary = f"Big money net {nq.get('direction', 'unknown')} on NQ ({nq.get('net_pct', 0)}%) and {es.get('direction', 'unknown')} on ES ({es.get('net_pct', 0)}%)."

            # Macro
            vix = macro.get('vix', {})
            fg = macro.get('fear_greed', {})
            macro_summary = f"VIX at {vix.get('value', '--')}, Fear & Greed at {fg.get('score', '--')} ({fg.get('rating', 'N/A')})."

            # Geopolitical — 2 sentences max, clean boundary
            flags = geo.get('active_flags', [])
            if flags:
                flag_texts = []
                for f in flags[:2]:
                    context = f.get('context', f.get('title', ''))
                    flag_texts.append(self.truncate_to_sentences(context, 2))
                geo_summary = ' | '.join(flag_texts)
            else:
                geo_summary = "No major geopolitical flags active this week."

            # Overall bias
            bias_direction = bias.get('bias', 'Neutral') if bias else 'Neutral'
            confidence = bias.get('confidence', 0) if bias else 0
            confidence_label = bias.get('confidence_label', '') if bias else ''

            # Week over week comparison (Friday only)
            wow_summary = None
            existing = self._load()
            last_week = existing.get('last_week_bias', {})

            if self.is_friday() and last_week:
                last_direction = last_week.get('bias', 'Neutral')
                last_confidence = last_week.get('confidence', 0)
                conf_change = confidence - last_confidence

                if bias_direction == last_direction:
                    if conf_change >= 10:
                        wow_summary = f"Regime conviction is strengthening — {bias_direction} with more certainty than last week (+{conf_change}% confidence)."
                    elif conf_change <= -10:
                        wow_summary = f"Regime staying {bias_direction} but conviction is fading ({conf_change}% confidence vs last week). Getting more complicated."
                    else:
                        wow_summary = f"Regime unchanged from last week — {bias_direction} with similar confidence."
                else:
                    if bias_direction == 'Neutral' or last_direction == 'Neutral':
                        wow_summary = f"Regime has shifted from {last_direction} to {bias_direction} — conditions getting more complicated. Reduce size."
                    elif bias_direction == 'Bearish' and last_direction == 'Bullish':
                        wow_summary = f"Regime has flipped Bullish → Bearish week over week. Significant shift — prioritize shorts, avoid longs."
                    elif bias_direction == 'Bullish' and last_direction == 'Bearish':
                        wow_summary = f"Regime has flipped Bearish → Bullish week over week. Significant shift — prioritize longs, avoid shorts."

            if len(scored) == 0:
                status_label = "Week Preview"
                overall = f"{status_label} — Institutional and Macro scoring active. Economic score pending first releases this week."
            elif pending:
                status_label = "Week In Progress"
                overall = f"{status_label} — {bias_direction} with {confidence}% confidence ({confidence_label}). {len(pending)} events still pending."
            else:
                status_label = "Week Wrap"
                overall = f"{status_label} — Final regime: {bias_direction} with {confidence}% confidence ({confidence_label})."
                if wow_summary:
                    overall += f" {wow_summary}"

            return {
                'generated_at': datetime.now(self.timezone).isoformat(),
                'overall': overall,
                'economic': econ_summary,
                'institutional': inst_summary,
                'macro': macro_summary,
                'geopolitical': geo_summary,
                'week_of': datetime.now(self.timezone).strftime('%B %d, %Y'),
                'wow_summary': wow_summary
            }
        except Exception as e:
            error_handler.handle(e, "Weekly Summary Generator")
            return None

    def fetch(self, formatted_data=None, bias=None):
        try:
            if formatted_data is None:
                return self._load() or None

            if bias:
                formatted_data['bias'] = bias

            summary = self.generate_summary(formatted_data)
            existing = self._load()

            # On Friday, store this week's bias for next week's comparison
            if self.is_friday() and bias:
                existing['last_week_bias'] = {
                    'bias': bias.get('bias', 'Neutral'),
                    'confidence': bias.get('confidence', 0),
                    'week_of': datetime.now(self.timezone).strftime('%B %d, %Y')
                }

            result = {
                'pillar': 'weekly_summary',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'summary': summary,
                'status': 'live',
                'last_week_bias': existing.get('last_week_bias', {})
            }

            self._save(result)
            pulse_logger.log(f"✓ Weekly Summary updated — {summary['overall'][:80] if summary else 'N/A'}")
            return result
        except Exception as e:
            error_handler.handle(e, "Weekly Summary")
            return self._load() or None

weekly_summary_pipeline = WeeklySummaryPipeline()
