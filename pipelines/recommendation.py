import json
import os
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger

PROP_FIRM_THRESHOLD_FILE = '/data/prop_firm_weekly_threshold.json'

class RecommendationEngine:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)

    def compute(self, bias_data, geo_data, macro_data):
        try:
            bias = bias_data.get('bias', 'Neutral') if bias_data else 'Neutral'
            confidence = bias_data.get('confidence', 0) if bias_data else 0

            if bias == 'Neutral' or confidence < 60:
                return None

            if confidence >= 70:
                return {
                    'mode': 'normal',
                    'label': f'{bias} — Half size',
                    'reason': f'Confidence {confidence}%',
                    'strength': 'strong'
                }
            return {
                'mode': 'quarter',
                'label': f'{bias} — Quarter size',
                'reason': f'Confidence {confidence}%',
                'strength': 'moderate'
            }

        except Exception as e:
            pulse_logger.log(f"⚠️ Recommendation engine failed: {e}", level="WARNING")
            return None

recommendation_engine = RecommendationEngine()


class PropFirmRecommendationEngine(RecommendationEngine):
    """Prop Firm recommendation — same pillar data, aggressive entry thresholds.

    Differences from Live:
      Bias threshold         ±0.30 quiet week (≤1 red folder day) / ±0.33 standard week (≥2)  (Live ±0.50)
      Show-card confidence     60%  (same as Live)
      Quarter-entry confidence 60%–69%  (same as Live)
      Half-entry confidence    ≥70%  (same as Live)
      Pillar alignment         ≥45% of total week weight must agree with bias
                               Quiet week: EC 15%, total 85%, threshold ≥38.25%
                               Standard week: EC 30%, total 100%, threshold ≥45%  (Live: none)

    Quiet week = 0 or 1 calendar days with at least one red folder event.
    A day with multiple red folder events counts as 1 red folder day.
    Threshold evaluated once per ISO week; persisted to PROP_FIRM_THRESHOLD_FILE.
    """

    _WEEK_WEIGHTS = {
        'standard': {'economic_calendar': 30, 'geopolitical': 25, 'institutional': 25, 'macro_sentiment': 20},
        'quiet':    {'economic_calendar': 15, 'geopolitical': 25, 'institutional': 25, 'macro_sentiment': 20},
    }

    def _count_red_folder_days(self, econ_data):
        """Count calendar days with at least one red folder (high-impact) event this week."""
        if not econ_data:
            return 0
        red_days = set()
        for e in econ_data.get('events', []):
            if e.get('impact', '').lower() == 'high':
                time_est = e.get('time_est', '')
                day = time_est.split(',')[0] if ',' in time_est else time_est[:10]
                if day:
                    red_days.add(day)
        return len(red_days)

    def _get_weekly_threshold(self, econ_data):
        """Return week mode dict. Reads cache for current ISO week; recomputes on new week.

        Returns dict with keys:
          bias_threshold, red_folder_days, is_new_week, is_quiet_week,
          ec_weight, total_weight, alignment_threshold
        """
        now = datetime.now(self.timezone)
        iso = now.isocalendar()
        current_week = (iso[0], iso[1])

        try:
            if os.path.exists(PROP_FIRM_THRESHOLD_FILE):
                with open(PROP_FIRM_THRESHOLD_FILE, 'r') as f:
                    cached = json.load(f)
                if tuple(cached.get('week', [])) == current_week and 'is_quiet_week' in cached:
                    return {
                        'bias_threshold': cached['threshold'],
                        'red_folder_days': cached['red_folder_days'],
                        'is_new_week': False,
                        'is_quiet_week': cached['is_quiet_week'],
                        'ec_weight': cached['ec_weight'],
                        'total_weight': cached['total_weight'],
                        'alignment_threshold': cached['alignment_threshold'],
                    }
        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm threshold cache read failed: {e}", level="WARNING")

        red_folder_days = self._count_red_folder_days(econ_data)
        is_quiet = red_folder_days <= 1
        threshold = 0.30 if is_quiet else 0.33
        ec_weight = 15 if is_quiet else 30
        total_weight = 85 if is_quiet else 100
        alignment_threshold = round(total_weight * 0.45, 2)  # 38.25 (quiet) or 45.0 (standard)

        try:
            atomic_write_json(PROP_FIRM_THRESHOLD_FILE, {
                'week': list(current_week),
                'threshold': threshold,
                'red_folder_days': red_folder_days,
                'is_quiet_week': is_quiet,
                'ec_weight': ec_weight,
                'total_weight': total_weight,
                'alignment_threshold': alignment_threshold,
                'set_at': now.isoformat(),
            })
        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm threshold cache write failed: {e}", level="WARNING")

        return {
            'bias_threshold': threshold,
            'red_folder_days': red_folder_days,
            'is_new_week': True,
            'is_quiet_week': is_quiet,
            'ec_weight': ec_weight,
            'total_weight': total_weight,
            'alignment_threshold': alignment_threshold,
        }

    def _no_rec(self, week_info):
        """No-recommendation sentinel — carries quiet week metadata for dashboard display."""
        return {
            'label': None,
            'quiet_week': week_info['is_quiet_week'],
            'ec_weight': week_info['ec_weight'],
            'bias_threshold': week_info['bias_threshold'],
        }

    def _rec(self, week_info, **kwargs):
        """Build a recommendation dict with quiet week metadata attached."""
        return {
            'quiet_week': week_info['is_quiet_week'],
            'ec_weight': week_info['ec_weight'],
            'bias_threshold': week_info['bias_threshold'],
            **kwargs,
        }

    def compute_prop_firm(self, bias_data, geo_data, macro_data, econ_data=None):
        try:
            week_info = self._get_weekly_threshold(econ_data)
            is_quiet = week_info['is_quiet_week']
            bias_threshold = week_info['bias_threshold']
            ec_weight = week_info['ec_weight']
            alignment_threshold = week_info['alignment_threshold']
            red_folder_days = week_info['red_folder_days']

            if week_info['is_new_week']:
                mode_label = 'quiet' if is_quiet else 'standard'
                day_s = 'day' if red_folder_days == 1 else 'days'
                pulse_logger.log(
                    f"📊 Prop Firm — new week detected: {mode_label} "
                    f"({red_folder_days} red folder {day_s})"
                )

            day_s = 'day' if red_folder_days == 1 else 'days'
            if is_quiet:
                pulse_logger.log(f"🔇 Quiet week active — {red_folder_days} red folder {day_s} — EC {ec_weight}%, bias ±{bias_threshold}")
            else:
                pulse_logger.log(f"📅 Standard week — {red_folder_days} red folder {day_s} — EC {ec_weight}%, bias ±{bias_threshold}")

            final_score = (bias_data.get('final_score', 0) or 0) if bias_data else 0
            if final_score >= bias_threshold:
                bias = 'Bullish'
            elif final_score <= -bias_threshold:
                bias = 'Bearish'
            else:
                return self._no_rec(week_info)

            pillar_weights = self._WEEK_WEIGHTS['quiet' if is_quiet else 'standard']
            pillar_contributions = (bias_data.get('pillar_contributions', {}) or {}) if bias_data else {}
            aligned_weight = sum(
                pillar_weights.get(p, 0)
                for p, c in pillar_contributions.items()
                if (bias == 'Bullish' and c.get('raw_score', 0) > 0.15)
                or (bias == 'Bearish' and c.get('raw_score', 0) < -0.15)
            )
            if aligned_weight < alignment_threshold:
                return self._no_rec(week_info)

            confidence = bias_data.get('confidence', 0) if bias_data else 0
            if confidence < 60:
                return self._no_rec(week_info)

            total_w = week_info['total_weight']
            if confidence >= 70:
                return self._rec(week_info,
                    mode='normal',
                    label=f'Prop Firm — {bias}, Normal entry',
                    reason=f'{aligned_weight}% of {total_w}% weight aligned · Confidence {confidence}%',
                    strength='strong',
                    bias=bias,
                )
            return self._rec(week_info,
                mode='quarter',
                label=f'Prop Firm — {bias}, Quarter entry',
                reason=f'Confidence {confidence}% — building toward Normal',
                strength='moderate',
                bias=bias,
            )

        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm recommendation engine failed: {e}", level="WARNING")
            return None


prop_firm_engine = PropFirmRecommendationEngine()
