import json
import os
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger
from pipelines.economic_calendar import economic_calendar_pipeline

PROP_FIRM_THRESHOLD_FILE = '/data/prop_firm_weekly_threshold.json'

class RecommendationEngine:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)

    def compute(self, bias_data, geo_data, macro_data):
        try:
            bias = bias_data.get('bias', 'Neutral') if bias_data else 'Neutral'
            confidence = bias_data.get('confidence', 0) if bias_data else 0

            if bias == 'Neutral' or confidence < 55:
                return None

            if confidence >= 65:
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
      Show-card confidence     55%  (same as Live)
      Quarter-entry confidence 55%–64%  (same as Live)
      Half-entry confidence    ≥65%  (same as Live)
      Pillar alignment         ≥45% of total week weight must agree with bias
                               Quiet week: EC 15%, total 85%, threshold ≥38.25%
                               Standard week: EC 30%, total 100%, threshold ≥45%  (Live: none)

    Quiet week = 0 or 1 calendar days with at least one red folder event.
    A day with multiple red folder events counts as 1 red folder day.
    Threshold recomputed once per calendar day — on the first genuinely-
    live EC cycle of that day — then held for the rest of the day.
    EC's red-folder-day count changes from calendar edits, not live market
    movement, so same-day freshness is what matters, not same-cycle.
    Persisted to PROP_FIRM_THRESHOLD_FILE, keyed by date (with the ISO
    week also stored, to distinguish a daily refresh from an actual new
    week for the "new week detected" log). On a cycle where EC data isn't
    usable, the last computed day's value is held rather than recomputed
    from empty data. A distinct log line fires whenever the quiet/standard
    classification itself flips relative to the last persisted value.
    """

    _WEEK_WEIGHTS = {
        'standard': {'economic_calendar': 30, 'geopolitical': 25, 'institutional': 25, 'macro_sentiment': 20},
        'quiet':    {'economic_calendar': 15, 'geopolitical': 25, 'institutional': 25, 'macro_sentiment': 20},
    }

    def _get_weekly_threshold(self, econ_data):
        """Return week mode dict. Recomputes once per calendar day — on the
        first genuinely-live EC cycle of that day — then holds for the rest
        of the day. EC's red-folder-day count changes because of calendar
        edits (new/reclassified events), not live market movement, so
        same-day freshness is what's needed, not same-cycle (5-min) freshness.

        Returns dict with keys:
          bias_threshold, red_folder_days, is_new_week, is_quiet_week,
          ec_weight, total_weight, alignment_threshold
        """
        now = datetime.now(self.timezone)
        iso = now.isocalendar()
        current_week = (iso[0], iso[1])
        today_str = now.strftime('%Y-%m-%d')

        prior_cached = None
        try:
            if os.path.exists(PROP_FIRM_THRESHOLD_FILE):
                with open(PROP_FIRM_THRESHOLD_FILE, 'r') as f:
                    on_disk = json.load(f)
                if on_disk.get('date') == today_str and 'is_quiet_week' in on_disk:
                    # Already computed today — hold for the rest of the day.
                    return {
                        'bias_threshold': on_disk['threshold'],
                        'red_folder_days': on_disk['red_folder_days'],
                        'is_new_week': False,
                        'is_quiet_week': on_disk['is_quiet_week'],
                        'ec_weight': on_disk['ec_weight'],
                        'total_weight': on_disk['total_weight'],
                        'alignment_threshold': on_disk['alignment_threshold'],
                    }
                prior_cached = on_disk  # from a previous day — change-log baseline
        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm threshold cache read failed: {e}", level="WARNING")

        events = econ_data.get('events', []) if econ_data else []
        status = (econ_data or {}).get('status')
        should_defer = status in ('unavailable', 'stale') or not events

        if should_defer:
            pulse_logger.log(
                f"⏳ Prop Firm weekly threshold — EC data not yet live this cycle "
                f"(status={status!r}, {len(events)} events) — deferring today's recompute, will retry next cycle"
            )
            if prior_cached:
                # Hold the last computed day's value until a live cycle today
                # actually succeeds — don't flicker to a fresh empty-data read.
                return {
                    'bias_threshold': prior_cached['threshold'],
                    'red_folder_days': prior_cached['red_folder_days'],
                    'is_new_week': False,
                    'is_quiet_week': prior_cached['is_quiet_week'],
                    'ec_weight': prior_cached['ec_weight'],
                    'total_weight': prior_cached['total_weight'],
                    'alignment_threshold': prior_cached['alignment_threshold'],
                }
            # No prior value at all — compute honestly from whatever econ_data
            # has (as before) but don't persist it.
            red_folder_days = economic_calendar_pipeline._count_red_folder_days(events)
            is_quiet = red_folder_days <= 1
            threshold = 0.30 if is_quiet else 0.33
            ec_weight = 15 if is_quiet else 30
            total_weight = 85 if is_quiet else 100
            return {
                'bias_threshold': threshold,
                'red_folder_days': red_folder_days,
                'is_new_week': True,
                'is_quiet_week': is_quiet,
                'ec_weight': ec_weight,
                'total_weight': total_weight,
                'alignment_threshold': round(total_weight * 0.45, 2),
            }

        # Canonical count — shared with economic_calendar.py's weak_ec_week
        # determination so the two paths can't silently drift apart again.
        # Genuinely live EC data — first live cycle of the new calendar day.
        red_folder_days = economic_calendar_pipeline._count_red_folder_days(events)
        is_quiet = red_folder_days <= 1
        threshold = 0.30 if is_quiet else 0.33
        ec_weight = 15 if is_quiet else 30
        total_weight = 85 if is_quiet else 100
        alignment_threshold = round(total_weight * 0.45, 2)  # 38.25 (quiet) or 45.0 (standard)

        if prior_cached and prior_cached['is_quiet_week'] != is_quiet:
            old_label = 'Quiet' if prior_cached['is_quiet_week'] else 'Standard'
            new_label = 'Quiet' if is_quiet else 'Standard'
            direction = 'added' if red_folder_days > prior_cached['red_folder_days'] else 'removed'
            pulse_logger.log(
                f"⚠️ Week classification changed: {old_label} → {new_label} "
                f"(EC {prior_cached['ec_weight']}% → {ec_weight}%, bias ±{prior_cached['threshold']} → ±{threshold}) "
                f"— red folder day {direction} since last check "
                f"({prior_cached['red_folder_days']} → {red_folder_days})"
            )

        is_new_week = prior_cached is None or prior_cached.get('week') != list(current_week)

        try:
            atomic_write_json(PROP_FIRM_THRESHOLD_FILE, {
                'week': list(current_week),
                'date': today_str,
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
            'is_new_week': is_new_week,
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
            if confidence < 55:
                return self._no_rec(week_info)

            total_w = week_info['total_weight']
            if confidence >= 65:
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
