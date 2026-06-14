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
        self.snapshot_dir = "/data/snapshots/daily"

    def get_regime_consistency(self):
        """Read last 10 snapshots, calculate regime consistency over trading days."""
        try:
            if not os.path.exists(self.snapshot_dir):
                return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

            raw_files = [f for f in os.listdir(self.snapshot_dir) if f.endswith('.json')]
            if not raw_files:
                return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

            loaded = []
            for f in raw_files:
                try:
                    with open(os.path.join(self.snapshot_dir, f), 'r') as fp:
                        snap = json.load(fp)
                    bias = snap.get('bias', {})
                    timestamp = snap.get('timestamp', '')
                    if bias and timestamp:
                        loaded.append((timestamp, snap))
                except Exception as e:
                    pulse_logger.log(f"⚠️ Snapshot read failed: {e}", level="WARNING")

            # Sort newest-first by the timestamp field inside the snapshot content
            loaded.sort(key=lambda t: t[0], reverse=True)

            snapshots = []
            for timestamp, snap in loaded[:20]:
                try:
                    bias = snap.get('bias', {})
                    dt = datetime.fromisoformat(timestamp)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=pytz.utc)
                    est = dt.astimezone(self.timezone)
                    if est.weekday() in [5, 6]:
                        continue
                    snapshots.append({
                        'bias': bias.get('bias', 'Neutral'),
                        'confidence': bias.get('confidence', 0),
                        'date': est.strftime('%Y-%m-%d')
                    })
                except Exception as e:
                    pulse_logger.log(f"⚠️ Snapshot parse failed: {e}", level="WARNING")
                    continue

            if not snapshots:
                return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

            # Deduplicate by date — take last snapshot per day
            seen_dates = {}
            for snap in snapshots:
                date = snap['date']
                if date not in seen_dates:
                    seen_dates[date] = snap

            daily_snaps = list(seen_dates.values())

            # Count consecutive days same direction starting from most recent
            if not daily_snaps:
                return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

            base_direction = daily_snaps[0]['bias']
            if base_direction == 'Neutral':
                return {'consistent': False, 'days': 0, 'direction': 'Neutral', 'avg_confidence': 0}

            streak = 0
            confidences = []
            for snap in daily_snaps:
                if snap['bias'] == base_direction and snap['confidence'] >= 50:
                    streak += 1
                    confidences.append(snap['confidence'])
                elif snap['bias'] == 'Neutral':
                    continue
                else:
                    break

            avg_confidence = round(sum(confidences) / len(confidences), 1) if confidences else 0
            consistent = streak >= 2 and avg_confidence >= 55

            return {
                'consistent': consistent,
                'days': streak,
                'direction': base_direction,
                'avg_confidence': avg_confidence
            }
        except Exception as e:
            pulse_logger.log(f"⚠️ Regime consistency check failed: {e}", level="WARNING")
            return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

    def _topic_keywords(self, headline):
        """Extract significant topic words from a headline for overlap matching."""
        keywords = {'iran', 'hormuz', 'ceasefire', 'fed', 'warsh', 'tariff', 'tariffs',
                    'china', 'russia', 'ukraine', 'israel', 'gaza', 'taiwan', 'nato',
                    'opec', 'oil', 'sanctions', 'nuclear', 'rate', 'rates', 'inflation',
                    'recession', 'default', 'debt', 'powell', 'treasury'}
        words = set(headline.lower().split())
        return words & keywords

    def _is_superseded(self, item, all_items):
        """Return True if a newer article covers the same story with a conflicting direction."""
        item_ts = item.get('timestamp') or item.get('published_at') or item.get('date') or ''
        item_direction = item.get('direction', '')
        item_keywords = self._topic_keywords(item.get('headline', ''))

        if not item_keywords or not item_direction or item_direction == 'neutral':
            return False

        for other in all_items:
            if other is item:
                continue
            other_ts = other.get('timestamp') or other.get('published_at') or other.get('date') or ''
            other_direction = other.get('direction', '')
            if not other_ts or not other_direction or other_direction == 'neutral':
                continue
            # Must be newer
            if other_ts <= item_ts:
                continue
            # Must conflict in direction
            conflicting = {('bullish', 'bearish'), ('bearish', 'bullish')}
            if (item_direction, other_direction) not in conflicting:
                continue
            # Must share topic keywords
            other_keywords = self._topic_keywords(other.get('headline', ''))
            if item_keywords & other_keywords:
                return True
        return False

    def get_uncertainty_signal(self, geo_data):
        """Get highest uncertainty score and count of high-uncertainty articles."""
        if not geo_data:
            return {'max_score': 0, 'high_count': 0, 'signal': 'none'}

        items = geo_data.get('news_items', [])
        flags = geo_data.get('active_flags', [])

        uncertainty_scores = []
        for item in items:
            score = item.get('uncertainty_score', 0)
            if score:
                if self._is_superseded(item, items):
                    pulse_logger.log(f"📰 Uncertainty score suppressed (superseded): {item.get('headline', '')[:60]}", level="DEBUG")
                    continue
                uncertainty_scores.append(score)

        # Also check flags for uncertainty context
        for flag in flags:
            score = flag.get('uncertainty_score', 0)
            if score:
                uncertainty_scores.append(score)

        if not uncertainty_scores:
            return {'max_score': 0, 'high_count': 0, 'signal': 'none'}

        max_score = max(uncertainty_scores)
        high_count = sum(1 for s in uncertainty_scores if s >= 70)

        if max_score >= 70:
            signal = 'high'
        elif max_score >= 40:
            signal = 'medium'
        else:
            signal = 'low'

        return {
            'max_score': max_score,
            'high_count': high_count,
            'signal': signal
        }

    def compute(self, bias_data, geo_data, macro_data):
        """Generate size recommendation based on 3 sources."""
        try:
            # Source 1 — Gemini uncertainty signal
            uncertainty = self.get_uncertainty_signal(geo_data)

            # Source 2 — VIX confirmatory
            vix = macro_data.get('vix', {}) if macro_data else {}
            vix_value = vix.get('value', 0) or 0
            vix_elevated = vix_value >= 22

            # Source 3 — Regime consistency
            consistency = self.get_regime_consistency()

            bias = bias_data.get('bias', 'Neutral') if bias_data else 'Neutral'
            confidence = bias_data.get('confidence', 0) if bias_data else 0

            # --- Decision logic ---

            # Neutral or very low confidence — no recommendation needed
            if bias == 'Neutral' or confidence < 20:
                return None

            # Below 55% — always quarter, regardless of other conditions
            if confidence < 55:
                return {
                    'mode': 'quarter',
                    'label': 'Conditions suggest: Quarter size',
                    'reason': 'Confidence not yet strong enough for Normal size',
                    'strength': 'weak'
                }

            # High uncertainty event active
            if uncertainty['signal'] == 'high':
                if uncertainty['high_count'] >= 2:
                    # Multiple high uncertainty events
                    return {
                        'mode': 'quarter',
                        'label': 'Conditions suggest: Quarter size',
                        'reason': 'Multiple high-uncertainty events active — execution conditions fragmented',
                        'strength': 'strong'
                    }
                else:
                    # Single high uncertainty event
                    if vix_elevated:
                        return {
                            'mode': 'quarter',
                            'label': 'Conditions suggest: Quarter size',
                            'reason': 'High-uncertainty event active · VIX elevated — wait for clarity',
                            'strength': 'strong'
                        }
                    else:
                        return {
                            'mode': 'quarter',
                            'label': 'Conditions suggest: Quarter size',
                            'reason': 'High-uncertainty event active — monitor before sizing up',
                            'strength': 'moderate'
                        }

            # Medium uncertainty — check VIX and consistency
            if uncertainty['signal'] == 'medium':
                if vix_elevated:
                    return {
                        'mode': 'quarter',
                        'label': 'Conditions suggest: Quarter size',
                        'reason': 'Developing event + elevated VIX — conditions not yet clear',
                        'strength': 'moderate'
                    }
                elif consistency['consistent'] and consistency['direction'] == bias:
                    return {
                        'mode': 'normal',
                        'label': 'Conditions support: Normal size',
                        'reason': f"Regime consistent {consistency['days']} days · Developing event but conditions manageable",
                        'strength': 'moderate'
                    }
                else:
                    return {
                        'mode': 'quarter',
                        'label': 'Conditions suggest: Quarter size',
                        'reason': 'Developing event active — regime not yet confirmed consistent',
                        'strength': 'weak'
                    }

            # Low or no uncertainty
            if consistency['consistent'] and consistency['direction'] == bias:
                return {
                    'mode': 'normal',
                    'label': 'Conditions support: Normal size',
                    'reason': f"Regime consistent {consistency['days']} days · {int(consistency['avg_confidence'])}% avg confidence · Conditions calm",
                    'strength': 'strong'
                }
            elif vix_elevated:
                return {
                    'mode': 'quarter',
                    'label': 'Conditions suggest: Quarter size',
                    'reason': 'VIX elevated — volatility not fully calm despite no major uncertainty events',
                    'strength': 'weak'
                }
            else:
                return {
                    'mode': 'quarter',
                    'label': 'Conditions suggest: Quarter size',
                    'reason': 'Regime not yet consistent enough to support Normal size',
                    'strength': 'weak'
                }

        except Exception as e:
            pulse_logger.log(f"⚠️ Recommendation engine failed: {e}", level="WARNING")
            return None

recommendation_engine = RecommendationEngine()


class PropFirmRecommendationEngine(RecommendationEngine):
    """Prop Firm recommendation — same pillar data, aggressive entry thresholds.

    Differences from Live:
      Bias threshold         ±0.30 quiet week (≤1 red folder day) / ±0.33 standard week (≥2)  (Live ±0.50)
      Show-card confidence     30%  (Live 20%)
      Quarter-entry confidence 35%  (Live 55%)
      Pillar alignment         ≥45% of total week weight must agree with bias
                               Quiet week: EC 15%, total 85%, threshold ≥38.25%
                               Standard week: EC 30%, total 100%, threshold ≥45%  (Live: none)
      Consistency streak       0 days  (Live 2 days)
      VIX hard limit           ≤ 26  (Live ≤ 22)
      High-uncertainty block   3+ articles ≥ 70  (Live 2+)

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
            uncertainty = self.get_uncertainty_signal(geo_data)
            vix = macro_data.get('vix', {}) if macro_data else {}
            vix_value = vix.get('value', 0) or 0
            vix_elevated = vix_value >= 26

            # Bias threshold and pillar weights set once per ISO week
            week_info = self._get_weekly_threshold(econ_data)
            is_quiet = week_info['is_quiet_week']
            bias_threshold = week_info['bias_threshold']
            ec_weight = week_info['ec_weight']
            alignment_threshold = week_info['alignment_threshold']
            red_folder_days = week_info['red_folder_days']

            # Log new week detection
            if week_info['is_new_week']:
                mode_label = 'quiet' if is_quiet else 'standard'
                day_s = 'day' if red_folder_days == 1 else 'days'
                pulse_logger.log(
                    f"📊 Prop Firm — new week detected: {mode_label} "
                    f"({red_folder_days} red folder {day_s})"
                )

            # Log every cycle
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

            # Pillar alignment: agreeing pillars must cover ≥45% of total week weight
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
            if confidence < 30:
                return self._no_rec(week_info)

            # Hard blocks
            if uncertainty['signal'] == 'high' and uncertainty['high_count'] >= 3:
                return self._rec(week_info,
                    mode='quarter',
                    label=f'Prop Firm — {bias}, Quarter entry',
                    reason='Multiple high-uncertainty events active — execution conditions fragmented',
                    strength='strong',
                    bias=bias,
                )

            if vix_elevated:
                return self._rec(week_info,
                    mode='quarter',
                    label=f'Prop Firm — {bias}, Quarter entry',
                    reason='VIX ≥ 26 — stay at quarter',
                    strength='moderate',
                    bias=bias,
                )

            if confidence < 35:
                return self._rec(week_info,
                    mode='quarter',
                    label=f'Prop Firm — {bias}, Quarter entry',
                    reason=f'Confidence {confidence}% — building toward Normal',
                    strength='weak',
                    bias=bias,
                )

            if uncertainty['signal'] == 'high':
                return self._rec(week_info,
                    mode='quarter',
                    label=f'Prop Firm — {bias}, Quarter entry',
                    reason='High-uncertainty event active — stay at quarter',
                    strength='moderate',
                    bias=bias,
                )

            if uncertainty['signal'] == 'medium':
                return self._rec(week_info,
                    mode='normal',
                    label=f'Prop Firm — {bias}, Normal entry',
                    reason='Developing event manageable · Conditions met',
                    strength='moderate',
                    bias=bias,
                )

            total_w = week_info['total_weight']
            return self._rec(week_info,
                mode='normal',
                label=f'Prop Firm — {bias}, Normal entry',
                reason=f'{aligned_weight}% of {total_w}% weight aligned · Conditions clear',
                strength='strong',
                bias=bias,
            )

        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm recommendation engine failed: {e}", level="WARNING")
            return None


prop_firm_engine = PropFirmRecommendationEngine()
