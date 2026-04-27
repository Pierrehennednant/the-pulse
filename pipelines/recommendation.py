import json
import os
from datetime import datetime, timedelta
import pytz
from config import TIMEZONE
from utils.logger import pulse_logger

class RecommendationEngine:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.snapshot_dir = "/data/snapshots/daily"

    def get_regime_consistency(self):
        """Read last 10 snapshots, calculate regime consistency over trading days."""
        try:
            if not os.path.exists(self.snapshot_dir):
                return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

            files = sorted(
                [f for f in os.listdir(self.snapshot_dir) if f.endswith('.json')],
                key=lambda f: os.path.getmtime(os.path.join(self.snapshot_dir, f)),
                reverse=True
            )[:20]

            if not files:
                return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

            snapshots = []
            for f in files:
                try:
                    with open(os.path.join(self.snapshot_dir, f), 'r') as fp:
                        snap = json.load(fp)
                        bias = snap.get('bias', {})
                        timestamp = snap.get('timestamp', '')
                        if bias and timestamp:
                            dt = datetime.fromisoformat(timestamp)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=pytz.utc)
                            # Skip weekends
                            est = dt.astimezone(self.timezone)
                            if est.weekday() in [5, 6]:
                                continue
                            snapshots.append({
                                'bias': bias.get('bias', 'Neutral'),
                                'confidence': bias.get('confidence', 0),
                                'date': est.strftime('%Y-%m-%d')
                            })
                except Exception as e:
                    pulse_logger.log(f"⚠️ Snapshot read failed: {e}", level="WARNING")
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

            # Below 50% confidence — card doesn't show at all
            if confidence < 50:
                return None

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
                    if confidence >= 55:
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
                            'reason': 'Confidence not yet strong enough for Normal size',
                            'strength': 'weak'
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
                if confidence >= 55:
                    return {
                        'mode': 'normal',
                        'label': 'Conditions support: Normal size',
                        'reason': f"Regime consistent {consistency['days']} days · {int(consistency['avg_confidence'])}% avg confidence · Conditions calm",
                        'strength': 'strong'
                    }
                else:
                    return {
                        'mode': 'quarter',
                        'label': 'Conditions suggest: Quarter size',
                        'reason': 'Confidence not yet strong enough for Normal size',
                        'strength': 'weak'
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
