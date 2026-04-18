import json
import os
from datetime import datetime, timedelta
import pytz
from config import TIMEZONE
from utils.logger import pulse_logger

class RecommendationEngine:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.snapshot_dir = "/data/snapshots"

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
                else:
                    break

            avg_confidence = round(sum(confidences) / len(confidences), 1) if confidences else 0
            consistent = streak >= 3 and avg_confidence >= 65

            return {
                'consistent': consistent,
                'days': streak,
                'direction': base_direction,
                'avg_confidence': avg_confidence
            }
        except Exception as e:
            pulse_logger.log(f"⚠️ Regime consistency check failed: {e}", level="WARNING")
            return {'consistent': False, 'days': 0, 'direction': None, 'avg_confidence': 0}

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
