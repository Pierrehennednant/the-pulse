from datetime import datetime
import pytz
from config import PILLAR_WEIGHTS, TIMEZONE
from utils.logger import pulse_logger

class BiasCalculator:
    def compute(self, formatted_data, size_mode='quarter', bias_threshold=0.5):
        total_score = 0.0
        pillar_contributions = {}
        active_pillars = 0
        pillar_signals = []
        weights = PILLAR_WEIGHTS

        weight_map = {
            'economic': 'economic_calendar',
            'geopolitical': 'geopolitical',
            'institutional': 'institutional',
            'macro': 'macro_sentiment'
        }

        for data_key, config_key in weight_map.items():
            pillar_data = formatted_data.get(data_key, {})
            score = pillar_data.get('pillar_score', 0)
            base_weight = weights.get(config_key, 0)
            weight = base_weight
            status = pillar_data.get('status', 'unavailable')

            # COT decay — stale weekly data weighs less as the week progresses
            if config_key == 'institutional':
                now = datetime.now(pytz.timezone(TIMEZONE))
                today = now.weekday()
                if today in [5, 6]:  # Weekend — COT data maximally stale, contribute nothing
                    weight = 0
                    pulse_logger.log("📉 COT decay — weekend, institutional weight zeroed")
                else:
                    if today == 4:  # Friday — 55% floor until 3:30 PM EST; full weight once COT posts
                        cutoff = now.replace(hour=15, minute=30, second=0, microsecond=0)
                        decay_factor = 1.0 if now >= cutoff else 0.55
                    else:
                        # Mon=0: 100%, Tue=1: 85%, Wed=2: 70%, Thu=3: 55%
                        decay_factor = {0: 1.0, 1: 0.85, 2: 0.70, 3: 0.55}[today]
                    weight = weight * decay_factor
                    if decay_factor < 1.0:
                        pulse_logger.log(f"📉 COT decay applied — {int(decay_factor * 100)}% of {base_weight}% base = {weight:.1f}% effective")
                    elif today == 0:
                        pulse_logger.log(f"✅ COT weight — Monday, full {base_weight}% effective")

            contribution = score * (weight / 100)
            total_score += contribution

            if status not in ['unavailable'] and score != 0:
                active_pillars += 1
                if score > 0.15:
                    pillar_signals.append('bullish')
                elif score < -0.15:
                    pillar_signals.append('bearish')
                else:
                    pillar_signals.append('neutral')

            pillar_contributions[config_key] = {
                'raw_score': score,
                'base_weight': base_weight,
                'weight': weight,
                'contribution': round(contribution, 3),
                'status': status
            }

        final_score = round(total_score, 3)

        if final_score >= bias_threshold:
            bias = 'Bullish'
            bias_emoji = '🟢'
        elif final_score <= -bias_threshold:
            bias = 'Bearish'
            bias_emoji = '🔴'
        else:
            bias = 'Neutral'
            bias_emoji = '🟡'

        if active_pillars == 0:
            confidence = 0
            confidence_label = 'No Data'
            confidence_color = 'gray'
        else:
            if bias == 'Bullish':
                agreeing = pillar_signals.count('bullish')
            elif bias == 'Bearish':
                agreeing = pillar_signals.count('bearish')
            else:
                agreeing = pillar_signals.count('neutral')

            agreement_pct = agreeing / active_pillars
            score_strength = min(abs(final_score) / 2.0, 1.0)
            confidence = int((agreement_pct * 0.6 + score_strength * 0.4) * 100)

            if confidence >= 70:
                confidence_label = 'High Confidence'
                confidence_color = 'green'
            elif confidence >= 60:
                confidence_label = 'Moderate Confidence'
                confidence_color = 'yellow'
            else:
                confidence_label = 'Low Conviction'
                confidence_color = 'orange'

        # Hard Neutral override — below 60% confidence forces Neutral regardless of score
        low_conviction_override = confidence < 60 and bias != 'Neutral'
        if low_conviction_override:
            bias = 'Neutral'
            bias_emoji = '🟡'

        # Trading Directive — confidence-based only
        if low_conviction_override:
            directive = "⚫ No Trade – Low Conviction."
            directive_color = "#7a8fa8"
        elif bias == 'Neutral':
            directive = "🟡 Neutral — Sit out."
            directive_color = "#f39c12"
        elif confidence >= 70:
            if bias == 'Bearish':
                directive = "🔴 Bearish — Half size."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — Half size."
                directive_color = "#2ecc71"
        else:
            if bias == 'Bearish':
                directive = "🔴 Bearish — Quarter size."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — Quarter size."
                directive_color = "#2ecc71"

        result = {
            'final_score': final_score,
            'bias': bias,
            'bias_emoji': bias_emoji,
            'confidence': confidence,
            'confidence_label': confidence_label,
            'confidence_color': confidence_color,
            'threshold_warning': None,
            'active_pillars': active_pillars,
            'pillar_signals': pillar_signals,
            'pillar_contributions': pillar_contributions,
            'gauge_value': int((final_score + 2) / 4 * 100),
            'directive': directive,
            'directive_color': directive_color,
            'size_mode': size_mode
        }

        pulse_logger.log(f"📊 Bias: {bias_emoji} {bias} | Confidence: {confidence}% ({confidence_label}) | Active Pillars: {active_pillars}/4 | Mode: {size_mode}")
        return result

bias_calculator = BiasCalculator()
