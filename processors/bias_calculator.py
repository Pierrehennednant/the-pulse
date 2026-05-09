from datetime import datetime
import pytz
from config import PILLAR_WEIGHTS_ESCALATION, PILLAR_WEIGHTS_EXPANSION, TIMEZONE
from utils.logger import pulse_logger

class BiasCalculator:
    def compute(self, formatted_data, size_mode='quarter', regime='escalation', calm_days_count=0, high_uncertainty_count=0, stability_score=50):
        total_score = 0.0
        pillar_contributions = {}
        active_pillars = 0
        pillar_signals = []
        threshold_warning = None

        weights = PILLAR_WEIGHTS_EXPANSION if regime == 'expansion' else PILLAR_WEIGHTS_ESCALATION

        weight_map = {
            'economic': 'economic_calendar',
            'geopolitical': 'geopolitical',
            'institutional': 'institutional',
            'macro': 'macro_sentiment'
        }

        for data_key, config_key in weight_map.items():
            pillar_data = formatted_data.get(data_key, {})
            score = pillar_data.get('pillar_score', 0)
            weight = weights.get(config_key, 0)
            status = pillar_data.get('status', 'unavailable')

            # COT decay — stale weekly data weighs less as the week progresses
            if config_key == 'institutional':
                today = datetime.now(pytz.timezone(TIMEZONE)).weekday()
                if today in [5, 6]:  # Weekend — COT data maximally stale, contribute nothing
                    weight = 0
                    pulse_logger.log("📉 COT decay — weekend, institutional weight zeroed")
                elif today not in [4]:  # Mon–Thu — apply progressive decay
                    # Mon=0: 80%, Tue=1: 60%, Wed=2: 40%, Thu=3: 20%
                    cot_decay = {0: 0.8, 1: 0.6, 2: 0.4, 3: 0.2}
                    decay_factor = cot_decay[today]
                    weight = weight * decay_factor
                    pulse_logger.log(f"📉 COT decay applied — {int(decay_factor * 100)}% weight ({weight:.1f}% effective)")

            contribution = score * (weight / 100)
            total_score += contribution

            if status not in ['unavailable'] and score != 0:
                active_pillars += 1
                if score > 0.2:
                    pillar_signals.append('bullish')
                elif score < -0.2:
                    pillar_signals.append('bearish')
                else:
                    pillar_signals.append('neutral')

            pillar_contributions[config_key] = {
                'raw_score': score,
                'weight': weight,
                'contribution': round(contribution, 3),
                'status': status
            }

        final_score = round(total_score, 3)

        if final_score >= 0.5:
            bias = 'Bullish'
            bias_emoji = '🟢'
        elif final_score <= -0.5:
            bias = 'Bearish'
            bias_emoji = '🔴'
        else:
            bias = 'Neutral'
            bias_emoji = '🟡'

        if active_pillars == 0:
            confidence = 0
            confidence_label = 'No Data'
            confidence_color = 'gray'
            threshold_warning = None
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

            # Persistence bonus — uses calm_days_count from regime detector
            if regime == 'expansion':
                persistence_bonus = min(calm_days_count * 2, 15)
            else:
                persistence_bonus = min(calm_days_count * 1, 8)

            confidence = int(min(confidence + persistence_bonus, 95))

            # Uncertainty dampening in escalation
            if regime == 'escalation' and high_uncertainty_count >= 2:
                confidence = int(confidence * 0.85)

            # Stability micro-adjustment — smooths noisy confidence fluctuations
            # Capped at ±5 points to avoid overriding primary signals
            stability_adjustment = int((stability_score - 50) / 10)
            stability_adjustment = max(-5, min(5, stability_adjustment))
            confidence = int(min(max(confidence + stability_adjustment, 0), 95))

            if confidence >= 70:
                confidence_label = 'High Confidence'
                confidence_color = 'green'
                threshold_warning = None
            elif confidence >= 40:
                confidence_label = 'Moderate Confidence'
                confidence_color = 'yellow'
                threshold_warning = None
            elif confidence >= 20:
                confidence_label = 'Low Confidence'
                confidence_color = 'orange'
                threshold_warning = f"⚠️ Low confidence ({confidence}%) — regime is unclear. Consider sitting out today."
            else:
                confidence_label = 'Very Low Confidence'
                confidence_color = 'red'
                threshold_warning = f"🚫 Very low confidence ({confidence}%) — pillars conflicting. Sit out."

        # Trading Directive — two size modes: quarter and normal (half)
        if bias == 'Neutral':
            directive = "🟡 Neutral — Sit out."
            directive_color = "#f39c12"
        elif confidence < 20:
            directive = "⚫ Conflicted — Sit out."
            directive_color = "#7a8fa8"
        elif size_mode == 'quarter':
            # Quarter size mode
            if bias == 'Bearish' and confidence >= 70:
                directive = "🔴 Bearish — Quarter first. Scale to half, then full on confirmation."
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 50:
                directive = "🔴 Bearish — Quarter first. Scale to half on confirmation only."
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 20:
                directive = "🟠 Bearish lean — Quarter only. No scaling."
                directive_color = "#ff8c00"
            elif bias == 'Bullish' and confidence >= 70:
                directive = "🟢 Bullish — Quarter first. Scale to half, then full on confirmation."
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 50:
                directive = "🟢 Bullish — Quarter first. Scale to half on confirmation only."
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 20:
                directive = "🟠 Bullish lean — Quarter only. No scaling."
                directive_color = "#ff8c00"
            else:
                directive = "⚫ Conflicted — Sit out."
                directive_color = "#7a8fa8"
        else:
            # Normal mode (half size)
            if bias == 'Bearish' and confidence >= 70:
                directive = "🔴 Bearish — Half first. Scale to full on confirmation."
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 50:
                directive = "🔴 Bearish — Half first. Scale to full on confirmation only."
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 20:
                directive = "🟠 Bearish lean — Half only. No scaling."
                directive_color = "#ff8c00"
            elif bias == 'Bullish' and confidence >= 70:
                directive = "🟢 Bullish — Half first. Scale to full on confirmation."
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 50:
                directive = "🟢 Bullish — Half first. Scale to full on confirmation only."
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 20:
                directive = "🟠 Bullish lean — Half only. No scaling."
                directive_color = "#ff8c00"
            else:
                directive = "⚫ Conflicted — Sit out."
                directive_color = "#7a8fa8"

        result = {
            'final_score': final_score,
            'bias': bias,
            'bias_emoji': bias_emoji,
            'confidence': confidence,
            'confidence_label': confidence_label,
            'confidence_color': confidence_color,
            'threshold_warning': threshold_warning if confidence < 40 else None,
            'active_pillars': active_pillars,
            'pillar_signals': pillar_signals,
            'pillar_contributions': pillar_contributions,
            'gauge_value': int((final_score + 2) / 4 * 100),
            'directive': directive,
            'directive_color': directive_color,
            'size_mode': size_mode,
            'stability_score': stability_score,
            'regime': regime
        }

        pulse_logger.log(f"📊 Bias: {bias_emoji} {bias} | Confidence: {confidence}% ({confidence_label}) | Active Pillars: {active_pillars}/4 | Mode: {size_mode} | Regime: {regime}")
        return result

bias_calculator = BiasCalculator()
