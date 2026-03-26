from config import PILLAR_WEIGHTS
from utils.logger import pulse_logger

class BiasCalculator:
    def __init__(self):
        self.weights = PILLAR_WEIGHTS

    def compute(self, formatted_data):
        total_score = 0.0
        pillar_contributions = {}
        active_pillars = 0
        pillar_signals = []

        weight_map = {
            'economic': 'economic_calendar',
            'geopolitical': 'geopolitical',
            'institutional': 'institutional',
            'macro': 'macro_sentiment',
            'news': 'news_sentiment'
        }

        for data_key, config_key in weight_map.items():
            pillar_data = formatted_data.get(data_key, {})
            score = pillar_data.get('pillar_score', 0)
            weight = self.weights.get(config_key, 0)
            status = pillar_data.get('status', 'unavailable')
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
                threshold_warning = None
            elif confidence >= 40:
                confidence_label = 'Moderate Confidence'
                confidence_color = 'yellow'
                threshold_warning = None
            elif confidence >= 20:
                confidence_label = 'Low Confidence'
                confidence_color = 'orange'
                threshold_warning = f"⚠️ Low confidence ({confidence}%) — regime is unclear. Consider reducing size or sitting out today."
            else:
                confidence_label = 'Very Low Confidence'
                confidence_color = 'red'
                threshold_warning = f"🚫 Very low confidence ({confidence}%) — pillars are conflicting. Avoid trading until regime clarifies."

        # Trading Directive
        if bias == 'Neutral':
            directive = "🟡 Neutral — No trade today. Sit out."
            directive_color = "#f39c12"
        elif confidence < 20:
            directive = "⚫ Regime conflicted — Sit out. If you must trade, quarter size only."
            directive_color = "#7a8fa8"
        elif bias == 'Bearish' and confidence >= 70:
            directive = "🔴 Bearish — Prioritize shorts. Half size first entry, full size second entry."
            directive_color = "#e74c3c"
        elif bias == 'Bearish' and confidence >= 50:
            directive = "🔴 Bearish — Lean short. Half size first entry, no second entry until confirmed."
            directive_color = "#e74c3c"
        elif bias == 'Bearish' and confidence >= 20:
            directive = "🟠 Bearish lean — Regime unclear. Half size only, no second entry today."
            directive_color = "#ff8c00"
        elif bias == 'Bullish' and confidence >= 70:
            directive = "🟢 Bullish — Prioritize longs. Half size first entry, full size second entry."
            directive_color = "#2ecc71"
        elif bias == 'Bullish' and confidence >= 50:
            directive = "🟢 Bullish — Lean long. Half size first entry, no second entry until confirmed."
            directive_color = "#2ecc71"
        elif bias == 'Bullish' and confidence >= 20:
            directive = "🟠 Bullish lean — Regime unclear. Half size only, no second entry today."
            directive_color = "#ff8c00"
        else:
            directive = "⚫ Regime conflicted — Sit out. If you must trade, quarter size only."
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
            'directive_color': directive_color
        }

        pulse_logger.log(f"📊 Bias: {bias_emoji} {bias} | Score: {final_score} | Confidence: {confidence}% ({confidence_label}) | Active Pillars: {active_pillars}/5")
        return result

bias_calculator = BiasCalculator()
