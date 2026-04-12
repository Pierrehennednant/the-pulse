from config import PILLAR_WEIGHTS
from utils.logger import pulse_logger

class BiasCalculator:
    def __init__(self):
        self.weights = PILLAR_WEIGHTS

    def compute(self, formatted_data, edi_result=None):
        total_score = 0.0
        pillar_contributions = {}
        active_pillars = 0
        pillar_signals = []
        threshold_warning = None

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

        # EDI integration — determine execution regime
        edi_regime = edi_result.get('regime', 'Normal') if edi_result else 'Normal'
        edi_reason = edi_result.get('reason', '') if edi_result else ''
        edi_emoji = edi_result.get('regime_emoji', '✅') if edi_result else '✅'
        edi_color = edi_result.get('regime_color', '#2ecc71') if edi_result else '#2ecc71'

        # Unified directive — direction from bias, sizing from EDI
        # Extreme volatility always overrides to sit out
        if edi_regime == 'Extreme':
            directive = f"🔴 Extreme Volatility — Sit out. No trade regardless of direction.\nReason: {edi_reason}"
            directive_color = '#e74c3c'
        elif bias == 'Neutral':
            directive = "🟡 Neutral — No trade today. Sit out."
            directive_color = "#f39c12"
        elif confidence < 20:
            directive = "⚫ Regime conflicted — Sit out."
            directive_color = "#7a8fa8"
        elif edi_regime == 'Elevated':
            if bias == 'Bearish' and confidence >= 70:
                directive = f"🔴 Bearish — Quarter size first entry. Scale to half only when confirmed. No full size today.\n{edi_emoji} Elevated volatility: {edi_reason}"
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 50:
                directive = f"🔴 Bearish — Quarter size only. No scaling until volatility drops.\n{edi_emoji} Elevated volatility: {edi_reason}"
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 20:
                directive = f"� Bearish lean — Quarter size only. Elevated volatility limits execution.\n{edi_emoji} {edi_reason}"
                directive_color = "#ff8c00"
            elif bias == 'Bullish' and confidence >= 70:
                directive = f"🟢 Bullish — Quarter size first entry. Scale to half only when confirmed. No full size today.\n{edi_emoji} Elevated volatility: {edi_reason}"
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 50:
                directive = f"🟢 Bullish — Quarter size only. No scaling until volatility drops.\n{edi_emoji} Elevated volatility: {edi_reason}"
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 20:
                directive = f"� Bullish lean — Quarter size only. Elevated volatility limits execution.\n{edi_emoji} {edi_reason}"
                directive_color = "#ff8c00"
            else:
                directive = "⚫ Regime conflicted — Sit out."
                directive_color = "#7a8fa8"
        else:
            # Normal conditions
            if bias == 'Bearish' and confidence >= 70:
                directive = "🔴 Bearish — Half size first entry. Scale to full only when confirmed."
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 50:
                directive = "🔴 Bearish — Half size first entry. No full size until confirmed."
                directive_color = "#e74c3c"
            elif bias == 'Bearish' and confidence >= 20:
                directive = "🟠 Bearish lean — Half size only. No scaling today."
                directive_color = "#ff8c00"
            elif bias == 'Bullish' and confidence >= 70:
                directive = "🟢 Bullish — Half size first entry. Scale to full only when confirmed."
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 50:
                directive = "🟢 Bullish — Half size first entry. No full size until confirmed."
                directive_color = "#2ecc71"
            elif bias == 'Bullish' and confidence >= 20:
                directive = "🟠 Bullish lean — Half size only. No scaling today."
                directive_color = "#ff8c00"
            else:
                directive = "⚫ Regime conflicted — Sit out."
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
            'edi': edi_result
        }

        pulse_logger.log(f"📊 Bias: {bias_emoji} {bias} | Confidence: {confidence}% | EDI: {edi_regime} | Directive set")
        return result

bias_calculator = BiasCalculator()
