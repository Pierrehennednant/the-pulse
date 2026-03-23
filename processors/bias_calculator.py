from config import PILLAR_WEIGHTS
from utils.logger import pulse_logger

class BiasCalculator:
    def __init__(self):
        self.weights = PILLAR_WEIGHTS

    def compute(self, formatted_data):
        total_score = 0.0
        pillar_contributions = {}

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
            contribution = score * (weight / 100)
            total_score += contribution
            pillar_contributions[config_key] = {
                'raw_score': score,
                'weight': weight,
                'contribution': round(contribution, 3)
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

        confidence = min(100, int(abs(final_score) / 2.0 * 100))

        result = {
            'final_score': final_score,
            'bias': bias,
            'bias_emoji': bias_emoji,
            'confidence': confidence,
            'pillar_contributions': pillar_contributions,
            'gauge_value': int((final_score + 2) / 4 * 100)
        }

        pulse_logger.log(f"📊 Bias: {bias_emoji} {bias} | Score: {final_score} | Confidence: {confidence}%")
        return result

bias_calculator = BiasCalculator()
