from datetime import datetime
import pytz
from config import TIMEZONE
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class NewsSentimentPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "news_sentiment"

    def analyze(self, geo_data):
        try:
            if not geo_data:
                return None
            all_items = (
                geo_data.get('financial_juice_items', []) +
                geo_data.get('unbiased_network_items', [])
            )
            if not all_items:
                return None
            scores = [item['sentiment_score'] for item in all_items if 'sentiment_score' in item]
            if not scores:
                return None
            avg_score = sum(scores) / len(scores)
            bullish_count = sum(1 for s in scores if s > 0.3)
            bearish_count = sum(1 for s in scores if s < -0.3)
            neutral_count = len(scores) - bullish_count - bearish_count
            total = len(scores)
            flags = geo_data.get('active_flags', [])
            theme = flags[0]['title'][:100] if flags else "No dominant theme identified"
            overall = 'bullish' if avg_score > 0.2 else 'bearish' if avg_score < -0.2 else 'neutral'
            return {
                'average_score': round(avg_score, 3),
                'overall_sentiment': overall,
                'bullish_pct': round(bullish_count / total * 100, 1),
                'bearish_pct': round(bearish_count / total * 100, 1),
                'neutral_pct': round(neutral_count / total * 100, 1),
                'total_headlines': total,
                'dominant_theme': theme,
                'pillar_score': round(max(-2.0, min(2.0, avg_score * 2)), 2)
            }
        except Exception as e:
            error_handler.handle(e, "News Sentiment Analyzer")
            return None

    def fetch(self, geo_data=None):
        try:
            analysis = self.analyze(geo_data)
            result = {
                'pillar': 'news_sentiment',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'analysis': analysis,
                'pillar_score': analysis['pillar_score'] if analysis else 0,
                'status': 'live'
            }
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ News Sentiment updated | Overall: {analysis['overall_sentiment'] if analysis else 'N/A'}")
            return result
        except Exception as e:
            error_handler.handle(e, "News Sentiment")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return cached['data']
            return None

news_sentiment_pipeline = NewsSentimentPipeline()
