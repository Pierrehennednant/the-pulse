import json
import os
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler
import requests
from bs4 import BeautifulSoup

class ManualInputPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "manual_inputs"
        self.headers = {'User-Agent': 'Mozilla/5.0'}

    def save_actual(self, event_title, actual_value, story_url=None):
        try:
            existing = cache.load(self.cache_key)
            inputs = existing['data'] if existing else {}

            story_context = None
            if story_url:
                story_context = self.fetch_story_context(story_url)

            inputs[event_title] = {
                'actual': actual_value,
                'story_url': story_url,
                'story_context': story_context,
                'timestamp': datetime.now(self.timezone).isoformat()
            }

            cache.save(self.cache_key, inputs)
            pulse_logger.log(f"✓ Manual input saved | {event_title}: {actual_value}")
            return True
        except Exception as e:
            error_handler.handle(e, "Manual Input")
            return False

    def fetch_story_context(self, url):
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text(strip=True) for p in paragraphs[:5]])
            return text[:500] if text else None
        except Exception as e:
            error_handler.handle(e, "Story Context Fetcher")
            return None

    def get_inputs(self):
        existing = cache.load(self.cache_key)
        return existing['data'] if existing else {}

    def clear_old_inputs(self):
        try:
            existing = cache.load(self.cache_key)
            if not existing:
                return
            inputs = existing['data']
            now = datetime.now(self.timezone)
            fresh = {}
            for title, data in inputs.items():
                try:
                    ts = datetime.fromisoformat(data['timestamp'])
                    if (now - ts).total_seconds() < 86400:
                        fresh[title] = data
                except:
                    pass
            cache.save(self.cache_key, fresh)
        except Exception as e:
            error_handler.handle(e, "Manual Input Cleanup")

manual_input_pipeline = ManualInputPipeline()
