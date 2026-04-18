import json
import os
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler
from bs4 import BeautifulSoup

class ManualInputPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "manual_inputs"
        self.permanent_file = "/data/permanent_manual_inputs.json"
        self.headers = {'User-Agent': 'Mozilla/5.0'}
        self._ensure_exists()

    def _ensure_exists(self):
        if not os.path.exists('/data'):
            os.makedirs('/data')
        if not os.path.exists(self.permanent_file):
            with open(self.permanent_file, 'w') as f:
                json.dump({}, f)

    def save_actual(self, event_title, actual_value, story_url=None):
        try:
            story_context = None
            if story_url:
                story_context = self.fetch_story_context(story_url)

            with open(self.permanent_file, 'r') as f:
                inputs = json.load(f)

            inputs[event_title] = {
                'actual': actual_value,
                'story_url': story_url,
                'story_context': story_context,
                'timestamp': datetime.now(self.timezone).isoformat()
            }

            with open(self.permanent_file, 'w') as f:
                json.dump(inputs, f, indent=2)

            pulse_logger.log(f"✓ Manual input saved permanently | {event_title}: {actual_value}")
            return True
        except Exception as e:
            error_handler.handle(e, "Manual Input")
            return False

    def fetch_story_context(self, url):
        try:
            response = fetch_with_retry(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text(strip=True) for p in paragraphs[:5]])
            return text[:500] if text else None
        except Exception as e:
            error_handler.handle(e, "Story Context Fetcher")
            return None

    def get_inputs(self):
        try:
            with open(self.permanent_file, 'r') as f:
                return json.load(f)
        except:
            return {}

    def clear_old_inputs(self):
        try:
            with open(self.permanent_file, 'r') as f:
                inputs = json.load(f)
            now = datetime.now(self.timezone)
            fresh = {}
            for title, data in inputs.items():
                try:
                    ts = datetime.fromisoformat(data['timestamp'])
                    if ts.tzinfo is None:
                        ts = self.timezone.localize(ts)
                    if (now - ts).total_seconds() < 86400:
                        fresh[title] = data
                except:
                    pass
            with open(self.permanent_file, 'w') as f:
                json.dump(fresh, f, indent=2)
        except Exception as e:
            error_handler.handle(e, "Manual Input Cleanup")

manual_input_pipeline = ManualInputPipeline()
