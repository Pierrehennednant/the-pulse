import json
import os
from datetime import datetime
import pytz
from config import TIMEZONE

COT_HISTORY_FILE = '/data/cot_history.json'

class DataFormatter:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)

    def _load_cot_history(self):
        try:
            if os.path.exists(COT_HISTORY_FILE):
                with open(COT_HISTORY_FILE, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return []

    def standardize(self, raw_data):
        formatted = {}
        for pillar_name, pillar_data in raw_data.items():
            if pillar_data is None:
                formatted[pillar_name] = {
                    'status': 'unavailable',
                    'pillar_score': 0,
                    'timestamp': datetime.now(self.timezone).isoformat()
                }
            else:
                formatted[pillar_name] = pillar_data
                if 'pillar_score' not in formatted[pillar_name]:
                    formatted[pillar_name]['pillar_score'] = 0

        if 'institutional' in formatted and formatted['institutional'].get('status') != 'unavailable':
            formatted['institutional']['cot_history'] = self._load_cot_history()

        return formatted

data_formatter = DataFormatter()
