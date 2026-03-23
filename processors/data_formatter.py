from datetime import datetime
import pytz
from config import TIMEZONE

class DataFormatter:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)

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
        return formatted

data_formatter = DataFormatter()
