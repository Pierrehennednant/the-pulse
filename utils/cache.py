import json
import os
from datetime import datetime
from utils.file_lock import atomic_write_json

class Cache:
    def __init__(self, cache_dir="./data"):
        self.cache_dir = cache_dir
        self._ensure_exists()
    
    def _ensure_exists(self):
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def save(self, key, data):
        cache_file = os.path.join(self.cache_dir, f"{key}.json")
        payload = {
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        atomic_write_json(cache_file, payload)
    
    def load(self, key):
        cache_file = os.path.join(self.cache_dir, f"{key}.json")
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                return json.load(f)
        return None
    
    def get_age_minutes(self, key):
        cached = self.load(key)
        if not cached:
            return float('inf')
        cached_time = datetime.fromisoformat(cached['timestamp'])
        return (datetime.now() - cached_time).total_seconds() / 60
    
    def is_stale(self, key, threshold_minutes):
        return self.get_age_minutes(key) > threshold_minutes

cache = Cache()
