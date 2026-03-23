import json
import os
import hashlib
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.logger import pulse_logger

class SnapshotGenerator:
    def __init__(self, snapshot_dir="./data/snapshots"):
        self.snapshot_dir = snapshot_dir
        self.timezone = pytz.timezone(TIMEZONE)
        self._ensure_exists()

    def _ensure_exists(self):
        if not os.path.exists(self.snapshot_dir):
            os.makedirs(self.snapshot_dir)

    def generate_id(self, timestamp):
        return hashlib.md5(timestamp.encode()).hexdigest()[:8].upper()

    def save(self, bias_score, formatted_data):
        timestamp = datetime.now(self.timezone).isoformat()
        snapshot_id = self.generate_id(timestamp)
        snapshot = {
            'id': snapshot_id,
            'timestamp': timestamp,
            'bias': bias_score,
            'pillars': formatted_data
        }
        snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{snapshot_id}.json")
        with open(snapshot_file, 'w') as f:
            json.dump(snapshot, f, indent=2)
        pulse_logger.log(f"📸 Snapshot saved | ID: {snapshot_id}")
        return snapshot_id

    def load(self, snapshot_id):
        snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{snapshot_id}.json")
        if os.path.exists(snapshot_file):
            with open(snapshot_file, 'r') as f:
                return json.load(f)
        return None

    def get_latest(self):
        files = sorted(os.listdir(self.snapshot_dir), reverse=True)
        if files:
            with open(os.path.join(self.snapshot_dir, files[0]), 'r') as f:
                return json.load(f)
        return None

snapshot_generator = SnapshotGenerator()
