import json
import os
import uuid
from utils.file_lock import atomic_write_json
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.logger import pulse_logger
from utils.cache import cache

class SnapshotGenerator:
    def __init__(self, snapshot_dir="/data/snapshots"):
        self.snapshot_dir = snapshot_dir
        self.timezone = pytz.timezone(TIMEZONE)
        self._ensure_exists()

    def _ensure_exists(self):
        if not os.path.exists(self.snapshot_dir):
            os.makedirs(self.snapshot_dir)

    def generate_id(self):
        return str(uuid.uuid4())

    def save(self, bias_score, formatted_data):
        timestamp = datetime.now(self.timezone).isoformat()
        snapshot_id = self.generate_id()
        weekly = None
        try:
            with open('/data/permanent_weekly_summary.json', 'r') as f:
                weekly = json.load(f)
        except:
            pass
        snapshot = {
            'id': snapshot_id,
            'timestamp': timestamp,
            'bias': bias_score,
            'pillars': formatted_data,
            'weekly_summary': weekly if weekly else None
        }
        snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{snapshot_id}.json")
        atomic_write_json(snapshot_file, snapshot)
        pulse_logger.log(f"📸 Snapshot saved | ID: {snapshot_id}")

        # Keep only last 50 snapshots
        all_snapshots = sorted(
            os.listdir(self.snapshot_dir),
            key=lambda f: os.path.getmtime(os.path.join(self.snapshot_dir, f)),
            reverse=True
        )
        if len(all_snapshots) > 50:
            for old_file in all_snapshots[50:]:
                os.remove(os.path.join(self.snapshot_dir, old_file))

        return snapshot_id

    def load(self, snapshot_id):
        snapshot_file = os.path.join(self.snapshot_dir, f"snapshot_{snapshot_id}.json")
        if os.path.exists(snapshot_file):
            with open(snapshot_file, 'r') as f:
                return json.load(f)
        return None

    def get_latest(self):
        files = sorted(
            os.listdir(self.snapshot_dir),
            key=lambda f: os.path.getmtime(os.path.join(self.snapshot_dir, f)),
            reverse=True
        )
        if files:
            with open(os.path.join(self.snapshot_dir, files[0]), 'r') as f:
                return json.load(f)
        return None

snapshot_generator = SnapshotGenerator()
