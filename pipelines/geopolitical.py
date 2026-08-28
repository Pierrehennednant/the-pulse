import json
import os
import re
import threading
import time
import requests
import concurrent.futures
from datetime import datetime, timedelta, timezone
from dateutil import parser as dateutil_parser
import pytz
import anthropic
from transformers import pipeline as hf_pipeline
from config import TIMEZONE, SENTIMENT_MODEL, THENEWS_API_KEY
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

MAX_ARTICLE_AGE_HOURS = 48


class GeopoliticalPipeline:
    GEO_BLOCKLIST_FILE = "/data/geo_blocklist.json"
    GEO_MANUAL_BLOCKLIST_FILE = "/data/geo_manual_blocklist.json"

    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "geopolitical"
        api_key = os.environ.get('ANTHROPIC_API_KEY', '')
        if not api_key:
            pulse_logger.log("ANTHROPIC_API_KEY not set", level="WARNING")
            self.anthropic_client = None
        else:
            self.anthropic_client = anthropic.Anthropic(api_key=api_key)
        self.pinned_store_file = "/data/pinned_stories.json"

    @staticmethod
    def _pin_ttl_timestamp(story):
        """Article time for pin TTL. Never uses pinned_at."""
        for field in ('published_at', 'timestamp', 'date'):
            val = (story.get(field) or '').strip()
            if val:
                return val
        return ''

    def _pin_is_expired(self, story):
        """True if the pin is older than 48h from the article timestamp, or has none.

        Missing/unparseable timestamps fail closed (treat as expired) so a pin
        cannot sit forever the way an empty published_at used to.
        """
        ts = self._pin_ttl_timestamp(story)
        if not ts:
            return True
        try:
            dt = dateutil_parser.parse(
                ts,
                default=datetime.now(timezone.utc),
                tzinfos={"EST": -18000, "EDT": -14400},
            )
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            return age_hours > MAX_ARTICLE_AGE_HOURS
        except Exception:
            return True
