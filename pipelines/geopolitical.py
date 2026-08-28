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
        self.pinned_store_file = "/data/pinned_stories.json"

    def _pin_is_expired(self, story):
        ts = self._pin_ttl_timestamp(story)
        if not ts:
            return True
        return False
