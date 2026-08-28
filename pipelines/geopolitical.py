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
            pulse_logger.log("⚠️ ANTHROPIC_API_KEY not set — Haiku classification will be unavailable", level="WARNING")
            self.anthropic_client = None
        else:
            self.anthropic_client = anthropic.Anthropic(api_key=api_key)
        self.pinned_store_file = "/data/pinned_stories.json"
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        self.sentiment_analyzer = hf_pipeline("sentiment-analysis", model=SENTIMENT_MODEL)
        self._ensure_geo_blocklist()
        self._seed_classifications()
        self._purge_blocked_from_cache()
        self.market_keywords = [
            'federal reserve', 'fomc', 'interest rate', 'interest rates',
            'rate hike', 'rate hikes', 'rate cut', 'rate cuts',
            'powell', 'inflation', 'cpi', 'ppi', 'gdp', 'jobs report', 'nonfarm',
            'tariff', 'tariffs', 'trade war', 'trade wars',
            'sanctions', 'sanction', 'sanctioned', 'sanctioning',
            'debt ceiling', 'government shutdown',
            'treasury', 'federal budget', 'deficit',
            'war', 'wars', 'military', 'nuclear',
            'attack', 'attacks', 'attacked', 'attacking',
            'missile', 'missiles', 'troops',
            'invasion', 'invade', 'invades', 'invaded', 'invading',
            'ceasefire', 'peace deal', 'peace deals',
            'escalation', 'escalate', 'escalates', 'escalated', 'escalating',
            'strait of hormuz',
            'iran', 'russia', 'china', 'nato', 'israel', 'ukraine',
            'recession', 'unemployment', 'oil price', 'oil prices', 'energy crisis',
            'supply chain', 'bank failure', 'bank failures',
            'default', 'defaulted', 'defaulting', 'currency crisis',
            'stock market', 'market crash', 'bear market', 'bull market',
            'nvidia', 'nvda', 'apple', 'aapl', 'microsoft', 'msft',
            'alphabet', 'googl', 'google', 'amazon', 'amzn', 'meta',
            'broadcom', 'avgo', 'amd', 'intel', 'intc',
            'taiwan semiconductor', 'tsm', 'coreweave',
            'deal', 'deals', 'partnership', 'partnerships',
            'billion', 'billions', 'chip', 'chips', 'capex',
            'ai infrastructure',
            'settlement', 'settlements', 'lawsuit', 'lawsuits',
            'verdict', 'verdicts', 'fined', 'fines',
            'judgment', 'judgments', 'judgement', 'judgements',
            'antitrust'
        ]
        self.ignore_keywords = [
            'investing club', 'war beneficiary stock', 'jim cramer', 'cramer',
            'we\'re adding', 'we like the message', 'top 10 things to watch',
            'portfolio buy', 'portfolio sell', 'charitable trust',
            'saturday night live', 'snl', 'comedy', 'movie', 'film', 'music',
            'album', 'concert', 'celebrity', 'oscars', 'grammy', 'emmy',
            'savannah guthrie', 'hoda kotb', 'today show', 'morning show',
            'taylor', 'kardashian', 'epstein', 'true crime',
            'sports', 'nba', 'nfl', 'nhl', 'mlb', 'soccer', 'football',
            'basketball', 'baseball', 'tennis', 'golf tournament',
            'prime day', 'spring sale', 'black friday', 'cyber monday',
            'walmart deals', 'amazon deals', 'shopping deals',
            'retail earnings', 'burritos', 'holiday shopping',
            'sheriff', 'local police', 'murder trial', 'missing person',
            'california court', 'county court', 'city council',
            'market timing', 'missing best days', 'long term investing',
            'retirement planning', 'personal finance tips', 'how to invest',
            'warren buffett says hold', 'buy and hold',
            'ai startup', 'venture capital', 'vc funding', 'app launch',
            'software update', 'new feature', 'product launch',
            'fashion', 'travel', 'food', 'recipe', 'weather',
            'bitcoin drops', 'crypto crash', 'nft', 'dogecoin', 'altcoin',
            'constitutional', 'historical background', 'legal analysis',
            'tax resistance', 'ice protests',
            'epstein', 'jeffery epstein', 'ghislaine',
            'honorary degree', 'awarded degree', 'awarded honorary',
            'wins award', 'receives award', 'lifetime achievement',
            'hall of fame', 'named ambassador', 'appointed ambassador',
            'named honorary', 'commencement', 'graduation',
            'campaign rally', 'reelection campaign', 'polling numbers',
            'approval rating', 'fundraiser', 'political ad',
            'charity', 'donation', 'philanthropy', 'volunteering',
            'community service', 'humanitarian award',
            'investing club subscribers', 'sunday column for investing',
            'cramer argues', 'jim cramer argues', 'according to cramer',
            'mad money', 'fast money', 'options action', 'halftime report',
            "here's why you should", "here's what to do", 'what investors should',
            'how to play', 'best stocks to buy', 'top stocks', 'stocks to watch', 'buy the dip',
            'quarterly earnings beat', 'quarterly earnings miss', 'revenue guidance',
            'eps beat', 'eps miss',
            'dream home', 'luxury real estate', 'mansion', 'yacht',
            'billionaire lifestyle', 'net worth revealed', 'richest people', 'wealthiest',
            'bitcoin price today', 'ethereum price', 'crypto rally', 'altcoin',
            'memecoin', 'dogecoin', 'shiba inu', 'nft mint',
            'warren buffett says', 'buffett says', 'berkshire hathaway', 'charlie munger',
            'sold too soon', 'flags tiny new buy', 'making calls on investments', 'still making calls',
            'barbie', 'dreamhouse', 'roller-skating', 'dream fest',
            'warehouse event', 'nightmare warehouse',
            'senator slams', 'sen. warren', 'warren slams',
            'slams trump', 'slams administration',
            'pressuring eu', 'tech regulations',
            'relaxing regulations', 'eu regulations',
            'congress slams', 'lawmaker slams',
            'representative slams', 'politician slams',
            'pressuring allies', 'diplomatic spat',
            'strongly condemns', 'harshly criticizes',
            'blasts white house', 'attacks policy',
            'award bonuses', 'bonuses to baristas', 'expand tipping',
            'turnaround efforts', 'employee experience',
            'customer experience', 'barista', 'tipping policy',
            'corporate turnaround', 'store closures',
            'layoffs at', 'hiring freeze', 'return to office',
            'work from home policy', 'corporate restructuring',
            'how to navigate', 'how to invest during',
            'what investors should do', 'navigating the confusion',
            'navigating uncertainty', 'how to protect',
            'investor playbook', 'what to do now',
            'mood of the stock market',
            'fuel surcharge', 'logistics surcharge', 'adds surcharge',
            'adding surcharge', 'energy surcharge', 'war surcharge',
            'raises prices due', 'higher prices due to',
        ]

    @staticmethod
    def is_article_too_old(timestamp_str, max_hours=MAX_ARTICLE_AGE_HOURS):
        """Return True if the article's timestamp is older than max_hours."""
        if not timestamp_str:
            return False
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            return age_hours > max_hours
        except Exception:
            return False

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

    def load_pinned_stories(self):
        """Load pinned stories, dropping any older than 48 hours or matching blocklist."""
        try:
            if not os.path.exists(self.pinned_store_file):
                return []
            with open(self.pinned_store_file, 'r') as f:
                pinned = json.load(f)
            manual_blocked = self._load_manual_blocklist_titles()
            blocklist = self._load_blocklist_strings()
            valid = []
            dirty = False
            for story in pinned:
                headline = story.get('headline', '')
                if manual_blocked and headline.lower() in manual_blocked:
                    pulse_logger.log(f"Manually blocked by user (pinned): {headline[:80]}")
                    dirty = True
                    continue
                if blocklist:
                    headline_lower = headline.lower()
                    matched = [b for b in blocklist if b in headline_lower]
                    if matched:
                        pulse_logger.log(f"Blocked by blocklist (pinned): {headline[:80]} | matched: {matched[0][:60]}")
                        dirty = True
                        continue
                if self._pin_is_expired(story):
                    pulse_logger.log(f"Age cutoff (pinned, article timestamp): {headline[:80]}")
                    dirty = True
                    continue
                valid.append(story)
            if dirty:
                self.save_pinned_stories(valid)
            return valid
        except Exception as e:
            pulse_logger.log(f"Failed to load pinned stories: {e}", level="WARNING")
            return []

    def save_pinned_stories(self, pinned):
        try:
            atomic_write_json(self.pinned_store_file, pinned)
        except Exception as e:
            pulse_logger.log(f"Failed to save pinned stories: {e}", level="WARNING")
