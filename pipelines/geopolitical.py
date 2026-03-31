import json
import os
import requests
from datetime import datetime, timedelta
import pytz
from transformers import pipeline as hf_pipeline
from config import TIMEZONE, SENTIMENT_MODEL, THENEWS_API_KEY
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class GeopoliticalPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "geopolitical"
        self.persistent_file = "./data/persistent_flags.json"
        self.sentiment_analyzer = hf_pipeline("sentiment-analysis", model=SENTIMENT_MODEL)
        self._ensure_persistent_file()
        self.market_keywords = [
            'federal reserve', 'fomc', 'interest rate', 'rate hike', 'rate cut',
            'powell', 'inflation', 'cpi', 'ppi', 'gdp', 'jobs report', 'nonfarm',
            'tariff', 'trade war', 'sanctions', 'debt ceiling', 'government shutdown',
            'treasury', 'federal budget', 'deficit',
            'war', 'military', 'nuclear', 'attack', 'missile', 'troops', 'invasion',
            'ceasefire', 'peace deal', 'escalation', 'strait of hormuz',
            'iran', 'russia', 'china', 'nato', 'israel', 'ukraine',
            'recession', 'unemployment', 'oil price', 'energy crisis',
            'supply chain', 'bank failure', 'default', 'currency crisis',
            'stock market', 'market crash', 'bear market', 'bull market'
        ]
        self.ignore_keywords = [
            # CNBC investment commentary
            'investing club', 'war beneficiary stock', 'jim cramer', 'cramer',
            'we\'re adding', 'we like the message', 'top 10 things to watch',
            'portfolio buy', 'portfolio sell', 'charitable trust',
            # Entertainment & celebrity
            'saturday night live', 'snl', 'comedy', 'movie', 'film', 'music',
            'album', 'concert', 'celebrity', 'oscars', 'grammy', 'emmy',
            'savannah guthrie', 'hoda kotb', 'today show', 'morning show',
            'taylor', 'kardashian', 'epstein', 'true crime',
            # Sports
            'sports', 'nba', 'nfl', 'nhl', 'mlb', 'soccer', 'football',
            'basketball', 'baseball', 'tennis', 'golf tournament',
            # Retail & consumer
            'prime day', 'spring sale', 'black friday', 'cyber monday',
            'walmart deals', 'amazon deals', 'shopping deals',
            'retail earnings', 'burritos', 'holiday shopping',
            # Local & crime
            'sheriff', 'local police', 'murder trial', 'missing person',
            'california court', 'county court', 'city council',
            # Opinion & advice
            'market timing', 'missing best days', 'long term investing',
            'retirement planning', 'personal finance tips', 'how to invest',
            'warren buffett says hold', 'buy and hold',
            # Tech that isnt market moving
            'ai startup', 'venture capital', 'vc funding', 'app launch',
            'software update', 'new feature', 'product launch',
            # Misc noise
            'fashion', 'travel', 'food', 'recipe', 'weather',
            'bitcoin drops', 'crypto crash', 'nft', 'dogecoin', 'altcoin',
            'constitutional', 'historical background', 'legal analysis',
            'tax resistance', 'ice protests',
            'epstein', 'jeffery epstein', 'ghislaine',
            # Awards & non-market events
            'honorary degree', 'awarded degree', 'awarded honorary',
            'wins award', 'receives award', 'lifetime achievement',
            'hall of fame', 'named ambassador', 'appointed ambassador',
            'named honorary', 'commencement', 'graduation',
            # Political non-market
            'campaign rally', 'reelection campaign', 'polling numbers',
            'approval rating', 'fundraiser', 'political ad',
            # Human interest
            'charity', 'donation', 'philanthropy', 'volunteering',
            'community service', 'humanitarian award',
            # Opinion & Commentary
            'investing club subscribers', 'sunday column for investing',
            'cramer argues', 'jim cramer argues', 'according to cramer',
            'mad money', 'fast money', 'options action', 'halftime report',
            # Market Advice/Tips (not news)
            "here's why you should", "here's what to do", 'what investors should',
            'how to play', 'best stocks to buy', 'top stocks', 'stocks to watch', 'buy the dip',
            # Earnings that aren't macro-moving
            'quarterly earnings beat', 'quarterly earnings miss', 'revenue guidance',
            'eps beat', 'eps miss',
            # Lifestyle/Consumer disguised as business
            'dream home', 'luxury real estate', 'mansion', 'yacht',
            'billionaire lifestyle', 'net worth revealed', 'richest people', 'wealthiest',
            # Crypto noise
            'bitcoin price today', 'ethereum price', 'crypto rally', 'altcoin',
            'memecoin', 'dogecoin', 'shiba inu', 'nft mint',
            # Investor commentary
            'warren buffett says', 'buffett says', 'berkshire hathaway', 'charlie munger',
            'sold too soon', 'flags tiny new buy', 'making calls on investments', 'still making calls',
            'barbie', 'dreamhouse', 'roller-skating', 'dream fest',
            'warehouse event', 'nightmare warehouse',
        ]

    # ── Persistent flag memory ──────────────────────────────────────────────

    def _ensure_persistent_file(self):
        if not os.path.exists('./data'):
            os.makedirs('./data')
        if not os.path.exists(self.persistent_file):
            with open(self.persistent_file, 'w') as f:
                json.dump({}, f)

    def _load_persistent_flags(self):
        try:
            with open(self.persistent_file, 'r') as f:
                return json.load(f)
        except:
            return {}

    def _save_persistent_flags(self, flags_dict):
        try:
            with open(self.persistent_file, 'w') as f:
                json.dump(flags_dict, f, indent=2)
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to save persistent flags: {e}", level="WARNING")

    def _merge_into_persistent(self, live_flags):
        """Merge live flags into persistent storage. Refresh timestamp if seen again."""
        stored = self._load_persistent_flags()
        now = datetime.now(pytz.utc).isoformat()

        for flag in live_flags:
            key = flag['flag_type'] + '_' + flag['title'][:50]
            if key in stored:
                # Refresh last seen timestamp
                stored[key]['last_seen'] = now
                stored[key]['status'] = 'Developing'
                stored[key]['title'] = flag['title']
                stored[key]['context'] = flag['context']
                stored[key]['link'] = flag['link']
            else:
                # New flag — store it
                stored[key] = {
                    **flag,
                    'first_seen': now,
                    'last_seen': now,
                }

        self._save_persistent_flags(stored)
        return stored

    def _get_active_persistent_flags(self, stored):
        """Return flags with time decay applied. Drop anything older than 48 hours."""
        now = datetime.now(pytz.utc)
        active = []

        for key, flag in stored.items():
            try:
                last_seen = datetime.fromisoformat(flag['last_seen'])
                if last_seen.tzinfo is None:
                    last_seen = pytz.utc.localize(last_seen)
                age_hours = (now - last_seen).total_seconds() / 3600

                if age_hours > 24:
                    continue  # Expired — drop it
                elif age_hours > 6:
                    flag = {**flag, 'status': 'Monitoring', 'last_seen_label': f'Last seen {int(age_hours)}h ago'}
                else:
                    flag = {**flag, 'status': 'Developing', 'last_seen_label': None}

                active.append(flag)
            except:
                continue

        active.sort(key=lambda x: x['priority'], reverse=True)
        return active[:5]

    def _clean_expired_flags(self, stored):
        """Remove flags older than 24 hours from persistent storage."""
        now = datetime.now(pytz.utc)
        cleaned = {}
        for key, flag in stored.items():
            try:
                last_seen = datetime.fromisoformat(flag['last_seen'])
                if last_seen.tzinfo is None:
                    last_seen = pytz.utc.localize(last_seen)
                age_hours = (now - last_seen).total_seconds() / 3600
                if age_hours <= 24:
                    cleaned[key] = flag
            except:
                cleaned[key] = flag
        return cleaned

    # ── Existing methods (unchanged) ────────────────────────────────────────

    def is_market_relevant(self, text):
        if not text:
            return False
        text_lower = text.lower()
        
        # Layer 1 — blocklist: explicit noise, always reject
        for ignore in self.ignore_keywords:
            if ignore in text_lower:
                return False
        
        # Layer 2 — allowlist: must contain at least one market keyword to proceed
        has_market_keyword = any(keyword in text_lower for keyword in self.market_keywords)
        if not has_market_keyword:
            return False
        
        return True

    def get_sentiment_score(self, text):
        try:
            result = self.sentiment_analyzer(text[:512])[0]
            score = result['score'] if result['label'] == 'POSITIVE' else -result['score']
            return round(score, 3)
        except:
            return 0.0

    def _parse_articles(self, data, seen_titles, items):
        for article in data.get('data', []):
            title = article.get('title', '')
            if not title or title in seen_titles:
                continue
            if not self.is_market_relevant(title):
                continue
            seen_titles.add(title)
            description = article.get('description', '') or ''
            full_text = f"{title} {description}"
            sentiment = self.get_sentiment_score(full_text)
            published = article.get('published_at', '')
            try:
                dt = datetime.fromisoformat(published.replace('Z', '+00:00'))
                est = dt.astimezone(pytz.timezone(TIMEZONE))
                timestamp = est.strftime('%b %d, %I:%M %p EST')
                date = est.strftime('%Y-%m-%d')
            except:
                timestamp = published
                date = datetime.now(self.timezone).strftime('%Y-%m-%d')
            try:
                dt = datetime.fromisoformat(published.replace('Z', '+00:00'))
                age_days = (datetime.now(pytz.UTC) - dt.replace(tzinfo=pytz.UTC) if dt.tzinfo is None else datetime.now(pytz.UTC) - dt).days
                if age_days > 7:
                    continue
            except:
                pass
            items.append({
                'headline': title,
                'description': ' '.join(description[:300].split()),
                'source': article.get('source', 'TheNewsAPI'),
                'timestamp': timestamp,
                'date': date,
                'link': article.get('url', ''),
                'sentiment_score': sentiment,
                'market_relevant': True
            })

    def fetch_news(self):
        categories = ['business', 'politics', 'tech']
        search_queries = [
            'federal reserve OR FOMC OR interest rate OR inflation',
            'tariff OR trade war OR sanctions OR trump economy',
            'war OR military OR nuclear OR iran OR russia OR china',
            'government shutdown OR debt ceiling OR congress',
            'recession OR GDP OR unemployment OR jobs'
        ]
        items = []
        seen_titles = set()
        errors = 0

        for category in categories:
            try:
                url = (
                    f"https://api.thenewsapi.com/v1/news/top"
                    f"?api_token={THENEWS_API_KEY}"
                    f"&language=en"
                    f"&categories={category}"
                    f"&limit=25"
                    f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,abcnews.go.com,washingtonpost.com,nytimes.com"
                )
                response = requests.get(url, timeout=10)
                self._parse_articles(response.json(), seen_titles, items)
            except Exception as e:
                errors += 1
                error_handler.handle(e, f"TheNewsAPI category:{category}")

        for query in search_queries:
            try:
                url = (
                    f"https://api.thenewsapi.com/v1/news/all"
                    f"?api_token={THENEWS_API_KEY}"
                    f"&language=en"
                    f"&search={requests.utils.quote(query)}"
                    f"&sort=published_at"
                    f"&limit=25"
                    f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,washingtonpost.com,nytimes.com"
                )
                response = requests.get(url, timeout=10)
                self._parse_articles(response.json(), seen_titles, items)
            except Exception as e:
                errors += 1
                error_handler.handle(e, f"TheNewsAPI search:{query[:30]}")

        if errors > 0 and not items:
            error_handler.handle(Exception(f"All {errors} TheNewsAPI requests failed"), "TheNewsAPI Fetcher")
        return items

    def identify_flags(self, items):
        flags = []
        high_impact_keywords = {
            'nuclear': 95, 'war': 90, 'invasion': 92, 'missile': 88,
            'attack': 85, 'bomb': 85, 'default': 85, 'fomc': 85,
            'rate hike': 80, 'rate cut': 80, 'powell': 78,
            'federal reserve': 78, 'tariff': 80, 'debt ceiling': 80,
            'shutdown': 75, 'sanctions': 75, 'troops': 78,
            'recession': 75, 'ceasefire': 70, 'deal': 65,
            'agreement': 65, 'escalation': 72, 'inflation': 70,
            'gdp': 68, 'jobs': 67
        }
        low_quality_sources = [
            'truthout', 'rawstory', 'mediaite', 'salon',
            'huffpost', 'breitbart', 'dailykos', 'thegatewaypundit',
            'thestockmarketwatch.com', 'rt.com', 'sputnik',
            'investing.com', 'economictimes.indiatimes.com', 'asiaone.com',
            'indiatimes.com', 'timesofindia.com', 'hindustantimes.com',
            'thecanary.co', 'uctoday.com', 'tass.com', 'tass.ru',
            'sputniknews.com', 'presstv.ir'
        ]
        trusted_sources = [
            'reuters', 'associated press', 'cnbc', 'bloomberg',
            'wall street journal', 'financial times', 'politico', 'axios',
            'marketwatch', 'fox business', 'the hill'
        ]

        for item in items:
            text = item['headline'].lower()
            source = item.get('source', '').lower()
            if any(lqs in source for lqs in low_quality_sources):
                continue
            priority = 0
            flag_type = None
            for keyword, score in high_impact_keywords.items():
                if keyword in text:
                    if score > priority:
                        priority = score
                        flag_type = keyword
            if priority >= 65:
                if any(ts in source for ts in trusted_sources):
                    priority = min(priority + 5, 99)
                flags.append({
                    'title': item['headline'],
                    'priority': priority,
                    'flag_type': flag_type,
                    'status': 'Developing',
                    'source': item['source'],
                    'date': item['date'],
                    'timestamp': item['timestamp'],
                    'link': item['link'],
                    'sentiment': item['sentiment_score'],
                    'predicted_impact': 'bearish' if item['sentiment_score'] < -0.3 else 'bullish' if item['sentiment_score'] > 0.3 else 'neutral',
                    'context': item.get('description', '')[:200]
                })

        flags.sort(key=lambda x: x['priority'], reverse=True)
        return flags[:5]

    def calculate_score(self, items, flags):
        if not items:
            return 0.0
        scores = [item['sentiment_score'] for item in items]
        base_score = sum(scores) / len(scores) if scores else 0
        flag_adjustment = 0
        for flag in flags:
            if flag['predicted_impact'] == 'bearish':
                flag_adjustment -= 0.2 * (flag['priority'] / 100)
            elif flag['predicted_impact'] == 'bullish':
                flag_adjustment += 0.2 * (flag['priority'] / 100)
        return round(max(-2.0, min(2.0, base_score + flag_adjustment)), 2)

    def fetch(self):
        try:
            # Check cache first
            existing = cache.load(self.cache_key)
            age_minutes = cache.get_age_minutes(self.cache_key)
            if existing and age_minutes < 3:
                pulse_logger.log("↺ Geopolitical — using cache (TheNewsAPI refresh every 3min)")
                return existing['data']

            # Fetch live news
            items = self.fetch_news()

            # Load persistent flags regardless of whether live fetch succeeded
            stored = self._load_persistent_flags()

            if not items:
                # API returned nothing — use persistent flags as fallback
                pulse_logger.log(f"↺ Geopolitical — live fetch empty, serving persistent flags", level="WARNING")
                stored = self._clean_expired_flags(stored)
                self._save_persistent_flags(stored)
                active_flags = self._get_active_persistent_flags(stored)
                score = self.calculate_score([], active_flags)
                result = {
                    'pillar': 'geopolitical',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'news_items': [],
                    'active_flags': active_flags,
                    'total_items': 0,
                    'pillar_score': score,
                    'status': 'persistent'
                }
                return result

            # Live fetch succeeded — identify flags and merge into persistent memory
            live_flags = self.identify_flags(items)
            stored = self._merge_into_persistent(live_flags)
            stored = self._clean_expired_flags(stored)
            self._save_persistent_flags(stored)
            active_flags = self._get_active_persistent_flags(stored)
            score = self.calculate_score(items, active_flags)

            result = {
                'pillar': 'geopolitical',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'news_items': items[:10],
                'financial_juice_items': items[:10],
                'unbiased_network_items': [],
                'active_flags': active_flags,
                'total_items': len(items),
                'pillar_score': score,
                'status': 'live'
            }
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ Geopolitical updated | {len(active_flags)} active flags | {len(items)} articles | Score: {score}")
            return result

        except Exception as e:
            error_handler.handle(e, "Geopolitical")
            # Last resort — serve persistent flags
            try:
                stored = self._load_persistent_flags()
                active_flags = self._get_active_persistent_flags(stored)
                if active_flags:
                    pulse_logger.log(f"↺ Geopolitical — exception fallback, serving {len(active_flags)} persistent flags")
                    return {
                        'pillar': 'geopolitical',
                        'timestamp': datetime.now(self.timezone).isoformat(),
                        'news_items': [],
                        'active_flags': active_flags,
                        'total_items': 0,
                        'pillar_score': self.calculate_score([], active_flags),
                        'status': 'persistent'
                    }
            except:
                pass
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return cached['data']
            return None

geopolitical_pipeline = GeopoliticalPipeline()
