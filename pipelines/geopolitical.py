import json
import os
import requests
import concurrent.futures
from datetime import datetime, timedelta
import pytz
from google import genai
from google.genai import types
from transformers import pipeline as hf_pipeline
from config import TIMEZONE, SENTIMENT_MODEL, THENEWS_API_KEY
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class GeopoliticalPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "geopolitical"
        self.persistent_file = "/data/persistent_flags.json"
        self.gemini_client = genai.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))
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
            # Political commentary without market impact
            'senator slams', 'sen. warren', 'warren slams',
            'slams trump', 'slams administration',
            'pressuring eu', 'tech regulations',
            'relaxing regulations', 'eu regulations',
            'congress slams', 'lawmaker slams',
            'representative slams', 'politician slams',
            # General political noise
            'pressuring allies', 'diplomatic spat',
            'strongly condemns', 'harshly criticizes',
            'blasts white house', 'attacks policy',
            # Single company labor/HR news
            'award bonuses', 'bonuses to baristas', 'expand tipping',
            'turnaround efforts', 'employee experience',
            'customer experience', 'barista', 'tipping policy',
            'corporate turnaround', 'store closures',
            'layoffs at', 'hiring freeze', 'return to office',
            'work from home policy', 'corporate restructuring',
            # Market navigation/advice disguised as news
            'how to navigate', 'how to invest during',
            'what investors should do', 'navigating the confusion',
            'navigating uncertainty', 'how to protect',
            'investor playbook', 'what to do now',
            'mood of the stock market',
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

    # ── Gemini AI Relevance Classifier ─────────────────────────────────────

    def classify_relevance_batch(self, articles):
        """Use Gemini Flash to classify which articles are genuinely market-moving for NQ/ES futures."""
        if not articles:
            return []

        # Build batch input
        article_list = ""
        for i, article in enumerate(articles):
            article_list += f"{i+1}. TITLE: {article['headline']}\n   DESC: {article.get('description', '')[:150]}\n\n"

        prompt = f"""You are assisting a professional NQ and ES futures day trader with pre-market preparation.

Your job is to read each headline and description, then make two decisions:

DECISION 1 — RELEVANCE
Is this genuinely new, market-moving information that would cause a futures trader to reconsider their directional bias for today's session? 

Think like a trader sitting down at 8AM asking: "Does this change anything about how I trade today?"

Pass if it involves: Federal Reserve policy or official commentary, geopolitical escalation or resolution affecting global risk sentiment, major economic data surprises, energy market shocks, trade policy changes with immediate impact, systemic financial risk, or significant government actions with direct market consequences.

Fail if it involves: opinion or commentary on past market moves, investment advice or tips, personal finance stories, single company news unless systemically important, celebrity investor quotes, lifestyle or consumer behavior stories, newsletter recap formats, or anything that describes what already happened rather than new information.

Before passing any article, run it through these six filters. If it fails any one of them, reject it:

FILTER 1 — SOURCE VS ECHO
Is this the event itself or a reaction to an event that already happened? Source events are new — the market hasn't priced them in yet. Echo events are corporate, institutional, or personal reactions to known macro situations. Ask: "Is this the cause or the effect?" If it's the effect — fail it. 
Examples: "Iran attacks oil tanker" = source, pass. "Amazon adds surcharge due to Iran war" = echo of a known event, fail. "Fed raises rates" = source, pass. "Airlines raise prices due to Fed hike" = echo, fail. Corporate adaptation to a known macro event is always an echo — never pass it regardless of how macro the language sounds.

FILTER 2 — RECENCY TEST
Is this reporting something happening RIGHT NOW or recapping something that already happened? Recaps, week-in-review pieces, "after X weeks of..." articles, and historical context pieces are not new information. The market already knows. Fail anything that describes past events rather than breaking developments.
Examples: "All Eyes on Wall St. After 5 Weeks of Losses" = recap, fail. "Iran war enters fifth week" = recap, fail. "Trump announces new tariffs today" = breaking, pass.

FILTER 3 — SPECIFICITY TEST
Is this about a specific actionable event or a general mood/sentiment piece? Vibe articles, market psychology pieces, and "how to navigate" content are not tradeable information. A trader needs facts, not feelings.
Examples: "The mood of the stock market is changing" = vibe, fail. "How to navigate the confusion" = advice, fail. "Fed Chair signals rate pause" = specific event, pass.

FILTER 4 — ACTOR TEST
Is the person or organization in this headline someone who directly moves markets through their decisions? Federal Reserve officials, heads of state, treasury secretaries, central bank chiefs, and major geopolitical actors = yes. State governors, backbench senators, corporate executives reacting to macro events, NASA, local officials = no, unless their specific action is systemically important to financial markets.
Examples: "Fed's Powell signals dovish shift" = market-moving actor, pass. "Maryland Governor warns of forever war" = state-level actor, fail. "Treasury Secretary Bessent announces new policy" = market-moving actor, pass.

FILTER 5 — MARKET DOMAIN TEST
Does this article exist within the domain of financial markets, geopolitics affecting markets, energy, trade, or monetary policy? Articles about space missions, scientific discoveries, social policy, healthcare unless market-moving, and non-financial government activity should be rejected even if they use financial language.
Examples: "Artemis II gets OK to fly to the moon" = wrong domain, fail. "NASA budget cut affects defense contractors" = market domain, pass. "Trump signs healthcare bill" = depends on market impact, evaluate carefully.

FILTER 6 — CONFIRMATION TRAP TEST
Is this article just confirming something the market already knows and has already priced in? If the macro situation is already established and this is just another data point piling on — it adds no new directional information. Fail it.
Examples: Iran war is already flagged as bearish → "Another company raises prices due to Iran war" = confirmation of known narrative, fail. Fed is already known hawkish → "Analyst says Fed will stay hawkish" = confirmation, fail. New ceasefire talks announced → "Markets react to ceasefire hopes" = new development, pass.

DECISION 2 — MARKET DIRECTION
If relevant, what is the directional impact on NQ and ES equity futures specifically?

Think like this: You are a trader. You just read this headline. Do you lean long or short on NQ right now?

Consider the full chain of consequences:
- War escalating → oil up → inflation up → Fed stays hawkish → equities down → BEARISH
- Ceasefire → oil down → inflation eases → Fed pivots → equities up → BULLISH  
- Company adding surcharges due to war → costs rise → margins compress → BEARISH
- Gas prices hitting new highs → consumer spending squeezed → BEARISH
- Strong jobs data → Fed stays hawkish → rates stay high → BEARISH for growth stocks
- Weak jobs data → Fed cuts sooner → BULLISH for equities
- Trump hawkish on trade → tariffs → supply chain costs → BEARISH
- Trump ceasefire deal → geopolitical risk off → BULLISH

Do not look at whether the headline sounds positive or negative in tone. Look at the downstream consequence for equity futures. A company "soaring" in surcharges is bearish. Markets "recovering on hopes" is bullish. Always think: what does this mean for the trader holding NQ right now?

Return ONLY a JSON array with no markdown, no explanation, no preamble. Exactly this format:
[{{"id": 1, "relevant": true, "confidence": 0.95, "category": "geopolitical", "direction": "bearish", "reason": "Amazon surcharge signals cost-push inflation and margin compression — bearish for equities"}}, ...]

Use only "bearish", "bullish", or "neutral" for direction.
Use confidence between 0.0 and 1.0.

Articles to classify:
{article_list}"""

        try:
            response = self.gemini_client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt
            )
            text = response.text.strip()
            # Strip markdown if present
            if '```' in text:
                text = text.split('```')[1]
                if text.startswith('json'):
                    text = text[4:]
            results = json.loads(text)
            return results
        except Exception as e:
            pulse_logger.log(f"⚠️ Gemini classifier failed: {e}", level="WARNING")
            return []

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
        import threading
        categories = ['business', 'politics', 'tech']
        search_queries = [
            'federal reserve OR FOMC OR interest rate OR inflation',
            'tariff OR trade war OR sanctions OR trump economy',
            'war OR military OR nuclear OR iran OR russia OR china',
            'government shutdown OR debt ceiling OR congress',
            'recession OR GDP OR unemployment OR jobs'
        ]

        def fetch_category(category):
            url = (
                f"https://api.thenewsapi.com/v1/news/top"
                f"?api_token={THENEWS_API_KEY}"
                f"&language=en"
                f"&categories={category}"
                f"&limit=25"
                f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,abcnews.go.com,washingtonpost.com,nytimes.com"
            )
            response = requests.get(url, timeout=10)
            return response.json()

        def fetch_query(query):
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
            return response.json()

        items = []
        seen_titles = set()
        seen_lock = threading.Lock()

        def safe_parse(data):
            local_items = []
            for article in data.get('data', []):
                title = article.get('title', '')
                if not title:
                    continue
                description = article.get('description', '') or ''
                full_check_text = f"{title} {description}"
                if not self.is_market_relevant(full_check_text):
                    continue
                published = article.get('published_at', '')
                try:
                    dt = datetime.fromisoformat(published.replace('Z', '+00:00'))
                    est = dt.astimezone(pytz.timezone(TIMEZONE))
                    timestamp = est.strftime('%b %d, %I:%M %p EST')
                    date = est.strftime('%Y-%m-%d')
                    age_days = (datetime.now(pytz.UTC) - dt).days
                    if age_days > 7:
                        continue
                except:
                    timestamp = published
                    date = datetime.now(self.timezone).strftime('%Y-%m-%d')
                local_items.append({
                    'headline': title,
                    'description': ' '.join(description[:300].split()),
                    'source': article.get('source', 'TheNewsAPI'),
                    'timestamp': timestamp,
                    'date': date,
                    'link': article.get('url', ''),
                    'sentiment_score': self.get_sentiment_score(f"{title} {description}"),
                    'market_relevant': True
                })
            return local_items

        # Run all API calls in parallel — max 12s total
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            cat_futures = [executor.submit(fetch_category, cat) for cat in categories]
            qry_futures = [executor.submit(fetch_query, q) for q in search_queries]
            all_futures = cat_futures + qry_futures

            for future in concurrent.futures.as_completed(all_futures, timeout=12):
                try:
                    data = future.result()
                    batch = safe_parse(data)
                    with seen_lock:
                        for item in batch:
                            if item['headline'] not in seen_titles:
                                seen_titles.add(item['headline'])
                                items.append(item)
                except Exception as e:
                    pulse_logger.log(f"⚠️ Parallel fetch failed: {e}", level="WARNING")

        if not items:
            pulse_logger.log("⚠️ All parallel fetches returned empty", level="WARNING")
            return []

        # Load Gemini classification cache
        gemini_cache_file = "/data/gemini_classifications.json"
        try:
            if os.path.exists(gemini_cache_file):
                with open(gemini_cache_file, 'r') as f:
                    gemini_cache = json.load(f)
            else:
                gemini_cache = {}
        except:
            gemini_cache = {}

        # Split into already classified vs new
        new_items = [i for i in items if i['headline'] not in gemini_cache]
        known_relevant = []
        for i in items:
            cached = gemini_cache.get(i['headline'], {})
            if cached.get('relevant') and cached.get('confidence', 0) >= 0.75:
                # Apply Gemini's direction if available, otherwise keep DistilBERT score
                if cached.get('direction'):
                    direction = cached['direction']
                    i['sentiment_score'] = 0.8 if direction == 'bullish' else -0.8 if direction == 'bearish' else 0.0
                    i['gemini_direction'] = direction
                known_relevant.append(i)

        # Articles not yet classified — use keyword filter as temporary pass
        keyword_passed = [
            i for i in new_items
            if self.is_market_relevant(i['headline'])
        ]

        # Return immediately — known relevant + keyword-passed new articles
        immediately_available = known_relevant + keyword_passed
        pulse_logger.log(f"⚡ Returning {len(immediately_available)} articles instantly ({len(known_relevant)} Gemini-verified, {len(keyword_passed)} keyword-passed)")

        # Run Gemini on new items in background
        if new_items:
            def background_classify():
                try:
                    pulse_logger.log(f"🤖 Gemini background classifying {len(new_items)} new articles...")
                    classifications = self.classify_relevance_batch(new_items)
                    if classifications:
                        for r in classifications:
                            idx = r['id'] - 1
                            if idx < len(new_items):
                                gemini_cache[new_items[idx]['headline']] = {
                                    'relevant': r.get('relevant', False),
                                    'confidence': r.get('confidence', 0),
                                    'category': r.get('category', ''),
                                    'direction': r.get('direction', None),
                                    'reason': r.get('reason', ''),
                                    'classified_at': datetime.now().isoformat()
                                }
                        with open(gemini_cache_file, 'w') as f:
                            json.dump(gemini_cache, f, indent=2)
                        pulse_logger.log(f"✅ Gemini background done — {len(classifications)} articles classified and cached")
                except Exception as e:
                    pulse_logger.log(f"⚠️ Background Gemini failed: {e}", level="WARNING")

            bg_thread = threading.Thread(target=background_classify, daemon=True)
            bg_thread.start()

        return immediately_available

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
