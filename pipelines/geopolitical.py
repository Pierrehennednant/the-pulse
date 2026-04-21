import json
import os
import requests
import concurrent.futures
from datetime import datetime, timedelta
import pytz
import anthropic
from transformers import pipeline as hf_pipeline
from config import TIMEZONE, SENTIMENT_MODEL, THENEWS_API_KEY
from utils.file_lock import atomic_write_json
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class GeopoliticalPipeline:
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
            # Corporate surcharge and price reaction (echo events)
            'fuel surcharge', 'logistics surcharge', 'adds surcharge',
            'adding surcharge', 'energy surcharge', 'war surcharge',
            'raises prices due', 'higher prices due to',
        ]

    # ── Gemini AI Relevance Classifier ─────────────────────────────────────

    def classify_relevance_batch(self, articles):
        """Use Claude Haiku to classify articles with full article context and generate clean summaries."""
        if not articles:
            return []
        if self.anthropic_client is None:
            return []

        # Build batch input with full article text
        article_list = ""
        for i, article in enumerate(articles):
            full_text = self.fetch_full_article(
                article.get('link', ''),
                article.get('description', '')
            )
            article_list += f"{i+1}. TITLE: {article['headline']}\n   FULL TEXT: {full_text}\n\n"

        prompt = f"""You are assisting a professional NQ and ES futures day trader with pre-market preparation.

Your job is to read each full article, then make three decisions:

DECISION 1 — RELEVANCE
Is this genuinely new, market-moving information that would cause a futures trader to reconsider their directional bias for today's session?

Think like a trader sitting down at 8AM asking: "Does this change anything about how I trade today?"

Pass if it involves: Federal Reserve policy or official commentary, geopolitical escalation or resolution affecting global risk sentiment, major economic data surprises, energy market shocks, trade policy changes with immediate impact, systemic financial risk, or significant government actions with direct market consequences.

Fail if it involves: opinion or commentary on past market moves, investment advice or tips, personal finance stories, single company news unless systemically important, celebrity investor quotes, lifestyle or consumer behavior stories, newsletter recap formats, or anything that describes what already happened rather than new information.

Before passing any article, run it through these six filters. If it fails any one of them, reject it:

FILTER 1 — SOURCE VS ECHO
Is this the event itself or a reaction to an event that already happened? A SOURCE event is new information the market hasn't priced in yet. It originates from a primary actor — a government, central bank, military, or natural force. An ECHO event is any person, company, or institution RESPONDING to or REPORTING ON a known macro situation.

Critical rule: If a company name appears as the subject of the headline and the headline describes them REACTING to a macro event (adding fees, raising prices, cutting jobs, warning of impacts, adjusting operations) — it is ALWAYS an echo. Reject it.

The presence of macro keywords like "war", "energy", "Iran", "tariff" in a headline does NOT make it a source event. Ask: who is the ACTOR and what ACTION did they take? If the actor is a corporation reacting to an existing situation — it's an echo regardless of the macro language surrounding it.

FILTER 2 — RECENCY TEST
Is this reporting something happening RIGHT NOW or recapping something that already happened? Recaps, week-in-review pieces, "after X weeks of..." articles, and historical context pieces are not new information.

FILTER 3 — SPECIFICITY TEST
Is this about a specific actionable event or a general mood/sentiment piece? Vibe articles, market psychology pieces, and "how to navigate" content are not tradeable information.

FILTER 4 — ACTOR TEST
Is the person or organization in this headline someone who directly moves markets through their decisions? Federal Reserve officials, heads of state, treasury secretaries, central bank chiefs, and major geopolitical actors = yes. State governors, backbench senators, corporate executives reacting to macro events, NASA, local officials = no, unless their specific action is systemically important to financial markets.

FILTER 5 — MARKET DOMAIN TEST
Does this article exist within the domain of financial markets, geopolitics affecting markets, energy, trade, or monetary policy? Articles about space missions, scientific discoveries, social policy, and non-financial government activity should be rejected even if they use financial language.

FILTER 6 — CONFIRMATION TRAP TEST
Is this article just confirming something the market already knows and has already priced in? If the macro situation is already established and this is just another data point piling on — it adds no new directional information. Fail it.

DECISION 2 — MARKET DIRECTION
If relevant, what is the directional impact on NQ and ES equity futures specifically?

Read the FULL article text carefully before deciding direction. Do not base direction on the headline alone.

Consider the full chain of consequences:
- War escalating → oil up → inflation up → Fed stays hawkish → equities down → BEARISH
- Ceasefire → oil down → inflation eases → Fed pivots → equities up → BULLISH
- Company adding surcharges due to war → costs rise → margins compress → BEARISH
- Gas prices hitting new highs → consumer spending squeezed → BEARISH
- Strong jobs data → Fed stays hawkish → rates stay high → BEARISH for growth stocks
- Weak jobs data → Fed cuts sooner → BULLISH for equities
- Trump hawkish on trade → tariffs → supply chain costs → BEARISH
- Trump ceasefire deal → geopolitical risk off → BULLISH
- Government shutdown ongoing → fiscal uncertainty → economic drag → BEARISH
- Government shutdown resolved → fiscal clarity → BULLISH
- Fed hawkish nominee → higher rates longer → BEARISH for NQ
- Fed independence threatened → institutional uncertainty → BEARISH
- Paying workers via executive order while shutdown continues → band-aid not resolution → BEARISH

DECISION 3 — SUMMARY
Write a clean 3-4 sentence market-focused summary of the article. Cover: what happened, who the key actor is, what the immediate consequence is, and what it means for NQ/ES traders today. Write it as if briefing a trader in 30 seconds. Do not use jargon. Be direct and specific.

Return ONLY a JSON array with no markdown, no explanation, no preamble. Exactly this format:
[{{"id": 1, "relevant": true, "confidence": 0.95, "category": "geopolitical", "direction": "bearish", "reason": "Iran war escalation directly affects oil and risk sentiment", "summary": "Your 3-4 sentence market summary here.", "uncertainty_score": 85}}]

Use only "bearish", "bullish", or "neutral" for direction.
Use confidence between 0.0 and 1.0.
If relevant is false, still provide a summary field but it can be empty string.

DECISION 4 — UNCERTAINTY SCORE
Rate how much uncertainty and execution difficulty this event creates for a day trader on a scale of 0-100.
This is NOT about how bearish or bullish the event is. This is ONLY about whether the event creates fragmented, unpredictable price action that makes clean entries difficult.

Score high (70-100) when:
- Event is unresolved and market doesn't know which way to price it
- Multiple conflicting actors or outcomes are possible
- Event is rapidly evolving with new developments expected today
- Market is in reaction mode — random spikes, no clean structure

Score medium (40-69) when:
- Event is significant but direction is becoming clearer
- Credible threat from major actor but not yet confirmed action
- Market has partially priced it in but uncertainty remains

Score low (0-39) when:
- Event confirms existing market direction — bearish or bullish, doesn't matter
- Resolution or ceasefire — uncertainty is reducing
- Market has clearly priced this in already
- Rumor with no confirmation and no market reaction yet

Key rule: A confirmed bearish event with clear direction scores LOW uncertainty even if it's very negative for markets. Uncertainty means the market doesn't know what to do — not that it's going down.

Articles to classify:
{article_list}"""

        try:
            response = self.anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text.strip()
            if '```' in text:
                text = text.split('```')[1]
                if text.startswith('json'):
                    text = text[4:]
            results = json.loads(text)
            pulse_logger.log(f"✅ Claude Haiku classified {len(results)} articles")
            return results
        except Exception as e:
            pulse_logger.log(f"⚠️ Claude Haiku classifier failed: {e}", level="WARNING")
            return []

    # ── Pinned Stories Store ─────────────────────────────────────────────────

    def load_pinned_stories(self):
        """Load pinned stories, dropping any older than 48 hours."""
        try:
            if not os.path.exists(self.pinned_store_file):
                return []
            with open(self.pinned_store_file, 'r') as f:
                pinned = json.load(f)
            from datetime import timezone
            now = datetime.now(timezone.utc)
            valid = []
            for story in pinned:
                try:
                    pinned_at = datetime.fromisoformat(story.get('pinned_at', ''))
                    if pinned_at.tzinfo is None:
                        pinned_at = pinned_at.replace(tzinfo=timezone.utc)
                    age_hours = (now - pinned_at).total_seconds() / 3600
                    if age_hours <= 48:
                        valid.append(story)
                except Exception as e:
                    pulse_logger.log(f"⚠️ Failed to parse pinned story timestamp: {e}", level="WARNING")
                    continue
            return valid
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load pinned stories: {e}", level="WARNING")
            return []

    def save_pinned_stories(self, pinned):
        """Save pinned stories to disk."""
        try:
            atomic_write_json(self.pinned_store_file, pinned)
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to save pinned stories: {e}", level="WARNING")

    def is_same_story(self, new_headline, pinned_headline):
        """Ask Haiku whether a new article supersedes a pinned story."""
        if self.anthropic_client is None:
            return False
        try:
            prompt = f"""You are evaluating whether two news headlines are about the same underlying geopolitical or market story.

NEW ARTICLE: {new_headline}
PINNED STORY: {pinned_headline}

Are these two headlines covering the same underlying story or event — even if the outcome has changed or the angle is different?

Examples of SAME story:
- "Hormuz blockade tightens" and "Iran opens Hormuz to commercial vessels" — same story, outcome changed
- "U.S.-Iran talks stall" and "Iran agrees to ceasefire terms" — same story, new development
- "Fed signals rate hike" and "Fed raises rates by 25bps" — same story, event occurred

Examples of DIFFERENT story:
- "Iran blockade" and "China tariffs escalate" — different geopolitical events
- "Fed rate decision" and "CPI data surprise" — different market events

Respond with only one word: SAME or DIFFERENT"""

            response = self.anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}]
            )
            result = response.content[0].text.strip().upper()
            return result == "SAME"
        except Exception as e:
            pulse_logger.log(f"⚠️ Haiku story comparison failed: {e}", level="WARNING")
            return False

    def update_pinned_store(self, new_items, classifications):
        """Update pinned store with newly Haiku-verified high-confidence articles."""
        pinned = self.load_pinned_stories()

        for r in classifications:
            idx = r['id'] - 1
            if idx >= len(new_items):
                continue
            if not r.get('relevant') or r.get('confidence', 0) < 0.75:
                continue
            if r.get('direction', 'neutral') == 'neutral':
                continue

            article = new_items[idx]
            headline = article.get('headline', '')

            new_entry = {
                'headline': headline,
                'summary': r.get('summary', article.get('description', '')),
                'direction': r.get('direction'),
                'confidence': r.get('confidence', 0),
                'uncertainty_score': r.get('uncertainty_score', 0),
                'source': article.get('source', ''),
                'timestamp': article.get('timestamp', ''),
                'date': article.get('date', ''),
                'link': article.get('link', ''),
                'pinned_at': datetime.now().isoformat()
            }

            # Ask Haiku whether this supersedes any existing pinned story
            replaced = False
            for i, pin in enumerate(pinned):
                if self.is_same_story(headline, pin.get('headline', '')):
                    pinned[i] = new_entry
                    replaced = True
                    pulse_logger.log(f"📌 Pinned story superseded: {headline[:60]}")
                    break

            if not replaced and len(pinned) < 5:
                pinned.append(new_entry)
                pulse_logger.log(f"📌 New story pinned: {headline[:60]}")

        self.save_pinned_stories(pinned)

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
        except Exception as e:
            pulse_logger.log(f"⚠️ Sentiment analyzer failed: {e}", level="WARNING")
            return 0.0

    def fetch_full_article(self, url, fallback_description):
        """Fetch full article text for Gemini context. Falls back to description if paywalled."""
        try:
            response = fetch_with_retry(url, headers=self.headers, timeout=8)
            if response.status_code != 200:
                return fallback_description
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            # Remove script, style, nav elements
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                tag.decompose()
            # Get paragraph text
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text(strip=True) for p in paragraphs[:15]])
            if len(text) < 200:
                return fallback_description
            return text[:3000]
        except Exception:
            return fallback_description

    def _parse_articles(self, data, seen_titles, items):
        for article in data.get('data', []):
            title = article.get('title', '')
            if not title or title in seen_titles:
                continue

            # Same-source deduplication — skip if same source posted similar headline within 2 hours
            is_source_duplicate = False
            source = article.get('source', '')
            published = article.get('published_at', '')
            for existing in items:
                if existing['source'] == source:
                    try:
                        existing_dt = datetime.fromisoformat(existing.get('published_at', '').replace('Z', '+00:00'))
                        current_dt = datetime.fromisoformat(published.replace('Z', '+00:00'))
                        time_diff_hours = abs((current_dt - existing_dt).total_seconds()) / 3600
                        if time_diff_hours < 2:
                            existing_words = set(existing['headline'].lower().split())
                            current_words = set(title.lower().split())
                            overlap = len(existing_words & current_words) / max(len(existing_words | current_words), 1)
                            if overlap > 0.3:
                                is_source_duplicate = True
                                break
                    except Exception as e:
                        pulse_logger.log(f"⚠️ Failed to parse article timestamp for dedup: {e}", level="WARNING")
            if is_source_duplicate:
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
            except Exception as e:
                pulse_logger.log(f"⚠️ Failed to parse article publish date: {e}", level="WARNING")
                timestamp = published
                date = datetime.now(self.timezone).strftime('%Y-%m-%d')
            try:
                dt = datetime.fromisoformat(published.replace('Z', '+00:00'))
                age_days = (datetime.now(pytz.UTC) - dt.replace(tzinfo=pytz.UTC) if dt.tzinfo is None else datetime.now(pytz.UTC) - dt).days
                if age_days > 7:
                    continue
            except Exception as e:
                pulse_logger.log(f"⚠️ Failed to compute article age, including anyway: {e}", level="WARNING")
            items.append({
                'headline': title,
                'description': ' '.join(description[:800].split()),
                'source': article.get('source', 'TheNewsAPI'),
                'timestamp': timestamp,
                'date': date,
                'link': article.get('url', ''),
                'sentiment_score': sentiment,
                'market_relevant': True,
                'published_at': article.get('published_at', '')
            })

    def fetch_news(self):
        import threading
        categories = ['business', 'politics', 'tech']
        search_queries = [
            'federal reserve OR tariff OR war OR iran OR sanctions OR recession OR trump'
        ]

        def fetch_category(category):
            url = (
                f"https://api.thenewsapi.com/v1/news/top"
                f"?api_token={THENEWS_API_KEY}"
                f"&language=en"
                f"&categories={category}"
                f"&limit=25"
                f"&published_after={(datetime.now(pytz.utc) - __import__('datetime').timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S')}"
                f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,abcnews.go.com,washingtonpost.com,nytimes.com"
            )
            response = fetch_with_retry(url, timeout=10)
            return response.json()

        def fetch_query(query):
            url = (
                f"https://api.thenewsapi.com/v1/news/all"
                f"?api_token={THENEWS_API_KEY}"
                f"&language=en"
                f"&search={requests.utils.quote(query)}"
                f"&sort=published_at"
                f"&limit=25"
                f"&published_after={(datetime.now(pytz.utc) - __import__('datetime').timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S')}"
                f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,washingtonpost.com,nytimes.com"
            )
            response = fetch_with_retry(url, timeout=10)
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
                except Exception as e:
                    pulse_logger.log(f"⚠️ Failed to parse article date in parallel fetch: {e}", level="WARNING")
                    timestamp = published
                    date = datetime.now(self.timezone).strftime('%Y-%m-%d')
                local_items.append({
                    'headline': title,
                    'description': ' '.join(description[:800].split()),
                    'source': article.get('source', 'TheNewsAPI'),
                    'timestamp': timestamp,
                    'date': date,
                    'link': article.get('url', ''),
                    'sentiment_score': self.get_sentiment_score(f"{title} {description}"),
                    'market_relevant': True
                })
            return local_items

        # Run 4 API calls in parallel — 3 categories + 1 combined search query
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            cat_futures = [executor.submit(fetch_category, cat) for cat in categories]
            qry_futures = [executor.submit(fetch_query, q) for q in search_queries]
            all_futures = cat_futures + qry_futures

            for future in concurrent.futures.as_completed(all_futures, timeout=20):
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
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load Haiku classification cache: {e}", level="WARNING")
            gemini_cache = {}

        # Split into already classified vs new
        new_items = [i for i in items if i['headline'] not in gemini_cache]
        known_relevant = []
        for i in items:
            cached = gemini_cache.get(i['headline'], {})
            if cached.get('relevant') and cached.get('confidence', 0) >= 0.75:
                from datetime import timezone
                classified_at = cached.get('classified_at', '')
                if classified_at:
                    try:
                        classified_dt = datetime.fromisoformat(classified_at)
                        if classified_dt.tzinfo is None:
                            classified_dt = classified_dt.replace(tzinfo=timezone.utc)
                        age_hours = (datetime.now(timezone.utc) - classified_dt).total_seconds() / 3600
                        if age_hours > 48:
                            continue
                    except Exception as e:
                        pulse_logger.log(f"⚠️ Failed to parse classified_at timestamp in cache: {e}", level="WARNING")
                if cached.get('direction'):
                    direction = cached['direction']
                    i['sentiment_score'] = 0.8 if direction == 'bullish' else -0.8 if direction == 'bearish' else 0.0
                    i['gemini_direction'] = direction
                if cached.get('summary'):
                    i['description'] = cached['summary']
                if cached.get('uncertainty_score') is not None:
                    i['uncertainty_score'] = cached['uncertainty_score']
                known_relevant.append(i)

        # Articles not yet classified — use keyword filter as temporary pass
        keyword_passed = [
            i for i in new_items
            if self.is_market_relevant(i['headline'])
        ]

        # Return immediately — known relevant + keyword-passed new articles
        immediately_available = known_relevant + keyword_passed

        # Inject pinned stories not already covered by a live article
        pinned_stories = self.load_pinned_stories()
        current_headlines = {i['headline'] for i in immediately_available}
        injected = 0
        retired = 0
        surviving_pins = []
        for pin in pinned_stories:
            pin_headline = pin.get('headline', '')
            # Exact headline already present — skip without retiring
            if pin_headline in current_headlines:
                surviving_pins.append(pin)
                continue
            # Check if any live article covers the same underlying story
            superseded = any(
                self.is_same_story(live['headline'], pin_headline)
                for live in immediately_available
            )
            if superseded:
                pulse_logger.log(f"📌 Pinned story retired — covered by live article: {pin_headline[:60]}")
                retired += 1
                continue
            # No live coverage — inject the pin
            immediately_available.append({
                'headline': pin_headline,
                'description': pin.get('summary', ''),
                'source': pin.get('source', ''),
                'timestamp': pin.get('timestamp', ''),
                'date': pin.get('date', ''),
                'link': pin.get('link', ''),
                'sentiment_score': 0.8 if pin.get('direction') == 'bullish' else -0.8 if pin.get('direction') == 'bearish' else 0.0,
                'gemini_direction': pin.get('direction'),
                'uncertainty_score': pin.get('uncertainty_score', 0),
                'market_relevant': True,
                'pinned': True
            })
            current_headlines.add(pin_headline)
            surviving_pins.append(pin)
            injected += 1
        if retired:
            self.save_pinned_stories(surviving_pins)

        pulse_logger.log(f"⚡ Returning {len(immediately_available)} articles instantly ({len(known_relevant)} Haiku-verified, {len(keyword_passed)} keyword-passed, {injected} pinned)")

        # Run Gemini on new items in background
        if new_items:
            def background_classify():
                try:
                    pulse_logger.log(f"🤖 Haiku background classifying {len(new_items)} new articles with full text...")
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
                                    'summary': r.get('summary', ''),
                                    'uncertainty_score': r.get('uncertainty_score', 0),
                                    'classified_at': datetime.now().isoformat()
                                }
                        atomic_write_json(gemini_cache_file, gemini_cache)
                        self.update_pinned_store(new_items, classifications)
                        pulse_logger.log(f"✅ Haiku background done — {len(classifications)} articles classified with summaries")
                except Exception as e:
                    pulse_logger.log(f"⚠️ Background Haiku failed: {e}", level="WARNING")

            bg_thread = threading.Thread(target=background_classify, daemon=True)
            bg_thread.start()

        def sort_key(item):
            val = item.get('published_at') or item.get('date') or ''
            try:
                dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                sort_val = dt.isoformat()
                pulse_logger.log(f"[sort_key DEBUG] headline={item.get('headline', '')!r} sort_val={sort_val!r}")
                return sort_val
            except Exception:
                pass
            ts = item.get('timestamp') or ''
            try:
                dt = datetime.strptime(ts, "%b %d, %I:%M %p EST")
                dt = dt.replace(year=datetime.now().year)
                sort_val = dt.isoformat()
                pulse_logger.log(f"[sort_key DEBUG] headline={item.get('headline', '')!r} sort_val={sort_val!r} (from timestamp)")
                return sort_val
            except Exception:
                sort_val = val or ts
                pulse_logger.log(f"[sort_key DEBUG] headline={item.get('headline', '')!r} sort_val={sort_val!r} (fallback)")
                return sort_val

        immediately_available.sort(key=sort_key, reverse=True)

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
                    'predicted_impact': item.get('gemini_direction') if item.get('gemini_direction') else ('bearish' if item['sentiment_score'] < -0.3 else 'bullish' if item['sentiment_score'] > 0.3 else 'neutral'),
                    'context': item.get('description', '')
                })

        flags.sort(key=lambda x: x['priority'], reverse=True)
        return flags[:5]

    def calculate_score(self, items, flags):
        if not items:
            return 0.0
        scores = []
        for item in items:
            if item.get('gemini_direction'):
                scores.append(0.8 if item['gemini_direction'] == 'bullish' else -0.8 if item['gemini_direction'] == 'bearish' else 0.0)
            else:
                scores.append(item['sentiment_score'])
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
            existing = cache.load(self.cache_key)
            age_minutes = cache.get_age_minutes(self.cache_key)
            if existing and age_minutes < 3:
                pulse_logger.log("↺ Geopolitical — using cache (TheNewsAPI refresh every 3min)")
                return existing['data']

            items = self.fetch_news()

            if not items:
                pulse_logger.log("↺ Geopolitical — fetch empty, using last cache")
                if existing:
                    existing['data']['status'] = 'cached'
                    return existing['data']
                return None

            flags = self.identify_flags(items)
            score = self.calculate_score(items, flags)

            result = {
                'pillar': 'geopolitical',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'news_items': items[:10],
                'active_flags': flags,
                'total_items': len(items),
                'pillar_score': score,
                'status': 'live'
            }
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ Geopolitical updated | {len(flags)} active flags | {len(items)} articles | Score: {score}")
            return result

        except Exception as e:
            error_handler.handle(e, "Geopolitical")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return cached['data']
            return None

geopolitical_pipeline = GeopoliticalPipeline()
