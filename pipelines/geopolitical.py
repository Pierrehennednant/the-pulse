# v2026-06-28
import json
import os
import threading
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

class GeopoliticalPipeline:
    GEO_BLOCKLIST_FILE = "/data/geo_blocklist.json"

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
            pulse_logger.log("⚠️ Haiku unavailable — keyword-only scoring in effect for unclassified articles", level="WARNING")
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

KNOWN ARTICLE OVERRIDES — if an article matches one of these titles exactly, use the specified tier, direction, and reasoning. Do not apply your normal tiering logic to these articles:
- "U.S.-Iran negotiations postponed as Netanyahu blasts Hezbollah over apparent attacks" → Tier 1, bearish, reasoning: "Collapse of U.S.-Iran negotiations with simultaneous military escalation — direct threat to regional stability and oil supply."
- "U.S. Navy ends blockade of Iran's ports and coastal areas" → Tier 2, bullish, reasoning: "Naval de-escalation removes energy supply disruption risk — positive for risk sentiment."

Your job is to read each full article, then make five decisions:

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
- Oil prices falling due to peace deal / ceasefire / geopolitical de-escalation → risk premium removed → inflation eases → equities up → BULLISH. CRITICAL: do NOT classify a peace-deal-driven oil price drop as Bearish. Stocks jump on this, not fall.
- Oil prices falling due to demand destruction, recession fears, or oversupply → growth slowdown → BEARISH
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
[{{"id": 1, "relevant": true, "confidence": 0.95, "category": "geopolitical", "direction": "bearish", "reason": "Iran war escalation directly affects oil and risk sentiment", "summary": "Your 3-4 sentence market summary here.", "uncertainty_score": 85, "tier": 1, "reasoning": "Active war escalation directly threatens oil supply and broad risk sentiment."}}]

Use only "bearish", "bullish", or "neutral" for direction.
Use confidence between 0.0 and 1.0.
Use tier as an integer: 1, 2, or 3.
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

DECISION 5 — TIER CLASSIFICATION
Classify the magnitude of this event's market impact into one of three tiers, using the full article context — not headline keywords.

Tier 1 (±1.7): Active war or escalation between major powers, nuclear threats/incidents, major confirmed peace deals or ceasefires that meaningfully reduce geopolitical risk, or credible major supply disruptions (e.g. Hormuz closure threat).
Tier 2 (±0.75): Significant troop buildups, major diplomatic breakdowns, new meaningful sanctions, or credible energy market threats that are not Tier 1 level.
Tier 3 (±0.35): Minor diplomatic noise, corporate geopolitical news, speculative or secondary headlines with limited immediate market relevance.

Key rules:
- Prioritize actual market impact and context over headline keywords. The presence of words like "ceasefire" or "deal" does not automatically make something Tier 1 — evaluate whether a real, credible development occurred.
- When uncertain between tiers, default to the lower tier.
- De-escalation and peace developments are generally Bullish for US equities. Escalation and conflict are generally Bearish.
- Oil/Energy Rule: Falling oil prices caused by geopolitical de-escalation or peace deals are Bullish for equities. Only classify oil price moves as Bearish when driven by demand destruction, recession fears, or oversupply.

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
            # Pin vs pin deduplication — keep newest when two pins cover the same story
            if len(valid) > 1:
                try:
                    valid.sort(key=lambda s: s.get('pinned_at', ''), reverse=True)
                    deduped = []
                    for candidate in valid:
                        c_headline = candidate.get('headline', '')
                        if any(self.is_same_story(c_headline, kept.get('headline', '')) for kept in deduped):
                            pulse_logger.log(f"📌 Pinned story deduplicated (older duplicate retired): {c_headline[:60]}")
                            continue
                        deduped.append(candidate)
                    if len(deduped) < len(valid):
                        valid = deduped
                        self.save_pinned_stories(valid)
                except Exception as e:
                    pulse_logger.log(f"⚠️ Pin vs pin dedup failed: {e}", level="WARNING")

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
                'pinned_at': datetime.now(timezone.utc).isoformat()
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

    def backfill_missing_tiers(self, active_headlines):
        """One-time-per-article maintenance pass: assign Haiku contextual tier to any
        cached classification that predates the tier field. Restricted to headlines
        currently active in this pipeline run (active_headlines) — never scans the
        full historical cache. Skips entries that already have a valid tier — safe to
        call repeatedly, becomes a no-op once the active set is fully backfilled.
        Processes one article per Haiku call so a single malformed response can't
        block the rest of the batch."""
        if self.anthropic_client is None:
            return
        gemini_cache_file = "/data/gemini_classifications.json"
        try:
            if not os.path.exists(gemini_cache_file):
                return
            with open(gemini_cache_file, 'r') as f:
                gemini_cache = json.load(f)
        except Exception as e:
            pulse_logger.log(f"⚠️ Tier backfill — failed to load Haiku classification cache: {e}", level="WARNING")
            return

        active_set = set(active_headlines)
        missing = {
            headline: entry for headline, entry in gemini_cache.items()
            if headline in active_set and entry.get('relevant') and entry.get('tier') not in (1, 2, 3)
        }
        if not missing:
            return

        pulse_logger.log(f"🔄 Tier backfill — {len(missing)} active article(s) missing tier field, classifying via Haiku one at a time...")

        updated = 0
        for headline, entry in missing.items():
            context = entry.get('summary') or entry.get('reason') or ''

            prompt = f"""You are assisting a professional NQ and ES futures day trader with pre-market preparation.

Classify the magnitude of this article's market impact into one of three tiers, using the available context — not headline keywords.

Tier 1 (±1.7): Active war or escalation between major powers, nuclear threats/incidents, major confirmed peace deals or ceasefires that meaningfully reduce geopolitical risk, or credible major supply disruptions (e.g. Hormuz closure threat).
Tier 2 (±0.75): Significant troop buildups, major diplomatic breakdowns, new meaningful sanctions, or credible energy market threats that are not Tier 1 level.
Tier 3 (±0.35): Minor diplomatic noise, corporate geopolitical news, speculative or secondary headlines with limited immediate market relevance.

Key rules:
- Prioritize actual market impact and context over headline keywords. The presence of words like "ceasefire" or "deal" does not automatically make something Tier 1 — evaluate whether a real, credible development occurred.
- When uncertain between tiers, default to the lower tier.
- De-escalation and peace developments are generally Bullish for US equities. Escalation and conflict are generally Bearish.
- Oil/Energy Rule: Falling oil prices caused by geopolitical de-escalation or peace deals are Bullish for equities. Only classify oil price moves as Bearish when driven by demand destruction, recession fears, or oversupply.

Return ONLY a JSON object with no markdown, no explanation, no preamble. Exactly this format:
{{"tier": 1, "direction": "bullish", "reasoning": "One short sentence explaining the tier and direction choice", "confidence": 0.85}}

Use only "bearish", "bullish", or "neutral" for direction.
Use tier as an integer: 1, 2, or 3.
Use confidence between 0.0 and 1.0.

TITLE: {headline}
CONTEXT: {context}"""

            try:
                response = self.anthropic_client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=512,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = response.content[0].text.strip()
                if '```' in text:
                    text = text.split('```')[1]
                    if text.startswith('json'):
                        text = text[4:]
                r = json.loads(text)
            except Exception as e:
                pulse_logger.log(f"⚠️ Tier backfill — Haiku call failed for '{headline[:60]}': {e}", level="WARNING")
                continue

            tier = r.get('tier')
            if tier not in (1, 2, 3):
                pulse_logger.log(f"⚠️ Tier backfill — malformed tier '{tier}' for '{headline[:60]}', leaving for keyword fallback", level="WARNING")
                continue
            gemini_cache[headline]['tier'] = tier
            gemini_cache[headline]['tier_reasoning'] = r.get('reasoning', '')
            if r.get('direction'):
                gemini_cache[headline]['direction'] = r['direction']
            if r.get('confidence') is not None:
                gemini_cache[headline]['confidence'] = r['confidence']
            updated += 1
            pulse_logger.log(
                f"🧭 Tier backfill | {headline[:60]} | Tier {tier} | {r.get('direction', 'unknown')} | "
                f"conf={r.get('confidence', 0)} | {r.get('reasoning', '')}"
            )

        if updated:
            atomic_write_json(gemini_cache_file, gemini_cache)
            pulse_logger.log(f"✅ Tier backfill complete — {updated} active article(s) now have Haiku tier")

    def _ensure_geo_blocklist(self):
        """Seed /data/geo_blocklist.json from the repo-bundled default if it doesn't
        exist on the persistent volume yet. Never creates an empty file."""
        if os.path.exists(self.GEO_BLOCKLIST_FILE):
            return
        repo_seed = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'geo_blocklist.json')
        try:
            if os.path.exists(repo_seed):
                with open(repo_seed, 'r') as f:
                    seed = json.load(f)
                atomic_write_json(self.GEO_BLOCKLIST_FILE, seed)
                pulse_logger.log(f"📋 Geo blocklist seeded from repo default — {len(seed)} entries")
            else:
                pulse_logger.log("📋 Geo blocklist — no repo seed and no volume file, skipping")
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to seed geo blocklist: {e}", level="WARNING")

    def _seed_classifications(self):
        """Merge repo-bundled classification entries into /data/gemini_classifications.json
        without overwriting entries that already exist on the volume. This ensures locked
        tier/direction values reach production on deploy while preserving live Haiku
        classifications."""
        volume_file = "/data/gemini_classifications.json"
        repo_seed = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'gemini_classifications.json')
        try:
            if not os.path.exists(repo_seed):
                return
            with open(repo_seed, 'r') as f:
                seed = json.load(f)
            if not seed:
                return
            if os.path.exists(volume_file):
                with open(volume_file, 'r') as f:
                    existing = json.load(f)
            else:
                existing = {}
            merged = 0
            for title, entry in seed.items():
                if title not in existing:
                    existing[title] = entry
                    merged += 1
            if merged:
                atomic_write_json(volume_file, existing)
                pulse_logger.log(f"📋 Classification seed — merged {merged} new entry(ies) from repo default")
        except Exception as e:
            pulse_logger.log(f"⚠️ Classification seed failed: {e}", level="WARNING")

    def maybe_reset_geo_blocklist(self):
        """Clear the geo blocklist on Sunday weekly reset, matching EC blocklist schedule."""
        geo_blocklist_file = self.GEO_BLOCKLIST_FILE
        if datetime.now(self.timezone).weekday() != 6:
            return
        this_week = datetime.now(self.timezone).strftime('%Y-%W')
        try:
            if os.path.exists(geo_blocklist_file):
                with open(geo_blocklist_file, 'r') as f:
                    data = json.load(f)
                if isinstance(data, dict) and data.get('__reset_week__') == this_week:
                    return
                if isinstance(data, list) and len(data) == 0:
                    return
        except Exception:
            pass
        atomic_write_json(geo_blocklist_file, {'__reset_week__': this_week})
        pulse_logger.log("🗑️ Geo blocklist cleared — Sunday weekly reset")

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
                age_days = (datetime.now(pytz.utc) - dt.replace(tzinfo=pytz.utc) if dt.tzinfo is None else datetime.now(pytz.utc) - dt).days
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
        if not THENEWS_API_KEY:
            pulse_logger.log("⚠️ THENEWS_API_KEY not set — skipping geopolitical news fetch", level="WARNING")
            return []
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
                f"&published_after={(datetime.now(pytz.utc) - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S')}"
                f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,abcnews.go.com,washingtonpost.com,nytimes.com"
            )
            response = fetch_with_retry(url, timeout=6, retries=2)
            if not response.ok:
                pulse_logger.log(f"⚠️ TheNewsAPI top stories returned {response.status_code} — skipping", level="WARNING")
                return {}
            return response.json()

        def fetch_query(query):
            url = (
                f"https://api.thenewsapi.com/v1/news/all"
                f"?api_token={THENEWS_API_KEY}"
                f"&language=en"
                f"&search={requests.utils.quote(query)}"
                f"&sort=published_at"
                f"&limit=25"
                f"&published_after={(datetime.now(pytz.utc) - timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S')}"
                f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,washingtonpost.com,nytimes.com"
            )
            response = fetch_with_retry(url, timeout=6, retries=2)
            if not response.ok:
                pulse_logger.log(f"⚠️ TheNewsAPI query '{query}' returned {response.status_code} — skipping", level="WARNING")
                return {}
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
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    est = dt.astimezone(pytz.timezone(TIMEZONE))
                    timestamp = est.strftime('%b %d, %I:%M %p EST')
                    date = est.strftime('%Y-%m-%d')
                    age_days = (datetime.now(pytz.utc) - dt).days
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

            try:
                completed = concurrent.futures.as_completed(all_futures, timeout=40)
                for future in completed:
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
            except concurrent.futures.TimeoutError:
                pending = sum(1 for f in all_futures if not f.done())
                pulse_logger.log(f"⚠️ Geo futures timeout — {pending} fetch(es) did not complete; using {len(items)} articles collected so far", level="WARNING")

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
                if cached.get('confidence') is not None:
                    i['haiku_confidence'] = cached['confidence']
                if cached.get('tier') in (1, 2, 3):
                    i['haiku_tier'] = cached['tier']
                if cached.get('tier_reasoning'):
                    i['haiku_tier_reasoning'] = cached['tier_reasoning']
                known_relevant.append(i)

        # Articles not yet classified — use keyword filter as temporary pass
        keyword_passed = [
            i for i in new_items
            if self.is_market_relevant(i['headline'])
        ]

        # Return immediately — known relevant + keyword-passed new articles
        immediately_available = known_relevant + keyword_passed

        # Deduplicate by story — sort newest first, then drop older articles covering the same story
        def _live_sort_key(item):
            for field in ('timestamp', 'published_at', 'date'):
                val = item.get(field) or ''
                if val:
                    try:
                        return dateutil_parser.parse(val, default=datetime.now(timezone.utc), tzinfos={"EST": -18000, "EDT": -14400}).isoformat()
                    except Exception:
                        pass
            return datetime.min.replace(tzinfo=timezone.utc).isoformat()

        immediately_available.sort(key=_live_sort_key, reverse=True)
        deduped_live = []
        for candidate in immediately_available:
            c_headline = candidate.get('headline', '')
            if any(self.is_same_story(c_headline, kept.get('headline', '')) for kept in deduped_live):
                pulse_logger.log(f"📰 Live article deduplicated (older duplicate dropped): {c_headline[:60]}")
                continue
            deduped_live.append(candidate)
        immediately_available = deduped_live

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

        # Geo blocklist — drop articles matching any blocked string after all sources merged
        geo_blocklist_file = self.GEO_BLOCKLIST_FILE
        try:
            if os.path.exists(geo_blocklist_file):
                with open(geo_blocklist_file, 'r') as f:
                    raw = json.load(f)
                geo_blocklist = raw if isinstance(raw, list) else []
            else:
                geo_blocklist = []
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load geo blocklist: {e}", level="WARNING")
            geo_blocklist = []
        if geo_blocklist:
            before = len(immediately_available)
            kept = []
            for i in immediately_available:
                headline_lower = i['headline'].lower()
                matched = [b for b in geo_blocklist if b.lower() in headline_lower]
                if matched:
                    pulse_logger.log(f"🚫 Geo blocklist HIT | {i['headline'][:80]} | matched: {matched[0][:60]}")
                else:
                    pulse_logger.log(f"✅ Geo blocklist PASS | {i['headline'][:80]}")
                    kept.append(i)
            immediately_available = kept
            blocked = before - len(immediately_available)
            if blocked:
                pulse_logger.log(f"🚫 Geo blocklist — {blocked} article(s) filtered out")

        pulse_logger.log(f"⚡ Returning {len(immediately_available)} articles instantly ({len(known_relevant)} Haiku-verified, {len(keyword_passed)} keyword-passed, {injected} pinned)")

        # Backfill tier for cache entries that predate contextual tiering — restricted
        # to headlines active in this run, processed one Haiku call per article.
        active_headlines = [i['headline'] for i in immediately_available]
        threading.Thread(target=self.backfill_missing_tiers, args=(active_headlines,), daemon=True).start()

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
                                headline = new_items[idx]['headline']
                                tier = r.get('tier')
                                if tier not in (1, 2, 3):
                                    if tier is not None:
                                        pulse_logger.log(f"⚠️ Haiku returned malformed tier '{tier}' for '{headline[:60]}' — falling back to keyword tiering", level="WARNING")
                                    tier = None
                                gemini_cache[headline] = {
                                    'relevant': r.get('relevant', False),
                                    'confidence': r.get('confidence', 0),
                                    'category': r.get('category', ''),
                                    'direction': r.get('direction', None),
                                    'reason': r.get('reason', ''),
                                    'summary': r.get('summary', ''),
                                    'uncertainty_score': r.get('uncertainty_score', 0),
                                    'tier': tier,
                                    'tier_reasoning': r.get('reasoning', ''),
                                    'classified_at': datetime.now(timezone.utc).isoformat()
                                }
                                pulse_logger.log(
                                    f"🧭 Haiku tier | {headline[:60]} | Tier {tier if tier is not None else 'N/A (fallback)'} | "
                                    f"{r.get('direction', 'unknown')} | conf={r.get('confidence', 0)} | {r.get('reasoning', '')}"
                                )
                        atomic_write_json(gemini_cache_file, gemini_cache)
                        self.update_pinned_store(new_items, classifications)
                        pulse_logger.log(f"✅ Haiku background done — {len(classifications)} articles classified with summaries")
                except Exception as e:
                    pulse_logger.log(f"⚠️ Background Haiku failed: {e}", level="WARNING")

            bg_thread = threading.Thread(target=background_classify, daemon=True)
            bg_thread.start()

        def sort_key(item):
            for field in ('timestamp', 'published_at', 'date'):
                val = item.get(field) or ''
                if val:
                    try:
                        parsed = dateutil_parser.parse(val, default=datetime.now(timezone.utc), tzinfos={"EST": -18000, "EDT": -14400})
                        return parsed.isoformat()
                    except Exception:
                        pass
            return datetime.min.replace(tzinfo=timezone.utc).isoformat()

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

    def _get_article_priority(self, item):
        """Return keyword-based flag priority for an article (0 = no qualifying keyword)."""
        _keywords = {
            'nuclear': 95, 'war': 90, 'invasion': 92, 'missile': 88,
            'attack': 85, 'bomb': 85, 'default': 85, 'fomc': 85,
            'rate hike': 80, 'rate cut': 80, 'powell': 78,
            'federal reserve': 78, 'tariff': 80, 'debt ceiling': 80,
            'shutdown': 75, 'sanctions': 75, 'troops': 78,
            'recession': 75, 'ceasefire': 70, 'deal': 65,
            'agreement': 65, 'escalation': 72, 'inflation': 70,
            'gdp': 68, 'jobs': 67
        }
        _low_quality = [
            'truthout', 'rawstory', 'mediaite', 'salon', 'huffpost', 'breitbart',
            'dailykos', 'thegatewaypundit', 'thestockmarketwatch.com', 'rt.com',
            'sputnik', 'investing.com', 'economictimes.indiatimes.com', 'asiaone.com',
            'indiatimes.com', 'timesofindia.com', 'hindustantimes.com',
            'thecanary.co', 'uctoday.com', 'tass.com', 'tass.ru',
            'sputniknews.com', 'presstv.ir'
        ]
        _trusted = [
            'reuters', 'associated press', 'cnbc', 'bloomberg',
            'wall street journal', 'financial times', 'politico', 'axios',
            'marketwatch', 'fox business', 'the hill'
        ]
        text = item.get('headline', '').lower()
        source = item.get('source', '').lower()
        if any(lqs in source for lqs in _low_quality):
            return 0
        priority = 0
        for keyword, kw_score in _keywords.items():
            if keyword in text:
                if kw_score > priority:
                    priority = kw_score
        if priority >= 65 and any(ts in source for ts in _trusted):
            priority = min(priority + 5, 99)
        return priority

    def calculate_score(self, items, flags):
        """Three-tier magnitude-weighted score. Tier comes from Haiku's contextual classification
        (haiku_tier, set during background classification) when available — Haiku evaluates real
        market impact/context rather than headline keywords. Falls back to keyword priority /
        confidence thresholds when Haiku tier is missing or malformed.
        Tier 1 (±1.7, 4× weight) | Tier 2 (±0.75, 2× weight) | Tier 3 (±0.35, 1× weight)
        Haiku path: article_score = tier_base × haiku_confidence.
        Fallback path: article_score = tier_base × confidence × flag_multiplier
        (flag_multiplier = 1 + 0.2 × priority/100 when priority ≥65).
        """
        if not items:
            return 0.0
        weighted_sum = 0.0
        total_weight = 0.0
        haiku_tier_count = 0
        fallback_tier_count = 0
        tier_map = {1: (1.7, 4.0), 2: (0.75, 2.0), 3: (0.35, 1.0)}
        for item in items:
            # Direction
            direction = item.get('gemini_direction')
            if direction == 'bullish':
                sign = 1.0
            elif direction == 'bearish':
                sign = -1.0
            else:
                s = item.get('sentiment_score', 0.0)
                sign = 1.0 if s > 0 else (-1.0 if s < 0 else 0.0)
            if sign == 0.0:
                continue
            # Haiku confidence (None for unclassified articles)
            haiku_conf = item.get('haiku_confidence')
            haiku_tier = item.get('haiku_tier')
            if haiku_tier in tier_map:
                haiku_tier_count += 1
                tier_score, tier_weight = tier_map[haiku_tier]
                conf = haiku_conf if haiku_conf is not None else 1.0
                article_score = tier_score * conf
                pulse_logger.log(
                    f"🧭 Geo tier (Haiku) | {item.get('headline', '')[:60]} | Tier {haiku_tier} | {direction} | "
                    f"conf={conf} | {item.get('haiku_tier_reasoning', '')}"
                )
            else:
                fallback_tier_count += 1
                # Fallback — keyword-based tier classification (used when Haiku tier
                # is missing/malformed, e.g. unclassified article or failed batch)
                kw_priority = self._get_article_priority(item)
                if (kw_priority >= 80) or (haiku_conf is not None and haiku_conf >= 0.85):
                    tier_score, tier_weight = 1.7, 4.0
                elif (65 <= kw_priority <= 79) or (haiku_conf is not None and 0.65 <= haiku_conf < 0.85):
                    tier_score, tier_weight = 0.75, 2.0
                else:
                    tier_score, tier_weight = 0.35, 1.0
                conf = haiku_conf if haiku_conf is not None else 1.0
                article_score = tier_score * conf
                if kw_priority >= 65:
                    article_score *= (1 + 0.2 * kw_priority / 100)
                pulse_logger.log(
                    f"🧭 Geo tier (keyword fallback) | {item.get('headline', '')[:60]} | base={tier_score} | "
                    f"priority={kw_priority} | {direction} | conf={conf}"
                )
            weighted_sum += article_score * sign * tier_weight
            total_weight += tier_weight
        tiered_count = haiku_tier_count + fallback_tier_count
        if tiered_count:
            pulse_logger.log(
                f"📊 Geo tier source ratio — Haiku: {haiku_tier_count}/{tiered_count} "
                f"({round(haiku_tier_count / tiered_count * 100)}%) | Keyword fallback: {fallback_tier_count}/{tiered_count}"
            )
        if total_weight == 0:
            return 0.0
        return round(max(-2.0, min(2.0, weighted_sum / total_weight)), 2)

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
                pinned = self.load_pinned_stories()
                if pinned:
                    pulse_logger.log(f"⚠️ Geopolitical — News API unavailable and no cache, using {len(pinned)} pinned stories only", level="WARNING")
                    flags = self.identify_flags(pinned)
                    score = self.calculate_score(pinned, flags)
                    return {
                        'pillar': 'geopolitical',
                        'timestamp': datetime.now(self.timezone).isoformat(),
                        'news_items': pinned[:10],
                        'active_flags': flags,
                        'total_items': len(pinned),
                        'pillar_score': score,
                        'status': 'pinned_only'
                    }
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
