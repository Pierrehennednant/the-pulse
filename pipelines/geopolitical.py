import copy
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
from pipelines.event_canon import ACTION_CATEGORIES, compute_event_id
from pipelines.event_store import event_store

MAX_ARTICLE_AGE_HOURS = 48


class GeopoliticalPipeline:
    GEO_BLOCKLIST_FILE = "/data/geo_blocklist.json"
    GEO_MANUAL_BLOCKLIST_FILE = "/data/geo_manual_blocklist.json"

    # Fields a resolved Haiku classification carries (gemini_cache's per-
    # headline entry shape) and what each is called once it's on a scored
    # item (calculate_score()/identify_flags() read the right-hand names).
    # A pin record uses the SAME left-hand names as a classification entry
    # (see PIN_CLASSIFICATION_FIELDS) specifically so this one mapping also
    # projects a pin onto a scored item at injection time. Add ONE entry
    # here when the classification schema grows a field that needs to
    # survive onto a scored item — not a line in three separate hand-typed
    # dict literals (known_relevant's cache-hit path,
    # _merge_fresh_classifications(), fetch_news()'s pin-injection block).
    # Confirmed via direct trace (Sep 2026): three independently
    # maintained copy sites had silently diverged — pins were missing
    # kind, tier_reasoning, and confidence at various points; the
    # cache-refresh-promotion path was missing kind and uncertainty_score.
    # This map is the fix for that class of bug recurring one field at a
    # time, not just for the specific fields found so far.
    CLASSIFICATION_TO_ITEM_FIELDS = {
        'direction': 'gemini_direction',
        'tier': 'haiku_tier',
        'confidence': 'haiku_confidence',
        'tier_reasoning': 'haiku_tier_reasoning',
        'uncertainty_score': 'uncertainty_score',
        'kind': 'kind',
        'gate_pass': 'haiku_gate_pass',
    }
    # Same names as the map's keys — what a pin record carries forward,
    # unrenamed, from the classification that created it.
    PIN_CLASSIFICATION_FIELDS = tuple(CLASSIFICATION_TO_ITEM_FIELDS.keys())

    # App-layer EC fold (Sep 18 Warsh fix) — a recap/analysis article whose
    # only substance is reaction to an event the Economic Calendar pillar
    # ALREADY scored (this week's FOMC decision/press conference/SEP) must
    # not also open a second Geo score for the same event. Haiku cannot see
    # EC state, so this is a deterministic, code-side join computed fresh
    # every calculate_score() call — never persisted onto a pin or the
    # classification cache, since EC's own state (has an actual been
    # entered yet?) can change from cycle to cycle. EVENT-TYPE MATCH + SAME
    # MEETING WINDOW + EC ROW ALREADY HAS AN ACTUAL are decided mechanically
    # by _check_ec_fold() below; NO NEW FACT is NOT re-derived here — it is
    # Haiku's own first_print/follow_up judgment (see the FOMC /
    # RATE-DECISION RECAP RULE in the classification prompt), already
    # resolved onto item['kind'] by the time calculate_score() runs. Fold
    # triggers only when the matcher AND Haiku's kind agree.
    FOMC_FOLD_EVENT_TITLES = (
        'Federal Funds Rate', 'FOMC Statement', 'FOMC Press Conference', 'FOMC Economic Projections',
    )
    # "SEP" deliberately excluded as a bare token — \bsep\b also matches the
    # "Sep 18" date-abbreviation style common in headlines/timestamps, which
    # would false-positive the event-type match on unrelated September news.
    # "economic projections" below covers the legitimate case without that
    # collision. No person's name appears in this list or the one below —
    # a name is never sufficient on its own to trigger a fold.
    FOMC_FOLD_PRIMARY_PHRASES = (
        'fomc', 'fomc meeting', 'press conference', 'economic projections',
        'federal funds', 'rate decision',
    )
    # "this week's hike" / "this week's rate increase" per spec, matched as
    # two independent tokens (this week + a rate-move word) rather than one
    # exact contiguous phrase — real headlines vary the wording in between
    # ("this week's 25 bp hike", "this week's rate increase to 4.5%").
    FOMC_FOLD_WEEK_PHRASE = 'this week'
    FOMC_FOLD_WEEK_ACTION_WORDS = ('hike', 'rate increase', 'rate hike', 'rate cut')
    FOMC_FOLD_WINDOW_DAYS = 10

    # Stage 4 of the structural first_print/follow_up rewrite: price-move /
    # pure market-tape analysis never enters Geo, decided MECHANICALLY
    # (_code_side_bucket_override below), not by Pass A's own bucket call.
    # Seed list, grows the same way FOMC_FOLD_PRIMARY_PHRASES/ignore_keywords
    # already do, from real observed misses — not meant to be exhaustive on
    # day one.
    TAPE_PATTERN_PHRASES = (
        'yields', 'vix', 'oil settle', 'oil settles', 'live updates',
    )

    # Max articles per classify_relevance_batch() call before chunking
    # (see _classify_relevance_batch_chunked() below). Confirmed live,
    # 2026-09-23: a single 25-article batch failed all 3 retry attempts
    # with stop_reason='max_tokens' — this schema's per-article output
    # (summary + reason + reasoning + tier/direction/confidence/category/
    # gate_pass) is heavy enough that 25 genuinely overflows the output
    # budget, and _call_haiku_classify()'s retry logic cannot recover from
    # this failure mode at all (retrying the identical oversized prompt
    # against the identical token cap truncates the same way every time).
    # Set well under that demonstrated-unsafe value, not empirically tuned
    # against it — no live access to binary-search the real boundary from
    # here, so conservative headroom over precision.
    HAIKU_CLASSIFY_CHUNK_SIZE = 10

    def _load_ec_fomc_anchors(self):
        """Dated EC calendar rows (this week's FOMC-day events) that already
        have a confirmed actual — the "EC already owns this event" anchor
        set _check_ec_fold() checks new Geo articles against. Read directly
        from the EC pillar's own persisted cache (same low-coupling pattern
        dashboard.py's _run_partial_refresh() already uses to read other
        pillars' caches) rather than threading EC state through
        fetch_news()'s call signature — Haiku never sees this, only this
        mechanical matcher does. Returns a list of 'YYYY-MM-DD' strings.

        SECOND SOURCE, MERGED IN — confirmed real, not fixed as a side
        effect of any prior change: the economic_calendar cache's `events`
        array only gains a manually-entered actual if that event row was
        already present in the cache at the moment
        ui/dashboard.py's manual-input-save route ran its in-place update
        (it loops over `ec_data.get('events', [])` and mutates the
        matching row — if the row isn't there, e.g. a genuinely quiet week
        with 0 EC events cached, the loop finds nothing and silently
        no-ops). The actual still lands in
        /data/permanent_manual_inputs.json (manual_input_pipeline's own
        permanent store) either way, but this method previously never read
        that file — so a same-day manual FOMC actual entered while the EC
        cache had no matching row was invisible to the fold matcher until
        the next full economic_calendar_pipeline.fetch() happened to
        re-run and merge it via apply_manual_inputs(). Reading
        permanent_manual_inputs.json directly here closes that gap without
        depending on the EC cache's row-mutation path or a fetch cycle
        landing first."""
        anchors = set()
        try:
            ec_cached = cache.load('economic_calendar')
            events = ec_cached.get('data', {}).get('events', []) if ec_cached else []
        except Exception as e:
            pulse_logger.log(f"⚠️ EC fold — failed to load economic_calendar cache: {e}", level="WARNING")
            events = []
        for e in events:
            if e.get('title') not in self.FOMC_FOLD_EVENT_TITLES:
                continue
            actual = e.get('actual')
            if actual in (None, '', 'Pending', 'N/A'):
                continue
            event_date = e.get('event_date', '')
            if event_date:
                anchors.add(event_date)

        try:
            from pipelines.manual_input import manual_input_pipeline
            manual_inputs = manual_input_pipeline.get_inputs()
        except Exception as e:
            pulse_logger.log(f"⚠️ EC fold — failed to load permanent_manual_inputs.json: {e}", level="WARNING")
            manual_inputs = {}
        for key, entry in manual_inputs.items():
            actual = entry.get('actual') if isinstance(entry, dict) else None
            if actual in (None, '', 'Pending', 'N/A'):
                continue
            # Key is 'Title::YYYY-MM-DD' when saved with an event_date, or
            # bare 'Title' (legacy / no event_date at save time) otherwise.
            title, _, event_date = key.partition('::')
            if title not in self.FOMC_FOLD_EVENT_TITLES:
                continue
            if event_date:
                anchors.add(event_date)
            else:
                # No event_date on the manual entry itself — fall back to
                # its save timestamp's date so a real entered actual still
                # anchors the fold instead of being silently dropped.
                ts = entry.get('timestamp', '')
                if ts:
                    anchors.add(ts[:10])

        return sorted(anchors)

    def _ec_fold_matches(self, item, ec_anchors):
        """Sections (a)-(c) of the app-layer EC fold ONLY — event-type
        phrase match, same meeting window, EC row already has an actual.
        No reference to kind/condition (d) here. Shared by:
          - _check_ec_fold() (ingest-time — ALSO gates on Haiku's fresh
            kind judgment for condition (d), see that method), and
          - _reevaluate_pinned_ec_folds() (the pin re-evaluation pass —
            deliberately does NOT gate on stored kind; see that method's
            docstring for why that's not the same thing as re-deriving
            condition (d) with a new string rule).
        Returns the matched EC event_date string ('YYYY-MM-DD') on a
        match, or None. Never keys on a person's name alone — no names
        appear in either phrase list this checks."""
        if not ec_anchors:
            return None
        text = f"{item.get('headline', '')} {item.get('description', '')} {item.get('summary', '')}".lower()
        event_type_match = any(self._keyword_matches(text, p) for p in self.FOMC_FOLD_PRIMARY_PHRASES)
        if not event_type_match:
            has_week_phrase = self._keyword_matches(text, self.FOMC_FOLD_WEEK_PHRASE)
            has_action_word = any(self._keyword_matches(text, w) for w in self.FOMC_FOLD_WEEK_ACTION_WORDS)
            event_type_match = has_week_phrase and has_action_word
        if not event_type_match:  # (a)
            return None
        article_date_str = (
            (item.get('published_at') or '').strip()
            or (item.get('date') or '').strip()
            or (item.get('timestamp') or '').strip()
        )
        parsed = self._pin_parsed_timestamp(article_date_str)
        if parsed is None:
            return None  # fail closed — can't confirm the meeting window
        for event_date_str in ec_anchors:
            try:
                event_dt = datetime.strptime(event_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                continue
            if event_dt <= parsed <= event_dt + timedelta(days=self.FOMC_FOLD_WINDOW_DAYS):
                return event_date_str  # (b) + (c) together
        return None

    def _check_ec_fold(self, item, ec_anchors):
        """Section 2 app-layer fold (ingest-time). ALL of the following
        must hold:
          (a)+(b)+(c) — mechanical, see _ec_fold_matches() above.
          (d) NO NEW FACT — NOT re-derived here; consumes Haiku's own kind
              judgment already on the item (kind == 'follow_up'). A
              first_print item never folds, regardless of (a)/(b)/(c) —
              it may still be a genuine new-fact story EC doesn't have.
        """
        if item.get('kind') != 'follow_up':  # (d) — Haiku's own call, not re-derived here
            return False
        return self._ec_fold_matches(item, ec_anchors) is not None

    def _reevaluate_pinned_ec_folds(self):
        """Code-only pass over currently-pinned stories, run on every
        fetch() call regardless of whether a live TheNewsAPI fetch happens
        this cycle. Closes the gap the ingest-time fold above doesn't
        touch: that fold only ever runs against a NEW article's fresh
        Haiku kind; the only thing that can change an EXISTING pin's
        stored kind afterward is the reactive two-agreeing-merges
        duplicate-merge path (_refresh_pin_classification()), which
        requires a NEW matching article to show up at all. A pin sitting
        alone with nothing new published about it never got re-evaluated
        — exactly what happened to the Sep 18 Warsh pin and the Sep 15
        NBC pin. No Haiku call here — never re-prompts Haiku on every pin.

        CONDITION (d) FOR THIS PASS, STATED EXPLICITLY (per instruction —
        not left to be inferred): unlike ingest-time classification, this
        pass does NOT require the pin's own stored kind to already be
        'follow_up' before folding it. It uses ONLY the mechanical
        (a)-(c) match (_ec_fold_matches()) as sufficient grounds to
        correct a pin's kind. This is a deliberate deviation from "use
        the pin's stored kind for (d)" — reasoned as follows, not slipped
        in quietly:
          - Every pin currently on record was classified before this fold
            mechanism existed at all. Its stored kind reflects Haiku's
            judgment in a world where "does EC already own this event"
            was never asked — unlike a freshly-classified article under
            the CURRENT prompt, whose kind IS a meaningful signal for (d)
            precisely because the prompt now explicitly asks Haiku to
            weigh this exact pattern (see the FOMC / RATE-DECISION RECAP
            RULE). A legacy pin's stored kind is not evidence either way.
          - Requiring stored kind == 'follow_up' here would make this
            pass a structural no-op for every pin that predates the fold
            (i.e. every pin that exists today) — it could never correct
            the Sep 18/Sep 15-style pins this fix exists to catch, which
            is the literal acceptance criterion (test A: no new article,
            pin alone must still be corrected).
          - This is NOT a new string-based "no new fact" re-derivation —
            no new phrase list or NLP heuristic is added beyond (a),
            which was already approved for the ingest-time fold.
        KNOWN LIMITATION, stated plainly: this means the pass cannot
        distinguish "a pure recap" from "a sharp new fact that also
        happens to name the press conference/rate decision and falls in
        the same date window" — only a live Haiku read of the article
        text can make that call reliably, which is exactly why ingest-
        time classification keeps requiring kind == 'follow_up'
        explicitly and this pass does not. FOMC_FOLD_PRIMARY_PHRASES are
        specific enough that this is a narrow risk in practice, but it is
        not the same guarantee ingest-time classification provides.

        "Score before/after" is logged at the pinned-set level (this
        pass's own calculate_score() over the pin list alone, before vs
        after any corrections) — not a full pillar total, since live
        articles for this cycle aren't fetched yet at this point in
        fetch(), and not a per-pin score, since that would require
        pulling calculate_score()'s inline tier/confidence/sign math out
        into its own reusable function — a larger refactor than this fix
        calls for. Flagging this interpretation explicitly rather than
        letting it pass unstated.
        """
        try:
            pins = self.load_pinned_stories()
        except Exception as e:
            pulse_logger.log(f"⚠️ Pin EC-fold re-evaluation — failed to load pins: {e}", level="WARNING")
            return
        if not pins:
            return
        ec_anchors = self._load_ec_fomc_anchors()
        score_before = self.calculate_score([dict(p) for p in pins], [])
        changed = False
        for pin in pins:
            headline = pin.get('headline', '(untitled)')
            old_kind = pin.get('kind', 'first_print')
            matched_date = self._ec_fold_matches(pin, ec_anchors)
            if matched_date is not None and old_kind != 'follow_up':
                pin['kind'] = 'follow_up'
                changed = True
                pulse_logger.log(
                    f"🔗 Pin EC-fold re-evaluation | '{headline[:60]}' | matched EC {matched_date} | "
                    f"kind {old_kind} → follow_up (now excluded from score)"
                )
            elif matched_date is not None:
                pulse_logger.log(
                    f"🔗 Pin EC-fold re-evaluation | '{headline[:60]}' | matched EC {matched_date} | "
                    f"kind already follow_up, no change"
                )
            else:
                pulse_logger.log(
                    f"🔗 Pin EC-fold re-evaluation | '{headline[:60]}' | no_ec_match | "
                    f"kind unchanged ({old_kind})"
                )
        if changed:
            self.save_pinned_stories(pins)
            score_after = self.calculate_score([dict(p) for p in pins], [])
            pulse_logger.log(
                f"🔗 Pin EC-fold re-evaluation — pinned-set score {score_before} → {score_after}"
            )

    def _apply_classification_fields(self, item, source):
        """Copy CLASSIFICATION_TO_ITEM_FIELDS from `source` (a
        gemini_cache entry or a pin record — both use the classification's
        own field names) onto `item` (a scored item dict). Single source
        of truth for known_relevant's cache-hit path,
        _merge_fresh_classifications(), and fetch_news()'s pin-injection
        block."""
        for src_field, item_field in self.CLASSIFICATION_TO_ITEM_FIELDS.items():
            val = source.get(src_field)
            if val is not None:
                item[item_field] = val

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
            # Tech/AI mega-deal terminology — deliberately does NOT include
            # bare company/ticker names (see self.company_keywords below).
            # A priority company name alone must be paired with one of THESE
            # words to satisfy Layer 2 — a bare mention is not enough, or
            # every routine company story (a product launch, an earnings
            # beat, an executive hire) would clear this gate purely by
            # naming one of those companies, regardless of subject matter.
            'deal', 'deals', 'partnership', 'partnerships',
            'billion', 'billions', 'chip', 'chips', 'capex',
            'ai infrastructure',
            'acquisition', 'acquisitions', 'acquire', 'acquires', 'acquired',
            'merger', 'mergers', 'merge', 'merges', 'merged',
            # Mega-cap regulatory/legal outcome terminology
            'settlement', 'settlements', 'lawsuit', 'lawsuits',
            'verdict', 'verdicts', 'fined', 'fines',
            'judgment', 'judgments', 'judgement', 'judgements',
            'antitrust'
        ]
        # Priority tech/AI mega-cap companies + tickers. NOT part of
        # market_keywords above — a bare mention of one of these alone does
        # NOT satisfy is_market_relevant()'s Layer 2 allowlist. It must
        # appear alongside an actual market_keyword (a deal, capex,
        # regulatory, or macro term) in the same text. This is what let a
        # pure product-feature story ("Apple releases test of redesigned
        # Siri AI before iPhone 18 hits stores this week") clear Gate 1
        # purely by naming "Apple," with no requirement that the story be
        # about a deal, capex commitment, or regulatory action.
        self.company_keywords = [
            'nvidia', 'nvda', 'apple', 'aapl', 'microsoft', 'msft',
            'alphabet', 'googl', 'google', 'amazon', 'amzn', 'meta',
            'broadcom', 'avgo', 'amd', 'intel', 'intc',
            'taiwan semiconductor', 'tsm', 'coreweave',
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

    def _fetch_and_stamp_articles(self, articles):
        """Parallel-fetch each article's full text (15s wall-clock cap,
        falls back to description on timeout/failure), stamping
        `_text_source`/`_full_text` onto each article dict in place.
        Factored out of classify_relevance_batch() so the new two-pass
        path (classify_relevance_batch_v2) shares the IDENTICAL fetch/
        timeout/fallback behavior instead of a second hand-copied version
        — same reasoning as why _build_classification_prompt was
        originally factored out for force_reclassify() to share."""
        # Fetch article URLs in parallel — sequential fetching at up to 24 s/URL
        # (fetch_with_retry default retries × 8 s timeout) accumulates to 240+ s
        # for a 10-article batch, stalling or killing the Haiku call entirely.
        def _fetch_one(art):
            return self.fetch_full_article(art.get('link', ''), art.get('description', ''))

        results_map = {i: articles[i].get('description', '') for i in range(len(articles))}
        text_source_map = {i: 'description_fallback' for i in range(len(articles))}
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(articles), 6))
        future_to_idx = {executor.submit(_fetch_one, art): i for i, art in enumerate(articles)}
        try:
            for future in concurrent.futures.as_completed(future_to_idx, timeout=15):
                idx = future_to_idx[future]
                try:
                    fetched = future.result()
                    results_map[idx] = fetched
                    # Mark as full_text only when fetch_full_article() returned something
                    # substantively different from the description fallback.
                    if fetched and fetched != articles[idx].get('description', ''):
                        text_source_map[idx] = 'full_text'
                except Exception:
                    pass  # keep description_fallback for this URL
        except concurrent.futures.TimeoutError:
            timed_out = sum(1 for f in future_to_idx if not f.done())
            if timed_out:
                pulse_logger.log(
                    f"⚠️ Article URL fetch — {timed_out} URL(s) timed out, falling back to description",
                    level="WARNING"
                )
        finally:
            executor.shutdown(wait=False)  # don't block on threads still running after timeout

        # Stamp each article so callers can write text_source into gemini_cache,
        # and so callers (update_pinned_store(), background_classify()'s
        # gemini_cache write) can persist the STORED full text for later
        # forced reclassification (see force_reclassify()) — never re-fetched
        # from story_url at reclassify time, since pages get edited,
        # paywalled, or redirected after the fact.
        for i, article in enumerate(articles):
            article['_text_source'] = text_source_map[i]
            article['_full_text'] = results_map[i]

    def classify_relevance_batch(self, articles):
        """Use Claude Haiku to classify articles with full article context and generate clean summaries."""
        if not articles:
            return []
        if self.anthropic_client is None:
            pulse_logger.log("⚠️ Haiku unavailable — keyword-only scoring in effect for unclassified articles", level="WARNING")
            return []

        self._fetch_and_stamp_articles(articles)

        # Build batch input
        article_list = ""
        for i, article in enumerate(articles):
            article_list += f"{i+1}. TITLE: {article['headline']}\n   FULL TEXT: {article['_full_text']}\n\n"

        prompt = self._build_classification_prompt(article_list)
        return self._call_haiku_classify(prompt)

    def _classify_relevance_batch_chunked(self, articles, chunk_size=None):
        """Chunked wrapper around classify_relevance_batch() — same output
        contract (a list of dicts with a 1-based 'id' matching `articles`'
        order, remapped back to global indices), safe for a live batch of
        any size.

        REAL BUG THIS FIXES, confirmed live 2026-09-23: a single
        classify_relevance_batch() call on a 25-article batch failed all 3
        retry attempts with stop_reason='max_tokens'. Retrying the
        identical oversized prompt against the identical token cap
        truncates the same way every time, so _call_haiku_classify()'s
        existing retry logic cannot recover from this failure mode at all
        — confirmed by the observed "failed every attempt" behavior, not
        just reasoned about. The whole cycle's new_items then classifies
        as [], background_classify()'s `if classifications:` gate skips
        every write for that cycle, and since gemini_classifications.json
        is append-only (nothing ever deletes a key), those headlines
        simply retry as "new" again next cycle — a real, usually
        self-healing, live-scoring DELAY on exactly the heavy-news days
        when timely classification matters most, not a permanent loss.

        Chunks never span a fetch_full_article() network fetch twice —
        classify_relevance_batch() calls _fetch_and_stamp_articles() on
        each chunk itself, stamping _full_text/_text_source onto the SAME
        dict objects passed in (this method deliberately does not copy
        `articles`), so callers holding the original list still see the
        stamps land correctly regardless of chunking.

        Shared by fetch_news()'s live path and daily_relevance_revalidation()
        — both call this same underlying function with the same output
        schema, so they carry the same overflow risk and now share the
        same fix and the same chunk-size constant, instead of two
        independent choices that could silently drift apart (this was
        previously a real gap: daily_relevance_revalidation() already
        hand-rolled its own chunking loop at a different, larger size —
        same risk, just far less frequently observed there since it runs
        once a day against shorter cached-summary context, not every 5
        minutes against full article text)."""
        if not articles:
            return []
        size = chunk_size or self.HAIKU_CLASSIFY_CHUNK_SIZE
        chunks = [articles[i:i + size] for i in range(0, len(articles), size)]
        if len(chunks) > 1:
            pulse_logger.log(
                f"🔀 Haiku classify — {len(articles)} article(s) split into {len(chunks)} chunk(s) "
                f"of up to {size} (avoids the max_tokens truncation a single oversized batch can hit)"
            )
        merged = []
        offset = 0
        for idx, chunk in enumerate(chunks, start=1):
            results = self.classify_relevance_batch(chunk)
            if not results and chunk:
                pulse_logger.log(
                    f"⚠️ Haiku classify — chunk {idx}/{len(chunks)} ({len(chunk)} article(s)) returned "
                    f"nothing, those article(s) stay unclassified this cycle",
                    level="WARNING"
                )
            for r in results:
                if isinstance(r, dict) and 'id' in r:
                    r = dict(r)
                    r['id'] = r['id'] + offset
                merged.append(r)
            offset += len(chunk)
        return merged

    # ── Two-pass classification (Stage 3 of the structural first_print/
    # follow_up rewrite) — NOT YET WIRED INTO fetch_news(). Standalone,
    # tested only with mocked Haiku responses (see the scope note in the
    # accompanying handoff — extraction quality against real article text
    # cannot be verified without live Haiku access). classify_relevance_batch()
    # above and force_reclassify() are UNCHANGED and remain the live
    # production path until this is deliberately cut over. ─────────────────

    def _build_pass_a_prompt(self, article_list):
        """Pass A — EXTRACTION ONLY, no kind, no tier. Coarse routing:
        `bucket` (geo|macro|ec|drop) determines whether Pass B runs at
        all. Pass A is NOT trying to replicate every nuance of the old
        monolithic relevance gate set — it only needs to be roughly
        right, since bucket="geo" items still pass through Pass B's own
        full relevance judgment (DECISION 1, all six filters, STANDARD
        EXCLUSIONS, DEAL GATE, gates) as a second, more careful check. A
        false positive here (bucket="geo" for something that should be
        dropped) costs one extra Pass B call and gets caught there. A
        false negative (bucket="drop" for something that should be geo)
        is the real risk this prompt has to manage — mitigated by erring
        toward inclusion when uncertain about TOPIC; whether it actually
        SCORES is decided by the event-store lookup and the new_fact
        field, not by this bucket call."""
        return f"""You are the first-pass triage step for a Nasdaq-100/S&P 500 futures pre-market macro dashboard. Your ONLY job is extraction and coarse routing — you do NOT decide tier, direction, or whether an event is a repeat of something already scored elsewhere. A separate second pass handles significance judgment, only for items you route to bucket "geo".

For each article, extract:
- actor: the primary country, institution, or person who performed the action — not a spokesperson, analyst, or critic commenting on someone else's action, the one who actually DID something.
- action: pick exactly ONE of these categories, never freeform verb text: {', '.join(ACTION_CATEGORIES)}.
- object: the specific target or subject of the action (a place, an agreement, a policy, a company, a weapons system), a few words.
- place: the country, region, or specific location the action occurred in or most directly concerns.
- event_time: the date (YYYY-MM-DD) the ACTUAL EVENT happened — not the article's publish date if they differ. An article published today about a press conference three days ago should give the press conference's date, not today's.
- new_fact: in ONE sentence, the specific new fact this article adds beyond what was already known before this article existed. Leave this as empty string "" if the article does not report anything new — a recap, a synthesis of several already-known conditions into one narrative, or a quote/statistic tied to a specific earlier occasion are NOT new facts even when presented in the present tense ("X says," "prices are rising"). Describe specifically what is NEW, or say nothing at all — do not describe what the article is generally about.
- bucket: exactly one of:
    "geo" — a geopolitical event, Fed/central-bank action or commentary, government/regulatory action, major tech/AI infrastructure deal or capex commitment, mega-cap regulatory/legal outcome, energy/trade/sanctions action, or any other development that could move NQ/ES risk appetite through something real, specific, and actionable.
    "macro" — broad market-tape commentary, price-level/yield/index-move description, or "how markets reacted" framing with no new discrete event of its own (see the PRICE-MOVE PATTERN below).
    "ec" — the article is substantively ABOUT a regularly scheduled, calendar-tracked economic release or Fed decision (jobs report, CPI, PMI, FOMC meeting/press conference/statement) rather than an independent geopolitical development — this dashboard's Economic Calendar pillar already tracks these separately.
    "drop" — not market-relevant at all: lifestyle, personal finance, celebrity/investor commentary, prediction-market odds, consumer shopping content, single-company HR/operational news, vibes/sentiment pieces with no specific actionable event, or a company merely reacting to (not causing) a macro event.

PRICE-MOVE / MARKET-TAPE PATTERN — route to "macro", not "geo": an article primarily describing yields, VIX, oil settle prices, or other index/price levels moving, or framed as "live updates:" / "markets today:" tape coverage, with no NEW discrete event of its own stated as happening today — even if it mentions geopolitical causes in passing. If the article's real subject is how the tape moved rather than what specific new thing happened, it's macro, not geo, regardless of which words appear in it.

DEAL/CAPEX SIZE GATE — applies only to M&A/partnership/capex articles about Nvidia, Apple, Microsoft, Alphabet/Google, Amazon, Meta, Broadcom, AMD, Intel, TSM, or a comparable major AI-infrastructure player: route to "drop" unless the dollar figure is confirmed by an actual press release, SEC filing, or earnings call/investor update (not merely anonymous-sourced reporting) AND either (a) the buyer is Nvidia/Microsoft/Alphabet/Amazon/Meta/Broadcom with a confirmed value of $20B or more, or (b) the transaction is a compute/foundry/networking/AI-energy/data-center deal of $50B or more regardless of buyer. A confirmed deal below these lines still routes to "drop" — it's a stock story, not a regime move.

STANDARD EXCLUSIONS — always "drop" regardless of company size or how prominently "AI" appears: routine product launches/feature rollouts, sub-$1B customer wins, minor earnings beats/misses, normal single-company operational noise (hiring, office moves, executive changes, minor guidance tweaks). Describing what a product now DOES, rather than a transaction, a capex commitment, or a legal/regulatory outcome, is "drop".

Articles to classify:
{article_list}

Return ONLY a JSON array, no markdown, no explanation. Exactly this format:
[{{"id": 1, "actor": "Russia", "action": "military_action", "object": "residential building in Kyiv", "place": "Ukraine", "event_time": "2026-09-20", "new_fact": "Russia launched an overnight drone strike on a residential building in Kyiv, killing at least 4 people.", "bucket": "geo"}}, {{"id": 2, "actor": "", "action": "market_commentary", "object": "10-year Treasury yield", "place": "United States", "event_time": "", "new_fact": "", "bucket": "macro"}}]

Leave actor/place/event_time as empty string "" if genuinely not determinable from the article — never guess or default to today's date."""

    def _build_pass_b_prompt(self, article_list):
        """Pass B — tier/direction/confidence. Runs ONLY for items code
        has already resolved to kind=first_print AND bucket=geo (see
        classify_relevance_batch_v2()). This is the existing, battle-
        tested _build_classification_prompt() content with the
        FIRST_PRINT/FOLLOW_UP section removed (kind is pre-resolved by
        the time this runs — Pass B never re-derives it) and every
        dangling reference to "kind"/"FOLLOW_UP" adjusted so the prompt
        doesn't presuppose a section that no longer exists here.

        KNOWN, DELIBERATE TRADEOFF, stated plainly rather than hidden:
        the shared substance (gates, tiers, direction chains, examples)
        is DUPLICATED from _build_classification_prompt() rather than
        factored into one shared helper both functions call. This is the
        opposite of this project's usual practice (see that method's own
        docstring on why it was factored out for force_reclassify() to
        avoid exactly this kind of drift) — done here only because
        _build_classification_prompt() is still the live production
        prompt (used by classify_relevance_batch() and force_reclassify())
        and untouched-by-this-change was judged the lower-risk option
        for a change this size, with zero live Haiku access to verify a
        cleverer parameterization didn't subtly break something. If/when
        this two-pass path replaces the single-pass one in production
        (Stage 5+), collapsing this duplication should happen then, not
        guessed at now."""
        return f"""You are assisting a professional NQ and ES futures day trader with pre-market preparation. This article has already been confirmed to introduce a specific new fact not previously scored — your job is to judge its market significance, not to re-decide whether it's new.

FALSE POSITIVES COST MORE THAN MISSES: you are classifying headlines for a Nasdaq-100/S&P 500 futures regime filter used by a low-frequency discretionary-exit system. False positives keep the operator flat or push a fake lean. When unsure whether an item reprices NQ or ES risk appetite, drop it. Do not "be complete."

Reject an item entirely (relevant: false, no tier) only if it fails one of the filters below on its own terms (source-vs-echo, actor test, market domain, standard exclusions, deal gate, etc.) — do not reject an item merely for being a restatement or recap; that determination has already been made upstream of this pass.

M&A/PARTNERSHIP/DEAL ITEMS: apply the DEAL GATE inside the TECH/AI MEGA-DEAL RULES section below FIRST, before anything else in this section. If an item fails that gate, set relevant: false and do not assign a tier.

KNOWN ARTICLE OVERRIDES — if an article matches one of these titles exactly, use the specified tier, direction, and reasoning. Do not apply your normal tiering logic to these articles:
- "U.S.-Iran negotiations postponed as Netanyahu blasts Hezbollah over apparent attacks" → Tier 1, bearish, reasoning: "Collapse of U.S.-Iran negotiations with simultaneous military escalation — direct threat to regional stability and oil supply."
- "U.S. Navy ends blockade of Iran's ports and coastal areas" → Tier 2, bullish, reasoning: "Naval de-escalation removes energy supply disruption risk — positive for risk sentiment."

TECH / AI MEGA-DEAL RULES — applies to any article centered on one of these companies: Nvidia, Apple, Microsoft, Alphabet/Google, Amazon, Meta, Broadcom, AMD, Intel, Taiwan Semiconductor (TSM), or a comparable major AI-infrastructure player (CoreWeave-scale or larger).

STANDARD EXCLUSIONS — CHECK THIS FIRST, BEFORE THE DEAL GATE BELOW OR ANYTHING ELSE IN THIS SECTION. Always reject (relevant: false, no tier), regardless of company size and regardless of how prominently "AI" appears in the headline: routine product launches, feature announcements, or beta/preview rollouts (a new Siri/Assistant/Copilot feature, a redesigned app or interface, a new device going on sale, an OS update); sub-$1B customer wins; minor earnings beats/misses; and normal single-company operational noise (hiring, office moves, executive changes, minor guidance tweaks). Mentioning "AI" does not exempt a story from this exclusion — only an actual M&A/partnership/capex transaction, or a regulatory/legal outcome, can clear this section at all. If the article describes what a company's product now DOES rather than a transaction, a capex commitment, or a legal/regulatory outcome, it fails here — stop, do not proceed to the DEAL GATE below.

Example — REJECT under this exclusion: "Apple releases test of redesigned Siri AI before iPhone 18 hits stores this week." A product feature rollout ahead of a device launch — no acquisition, no capex figure, no regulatory action. relevant: false, despite naming a priority company and mentioning "AI."

DEAL GATE (replaces the old $2B floor for M&A/partnership/minority-stake items only — a hyperscaler's own capex/guidance print from an earnings call or investor update is a different category, still governed by the locked capex rule elsewhere, and bypasses this gate entirely):

OUT — relevant: false, no tier: any M&A/partnership/minority-stake commitment under $20B (unless it's a hyperscaler capex/guidance print, which doesn't use this gate at all).

LIVE (goes on to normal scoring below) requires BOTH a size test AND a confirmation test — a deal that only clears the size threshold via an unconfirmed report (anonymous sources, "people familiar with the matter," analyst speculation, or a single outlet's own reporting with no primary-source citation) does NOT clear this gate, no matter how specific or credible the dollar figure sounds. The dollar figure itself must be confirmed by an actual press release, SEC filing (e.g. an 8-K), or the company's own earnings call/investor update — not merely reported by a news outlet citing unnamed sources. A credible report that clearly identifies a specific pending deal and figure, sourced only to anonymous/unofficial channels with no company or regulatory confirmation yet, does not clear this gate — treat it as an unconfirmed rumor and reject it under this gate, UNLESS the article is merely restating the terms of a deal that is independently already confirmed elsewhere (by an actual press release, SEC filing, or earnings call/investor update), in which case this clause does not apply: score it normally against the already-confirmed size and terms, regardless of this particular article's own weaker sourcing.

With that confirmation requirement satisfied, LIVE if EITHER:
(a) the buyer is Nvidia, Microsoft, Alphabet/Google, Amazon, Meta, or Broadcom AND the confirmed disclosed value is $20B or more, OR
(b) the transaction is a compute/foundry/networking/AI-energy/data-center deal of $50B or more, regardless of buyer.

AMD, TSM, Intel, and Apple do NOT get the automatic $20B line — only the $50B-any-buyer line applies to them, unless the deal changes export rules, foundry capacity, or the legal stack in a way statable as an index-level effect in one sentence.

Calibration, not exact-match overrides — reason from the rule, not these specific numbers: a ~$13B software/AI-startup purchase by a single mega-cap buyer is OUT (below $20B, a stock story, not a regime move). A ~$32B or ~$20B confirmed acquisition by one of the six named buyers is LIVE. A ~$40B infrastructure consortium deal is OUT under both tests (no single buyer clears $20B, and $40B misses the $50B infrastructure line). An ~$80B confirmed infrastructure/chip-producer takeout is LIVE under the $50B-any-buyer line regardless of buyer.

SOURCE PRIORITY: Prefer information from a press release or SEC filing first, an earnings call or investor update second, and Tier-1 financial media (Reuters, Bloomberg, WSJ, CNBC breaking coverage) third. Discount unconfirmed reports, analyst speculation, or secondary outlets restating another outlet's story.

TIER FOR DEALS THAT CLEAR THE GATE ABOVE (use in place of the geopolitical tier definitions in DECISION 5 for this category; a deal that fails the gate is never tiered at all — Tier 3 is not a landing spot for a gate failure. A capex beat keeps its own separate tier treatment below, not this section):
Tier 1: an immediate, clear index-level catalyst — a finalized, signed transformative takeout with a stated close path, or a comparable unambiguous done-deal.
Tier 2: material but still contingent — announced but not yet closed, a regulator still ahead, or "getting close" language from a primary actor plus a confirming third party.
Tier 3: the deal itself is confirmed (buyer, target, and size disclosed via a real press release/8-K/earnings call — the gate's confirmation requirement is already satisfied), but the article is otherwise thin — single-outlet coverage of that disclosure with no additional corroboration yet, or the disclosure itself is a brief/preliminary announcement without full deal terms. Tier 3 always means "confirmed but under-specified," never "unconfirmed."
Capex beat (separate from the deal gate — evaluated on its own, not against the $20B/$50B thresholds): Tier 1 for a capex beat >20% over prior guidance paired with strong demand/backlog framing; Tier 2 for a strategic government/industrial partnership or multi-year build-out without near-term capex acceleration.

DIRECTION FOR TECH/AI MEGA-DEALS — DELIBERATELY DIFFERENT FROM THE GEOPOLITICAL CHAINS BELOW: Do not default to a confident bullish or bearish call for this category. Even a large, clearly-covered mega-deal can coincide with a same-day stock move driven by unrelated macro conditions — a confident directional call here risks being wrong for reasons that have nothing to do with the deal's actual merits (real example: the Apple-Broadcom $30B chip deal, August 2026). Default to "neutral" and use the summary/reasoning fields to surface the event, its size, and its terms factually. Only lean bullish or bearish when the article itself contains genuinely one-sided evidence:
- Lean BEARISH only if the article contains explicit margin-pressure language or explicit no-ROI/return-timeline-risk language from the company or credible analysts.
- Lean BULLISH only if there is a capex beat >20% over prior guidance AND an explicit strong-demand, backlog, or monetization link stated in the article (not inferred).
- Otherwise (the item already cleared STANDARD EXCLUSIONS and the DEAL GATE above, but the article itself contains no one-sided bullish/bearish evidence): direction = "neutral", relevant = true, and the summary should surface the deal size/terms/actors so a trader can weigh it themselves.

MEGA-CAP REGULATORY/LEGAL OUTCOME RULES — applies to any article reporting a settlement, fine, verdict, judgment, or forced product/business change resulting from a regulatory or legal proceeding against one or more priority mega-cap companies (Nvidia, Apple, Microsoft, Alphabet/Google, Amazon, Meta, Broadcom, AMD, Intel, Taiwan Semiconductor (TSM), or a comparable index-weight tech/AI-infrastructure company).

TWO SEPARATE, ALWAYS-INDEPENDENT ASSESSMENTS:
1. COMPANY-LEVEL IMPACT — negative, neutral, or positive for the company itself.
2. NQ/TECH-COMPLEX IMPACT — negative, neutral, or positive for broader risk appetite today.
These can disagree — a clearly negative outcome for one company can be a non-event for the index, and vice versa. Only the NQ-level assessment sets this article's direction and tier. The company-level assessment is reported in the reason field but never overrides the NQ-level read.

TIER IS DRIVEN BY MAGNITUDE OF ACTUAL NQ-LEVEL IMPACT, NOT SURFACE PATTERN-MATCHING. Do not tier by simply counting how many companies are involved — count is one input, not the rule. Weigh all of the following together for the specific headline in front of you; none of them is individually decisive:
- Scale of the number relative to the company/companies involved — a fine that's trivial relative to one company's cash flow is not the same magnitude as one that's trivial for a smaller company but material for a larger one, or vice versa. Judge relative to what the entity(ies) involved can actually absorb, not the raw dollar figure alone.
- How many priority mega-caps are affected, and whether the exposure is genuinely shared or coincidental — two unrelated companies fined for two unrelated things on the same day is different from two companies fined for the same underlying practice, which signals a sector-wide pattern regulators are now pursuing broadly.
- Certainty of the number — capped/known/payable-over-time vs. open-ended/uncapped/appealable.
- Whether the market had already priced in a worse outcome — genuine relief vs. genuine surprise.
- Forward-looking business impact — does the remedy change how the company/sector operates going forward (product redesign, engagement limits, structural changes), or does it just resolve past liability with no operational change?
- Confirmed market reaction, if available — did the stock/sector actually move on the print, or is that unknown/pending?
- Whether this looks like an isolated event or part of an emerging pattern — one lawsuit is different from what looks like the third in a developing wave of similar actions against the same sector.

TIER CLASSIFICATION FOR MEGA-CAP REGULATORY/LEGAL OUTCOMES (use in place of the geopolitical tier definitions in DECISION 5 for this category), defined by the combined weight of the factors above, not any single factor alone:
Tier 3: The weighted factors suggest limited, contained NQ-level impact — no matter whether that's one company or several, and no matter the raw dollar size, if the market can plausibly absorb it without a genuine repricing of sector risk.
Tier 2: The weighted factors suggest a real, if not extreme, shift in how the market prices sector-wide legal/regulatory risk — this can be triggered by one company (an unexpected verdict) or several (a coordinated pattern), driven by the substance of the factors above, not a headcount threshold.
Tier 1 (rare): Structural/existential impact to a core NQ business model, or a same-day event significant enough to reprice the sector's risk premium broadly.

DIRECTION FOR MEGA-CAP REGULATORY/LEGAL OUTCOMES IS NOT A LOOKUP TABLE. Do not pre-map specific factor combinations to specific directions. Weigh the factors for the specific headline and reach whatever direction genuinely fits — bearish, neutral, or bullish for NQ specifically (separate from whatever the company-level read is). A "negative for the company" outcome does not automatically mean bearish for NQ, and a "positive for the company" outcome does not automatically mean bullish for NQ — judge the NQ-level factors on their own terms.

REASON FIELD FORMAT FOR THIS CATEGORY: write the reason field as three lines — company-level assessment (one line, exactly one of negative/neutral/positive with a brief cause), NQ-level assessment (one line, exactly one of negative/neutral/positive, naming which factors above weighed most), then tier. Format: "Company: [negative/neutral/positive] — [why]. NQ: [negative/neutral/positive] — [why, citing the deciding factors]. Tier: [1/2/3]." If the two levels disagree, the NQ-level read is what's scored.

Your job is to read each full article, then make five decisions:

DECISION 1 — RELEVANCE
Is this genuinely new, market-moving information that would cause a futures trader to reconsider their directional bias for today's session?

Think like a trader sitting down at 8AM asking: "Does this change anything about how I trade today?"

Pass if it involves: Federal Reserve policy or official commentary, geopolitical escalation or resolution affecting global risk sentiment, major economic data surprises, energy market shocks, trade policy changes with immediate impact, systemic financial risk, or significant government actions with direct market consequences, or major tech/AI infrastructure deals/capex commitments meeting the TECH/AI MEGA-DEAL RULES threshold above, or mega-cap regulatory/legal outcomes meeting the MEGA-CAP REGULATORY/LEGAL OUTCOME RULES above.

Fail if it involves: opinion or commentary on past market moves, investment advice or tips, personal finance stories, single company news unless systemically important, celebrity investor quotes, lifestyle or consumer behavior stories, retail shopping guides or consumer deal/discount roundups (e.g. "back to school savings," "extra deals," holiday shopping tips, or similar listicle-style consumer spending content — even if framed around tariffs or prices), prediction-market or betting-market odds and probability content (e.g. Kalshi, Polymarket, or PredictIt contract prices or probability shifts on a geopolitical or economic outcome) — a market's aggregated probability estimate is not itself a new event, even when the underlying outcome concerns something market-moving like a nuclear deal, election, or rate decision, newsletter recap formats, or anything that describes what already happened rather than new information and has no traceable connection to anything genuinely market-moving.

Before passing any article, run it through these six filters. If it fails any one of them, reject it:

FILTER 1 — SOURCE VS ECHO
Is this the event itself or a reaction to an event that already happened? A SOURCE event is new information the market hasn't priced in yet. It originates from a primary actor — a government, central bank, military, or natural force. An ECHO event is any person, company, or institution RESPONDING to or REPORTING ON a known macro situation.

Critical rule: If a company name appears as the subject of the headline and the headline describes them REACTING to a macro event (adding fees, raising prices, cutting jobs, warning of impacts, adjusting operations) — it is ALWAYS an echo. Reject it.

Exception: a company announcing, signing, or confirming its own new deal, partnership, or capex commitment is the SOURCE of that event, not an echo — they are the primary actor creating new information, not reacting to someone else's action. Reserve "echo" for a company responding to an external event (tariffs, energy prices, a war, a competitor's move, etc.).

A regulatory settlement, fine, verdict, or court judgment against a company is also a SOURCE event, not an echo — the regulator or court is the primary actor delivering new information, even though the company is the named subject.

The presence of macro keywords like "war", "energy", "Iran", "tariff" in a headline does NOT make it a source event. Ask: who is the ACTOR and what ACTION did they take? If the actor is a corporation reacting to an existing situation — it's an echo regardless of the macro language surrounding it.

FILTER 2 — RECENCY TEST
Is this reporting something happening now and still live and current, or something genuinely stale — a week-in-review piece, an "after X weeks of..." article, or historical context with no live connection? This article has already been confirmed upstream to introduce a specific new fact — reject it here only if, on full reading, that claimed new fact turns out to be purely historical or retrospective framing rather than something live and current today.

FILTER 3 — SPECIFICITY TEST
Is this about a specific actionable event or a general mood/sentiment piece? Vibe articles, market psychology pieces, and "how to navigate" content are not tradeable information.

FILTER 4 — ACTOR TEST
Is the person or organization in this headline someone who directly moves markets through their decisions? Federal Reserve officials, heads of state, treasury secretaries, central bank chiefs, and major geopolitical actors = yes. State governors, backbench senators, corporate executives reacting to macro events, NASA, local officials = no, unless their specific action is systemically important to financial markets.

FILTER 5 — MARKET DOMAIN TEST
Does this article exist within the domain of financial markets, geopolitics affecting markets, energy, trade, or monetary policy? Articles about space missions, scientific discoveries, social policy, and non-financial government activity should be rejected even if they use financial language.

FILTER 6 — CONFIRMATION TRAP TEST
Is this article just confirming something the market already knows and has already priced in? This article has already been confirmed upstream to introduce a specific new fact — reject it here only if, on full reading, that claimed new fact adds no real new directional information on its own and cannot be tied to anything specific, live, and actionable beyond the already-known context. A genuine new development does not fail this filter merely because it occurs within an already-known larger situation.

POLITICAL PRESSURE ON THE FED — GATE (a special outcome, NOT a DECISION 1 rejection): applies to any article centered on a President, administration official, or member of Congress pressuring, criticizing, or being described as on a "collision course" with the Federal Reserve, its chair, or FOMC members over policy.

An item needs a realistic path to moving NQ/ES risk BY ITSELF to score. Pressure and rhetoric alone are not that path — they stay on the Economic Calendar pillar's own speech card (Neutral unless they change the actual policy path), not here, unless a genuinely new concrete action is confirmed in this article.

FAILS THE GATE (gate_pass: false — still relevant: true, but direction must be "neutral" and tier must be omitted): renewed or continued pressure, "collision course"/"boxed in" framing, criticism of Fed independence, calls for rate cuts or a chair's removal, speculation about what the Fed chair might do — with NO new concrete action in THIS article. A new interview, a new op-ed, or a new round of the same rhetoric does not clear this on its own.

CLEARS THE GATE (gate_pass: true, or omit the field — proceed to normal scoring below): a concrete institutional action has actually occurred and is confirmed in this article — the Fed chair or a governor is fired or resigns, a replacement is confirmed, legislation is introduced or passed that changes the Fed's structure or mandate, or a formal White House directive or executive order is issued to the FOMC.

ANTI-RECURRENCE CHECK: do not score both "if the chair capitulates" and "if the market reads it as politicization" as bearish outcomes for the same article — that is not identifying a catalyst, it is asserting the story matters no matter what happens. If your own reasoning would call every possible outcome of a pressure story bearish, that itself is the signal that this fails the gate.

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
[{{"id": 1, "relevant": true, "confidence": 0.95, "category": "geopolitical", "direction": "bearish", "reason": "Iran war escalation directly affects oil and risk sentiment", "summary": "Your 3-4 sentence market summary here.", "uncertainty_score": 85, "tier": 1, "reasoning": "Active war escalation directly threatens oil supply and broad risk sentiment."}}, {{"id": 2, "relevant": true, "confidence": 0.8, "category": "geopolitical", "direction": "neutral", "reason": "Renewed pressure on the Fed chair with no new concrete action — fails the political pressure gate.", "summary": "Your 3-4 sentence market summary here.", "uncertainty_score": 60, "gate_pass": false, "reasoning": "Continued rhetoric, no firing/resignation/legislation/directive — not a catalyst by itself."}}]

Use only "bearish", "bullish", or "neutral" for direction.
Use confidence between 0.0 and 1.0.
Use tier as an integer: 1, 2, or 3. Omit tier entirely when gate_pass is false.
If relevant is false, still provide a summary field but it can be empty string.
Use gate_pass: false ONLY for an item that fails the POLITICAL PRESSURE ON THE FED — GATE above — it stays relevant: true, but direction must be "neutral" and tier must be omitted, so it contributes zero score. Omit gate_pass entirely (or use true) for every other item — this field exists solely to mark that one gate-failure case as visible-but-non-scoring rather than dropped.

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

For tech/AI mega-deal articles, use the TIER CLASSIFICATION FOR TECH/AI MEGA-DEALS section above instead of the definitions below.
For mega-cap regulatory/legal outcome articles, use the TIER CLASSIFICATION FOR MEGA-CAP REGULATORY/LEGAL OUTCOMES section above instead of the definitions below.

Tier 1 (±1.7): Active war or escalation between major powers, nuclear threats/incidents, major confirmed peace deals or ceasefires that meaningfully reduce geopolitical risk, or credible major supply disruptions (e.g. Hormuz closure threat).
Tier 2 (±0.75): Significant troop buildups, major diplomatic breakdowns, new meaningful sanctions, or credible energy market threats that are not Tier 1 level.
Tier 3 (±0.35): Minor diplomatic noise, corporate geopolitical news, speculative or secondary headlines with limited immediate market relevance.

Key rules:
- Prioritize actual market impact and context over headline keywords. The presence of words like "ceasefire" or "deal" does not automatically make something Tier 1 — evaluate whether a real, credible development occurred.
- When uncertain between tiers, default to the lower tier.
- De-escalation and peace developments are generally Bullish for US equities. Escalation and conflict are generally Bearish.
- Oil/Energy Rule: Falling oil prices caused by geopolitical de-escalation or peace deals are Bullish for equities. Only classify oil price moves as Bearish when driven by demand destruction, recession fears, or oversupply.
- Major Economic Data Exception — NFP / CPI / GDP only: If the article reports actual data for Non-Farm Payrolls (NFP), CPI (any variant: Core CPI, CPI m/m, CPI y/y), or GDP (any variant: GDP q/q, Final GDP), AND the deviation of actual from consensus forecast is 50% or greater in absolute relative terms (e.g. NFP 57K actual vs 114K expected = 50% miss; CPI 0.6% actual vs 0.3% expected = 100% beat), classify as Tier 1 regardless of other factors. Cite the specific actual vs. forecast figures and the % deviation as the reasoning. This exception does NOT apply to ISM, PMI, Retail Sales, ADP, or any other economic data — those continue to use standard Tier 2/3 judgment.

Articles to classify:
{article_list}"""

    def _code_side_bucket_override(self, article, extraction):
        """Stage 4 of the structural rewrite: price-move / pure market-tape
        analysis never enters Geo, decided MECHANICALLY here, never left
        to Pass A's own bucket judgment call. Checks the article's title/
        description/summary for TAPE_PATTERN_PHRASES (yields, VIX, oil
        settle, "live updates" framing); if matched AND the extraction
        does not state a genuinely new event dated TODAY, forces bucket
        to "macro" regardless of what Pass A said. This is what kills the
        CNBC-yields-style case as a PATTERN — any article matching this
        shape, not a one-off headline special case.

        The "no new event dated today" qualifier is the whole point: an
        article that happens to mention yields/VIX/oil in passing while
        reporting a genuinely new same-day event is NOT overridden — only
        pure tape narration with nothing new stated for today is."""
        text = f"{article.get('headline', '')} {article.get('description', '')} {article.get('summary', '')}".lower()
        if not any(self._keyword_matches(text, p) for p in self.TAPE_PATTERN_PHRASES):
            return extraction.get('bucket', 'drop')

        today_str = datetime.now(self.timezone).strftime('%Y-%m-%d')
        event_time = (extraction.get('event_time') or '').strip()[:10]
        if event_time == today_str:
            return extraction.get('bucket', 'drop')

        original_bucket = extraction.get('bucket', 'drop')
        if original_bucket != 'macro':
            pulse_logger.log(
                f"🔧 Code-side bucket override — {article.get('headline', '')[:60]!r} matched a "
                f"market-tape pattern with no new event dated today, forcing bucket macro (was {original_bucket!r})"
            )
        return 'macro'

    def classify_relevance_batch_v2(self, articles):
        """Orchestration for the two-pass structural rewrite. Standalone —
        NOT called by fetch_news() yet (see the module-level comment
        above). For each article: Pass A extracts identity fields and a
        coarse bucket; code computes the canonicalized event_id and looks
        it up in the event store; a store HIT (or a failed/incomplete
        extraction) resolves to kind=follow_up, score 0, no Pass B call
        at all; a genuine MISS with bucket="geo" claims the event_id as
        first_print and runs Pass B for tier/direction/confidence, then
        fills in score_applied. bucket != "geo" (macro/ec/drop) never
        reaches Pass B regardless of store outcome — this is what cuts
        Haiku calls/cost for every recap and off-topic item per the spec.

        Returns a list of dicts, one per input article, each already
        carrying kind/bucket/event_id and — for first_print+geo items —
        the full Pass B classification fields merged in.
        """
        if not articles:
            return []
        if self.anthropic_client is None:
            pulse_logger.log("⚠️ Haiku unavailable — keyword-only scoring in effect for unclassified articles", level="WARNING")
            return []

        self._fetch_and_stamp_articles(articles)

        pass_a_list = ""
        for i, article in enumerate(articles):
            pass_a_list += f"{i+1}. TITLE: {article['headline']}\n   FULL TEXT: {article['_full_text']}\n\n"
        pass_a_prompt = self._build_pass_a_prompt(pass_a_list)
        pass_a_results = self._call_haiku_classify(pass_a_prompt, article_count=len(articles))
        pass_a_by_id = {r.get('id'): r for r in pass_a_results if isinstance(r, dict)}

        results = []
        pass_b_indices = []       # indices into `results` needing a Pass B call
        pass_b_article_list = ""
        pass_b_n = 0

        for i, article in enumerate(articles):
            extraction = pass_a_by_id.get(i + 1)

            # RULE 3 — DEFAULT TO FOLLOW_UP, FIRST PRINT IS EARNED: a
            # missing/failed extraction never defaults to first_print.
            if not extraction:
                results.append({
                    'headline': article.get('headline', ''),
                    'kind': 'follow_up', 'bucket': 'drop', 'event_id': None,
                    'relevant': False, 'reason': 'Pass A extraction failed or missing for this item',
                })
                continue

            actor = extraction.get('actor', '')
            action = extraction.get('action', '')
            obj = extraction.get('object', '')
            place = extraction.get('place', '')
            event_time = extraction.get('event_time', '')
            new_fact = (extraction.get('new_fact') or '').strip()
            bucket = self._code_side_bucket_override(article, extraction)

            base = {
                'headline': article.get('headline', ''), 'actor': actor, 'action': action,
                'object': obj, 'place': place, 'event_time': event_time, 'bucket': bucket,
            }

            if not new_fact:
                # No new fact asserted — default to follow_up regardless of
                # bucket or store state. Still compute event_id where
                # possible so a later genuine new_fact about this same
                # event has something to have matched against, but this
                # item itself never claims first_print.
                base.update({'kind': 'follow_up', 'event_id': compute_event_id(actor, action, place, event_time), 'relevant': False})
                results.append(base)
                continue

            if bucket != 'geo':
                # macro/ec/drop never reaches Pass B or the event store,
                # regardless of new_fact — this is the cost-saving cut.
                base.update({'kind': 'follow_up', 'event_id': None, 'relevant': False})
                results.append(base)
                continue

            event_id = compute_event_id(actor, action, place, event_time)
            if event_id is None:
                # Missing actor/place/event_time despite a stated new_fact
                # — cannot establish identity, fails closed to follow_up
                # per rule 3 rather than risking a loose/no-op hash.
                base.update({'kind': 'follow_up', 'event_id': None, 'relevant': False,
                             'reason': 'Incomplete extraction (actor/place/event_time) — cannot establish event identity'})
                results.append(base)
                continue

            existing = event_store.lookup(event_id)
            if existing is not None:
                base.update({'kind': 'follow_up', 'event_id': event_id, 'relevant': False,
                             'reason': f'Matches already-scored event {event_id} (first seen {existing.get("first_seen", "")})'})
                results.append(base)
                continue

            # Genuine miss, bucket=geo, real new_fact — claim the identity
            # now (two-step write, see event_store.py) and queue for Pass B.
            event_store.record_first_print(event_id, actor, action, place, event_time)
            base.update({'kind': 'first_print', 'event_id': event_id})
            results.append(base)
            pass_b_indices.append(i)
            pass_b_n += 1
            pass_b_article_list += f"{pass_b_n}. TITLE: {article['headline']}\n   FULL TEXT: {article['_full_text']}\n\n"

        if pass_b_indices:
            pass_b_prompt = self._build_pass_b_prompt(pass_b_article_list)
            pass_b_results = self._call_haiku_classify(pass_b_prompt, article_count=len(pass_b_indices))
            pass_b_by_id = {r.get('id'): r for r in pass_b_results if isinstance(r, dict)}
            for pos, result_idx in enumerate(pass_b_indices):
                pb = pass_b_by_id.get(pos + 1)
                if not pb:
                    pulse_logger.log(
                        f"⚠️ Pass B — no result for {results[result_idx]['headline'][:60]!r} "
                        f"(event_id {results[result_idx]['event_id']}) — left unscored this cycle",
                        level="WARNING"
                    )
                    continue
                results[result_idx].update(pb)
                # Route Pass B's raw Haiku-schema output through the SAME
                # translation the legacy v1 path uses (CLASSIFICATION_TO_ITEM_FIELDS
                # via _apply_classification_fields()) so calculate_score() actually
                # sees it under the item-schema names it reads (haiku_tier,
                # gemini_direction, haiku_gate_pass, haiku_confidence,
                # haiku_tier_reasoning) — not just Pass B's raw key names
                # ('tier', 'direction', 'gate_pass', 'confidence') sitting inert
                # next to them. Without this, a future cutover would silently
                # defeat gate_pass entirely (calculate_score() checks
                # item.get('haiku_gate_pass') is False, which would never be
                # true if that key were simply absent) and every item would
                # fall through to the sentiment/keyword tier fallback instead
                # of using Pass B's real tier. The raw pb.update() above is
                # kept too — _run_shadow_classification()'s own comparison
                # logging and this project's existing tests both read the raw
                # key names directly, so both representations coexist on the
                # same result dict rather than one replacing the other.
                # _apply_classification_fields() expects 'tier_reasoning', not
                # Pass B's raw 'reasoning' key — renamed here, the one place
                # the two schemas actually differ in field name rather than
                # presence, before handing off to the real shared translator.
                translation_source = dict(pb)
                translation_source['tier_reasoning'] = pb.get('reasoning', '')
                self._apply_classification_fields(results[result_idx], translation_source)
                # score_applied is bookkeeping for the store, not a re-derivation
                # of calculate_score()'s tier-magnitude math — store the tier
                # actually assigned (None if rejected or gated to neutral),
                # so a later duplicate's log line can say what tier this event
                # was scored at without duplicating the tier->magnitude table.
                tier_applied = pb.get('tier') if pb.get('relevant') else None
                event_store.update_score_applied(results[result_idx]['event_id'], tier_applied)

        return results

    def _run_shadow_classification(self, new_items, real_classifications):
        """Shadow/dry-run mode for validating Stage 3 (classify_relevance_batch_v2)
        against real incoming articles before it is ever allowed to
        replace the live scoring path. Comparison-logging ONLY.

        ISOLATION GUARANTEES, and how each is actually true (not just
        asserted):
          - Operates on copy.deepcopy(new_items), never the real list —
            classify_relevance_batch_v2() calls _fetch_and_stamp_articles()
            internally, which mutates article dicts in place
            (_full_text/_text_source); running it on the real objects
            that background_classify() and update_pinned_store() go on to
            read from would risk stamping them a second time. The deep
            copy makes this impossible regardless of call order.
          - Its return value (shadow_results) is only ever read by the
            comparison/logging loop below — never passed to cache.save(),
            update_pinned_store(), or any dict this method's own caller
            (background_classify()) uses afterward. Grep
            'event_store\\.|event_canon' in this repo to confirm nothing
            outside classify_relevance_batch_v2()/this method references
            the Stage 1-3 modules at all.
          - Wrapped in its own try/except distinct from background_classify()'s
            outer except — an exception here is logged under its own
            "Shadow mode" line and returns, rather than surfacing as a
            misleading "Background Haiku failed" (which would wrongly
            implicate the REAL classification that, by construction, has
            already completed and been written by the time this runs).
        """
        try:
            shadow_copy = copy.deepcopy(new_items)
            shadow_results = self.classify_relevance_batch_v2(shadow_copy)
        except Exception as e:
            pulse_logger.log(f"⚠️ Shadow mode (Stage 3) — v2 classification failed, real scoring unaffected: {e}", level="WARNING")
            return

        real_by_headline = {}
        for r in (real_classifications or []):
            idx = r.get('id', 0) - 1
            if 0 <= idx < len(new_items):
                real_by_headline[new_items[idx]['headline']] = r

        agree = disagree = no_comparison = 0
        for shadow in shadow_results:
            headline = shadow.get('headline', '')
            real = real_by_headline.get(headline)
            new_kind = shadow.get('kind')
            new_tier = shadow.get('tier')
            new_bucket = shadow.get('bucket')

            if real is None:
                no_comparison += 1
                pulse_logger.log(
                    f"🔬 SHADOW no-comparison — {headline[:70]!r} | real path returned no result for this "
                    f"item this cycle | new: kind={new_kind} tier={new_tier} bucket={new_bucket}"
                )
                continue

            old_kind = real.get('kind')
            if old_kind not in ('first_print', 'follow_up'):
                old_kind = 'first_print'  # mirrors background_classify()'s own malformed-kind default
            old_tier = real.get('tier')
            old_relevant = real.get('relevant')

            kinds_match = old_kind == new_kind
            tiers_match = True if old_kind != 'first_print' or new_kind != 'first_print' else old_tier == new_tier

            if kinds_match and tiers_match:
                agree += 1
                continue

            disagree += 1
            if old_kind == 'first_print' and new_kind == 'follow_up':
                direction = 'v2_deduped (old=first_print, new=follow_up) — likely a genuine dedup catch, the whole point of this rewrite'
            elif old_kind == 'follow_up' and new_kind == 'first_print':
                direction = 'v2_more_permissive (old=follow_up, new=first_print) — needs scrutiny, NOT assumed fine just because it is not the bug being chased'
            elif not tiers_match:
                direction = f'tier drift within agreeing kind={old_kind} (old_tier={old_tier}, new_tier={new_tier})'
            else:
                direction = f'other (old_kind={old_kind}, new_kind={new_kind})'

            pulse_logger.log(
                f"🔬 SHADOW DISAGREEMENT — {headline[:70]!r} | {direction} | "
                f"old: kind={old_kind} tier={old_tier} relevant={old_relevant} | "
                f"new: kind={new_kind} tier={new_tier} bucket={new_bucket} event_id={shadow.get('event_id')}"
            )

        pulse_logger.log(
            f"🔬 Shadow mode (Stage 3) — cycle summary: {agree} agree / {disagree} disagree / "
            f"{no_comparison} no-comparison out of {len(shadow_results)} items"
        )

    def _build_classification_prompt(self, article_list):
        """The full, current classification prompt, parameterized only by
        the pre-built "N. TITLE: ... FULL TEXT: ..." block. Factored out of
        classify_relevance_batch() so force_reclassify() can build the
        IDENTICAL prompt for a single stored article — a forced
        reclassification must judge against the exact same rules a live
        classify() call would use, never a hand-copied second version that
        can silently drift from this one."""
        return f"""You are assisting a professional NQ and ES futures day trader with pre-market preparation.

FALSE POSITIVES COST MORE THAN MISSES: you are classifying headlines for a Nasdaq-100/S&P 500 futures regime filter used by a low-frequency discretionary-exit system. False positives keep the operator flat or push a fake lean. When unsure whether an item reprices NQ or ES risk appetite, drop it. Do not "be complete." Completeness is how $12.9B software deals sit in the score for two days after the tape is done.

FIRST_PRINT vs FOLLOW_UP — classify every relevant item as one of these before anything else below. This determines how long the item can live in the score, not whether it's interesting enough to include.

FIRST_PRINT (48-hour clock): the article introduces a new fact — new kinetic action, new official restriction, new signed agreement, new disclosed size/terms, or first confirmation from a primary actor. A new kinetic act after silence of 5+ trading days is FIRST_PRINT even if it's the same war or the same underlying conflict.

FOLLOW_UP (24-hour clock): the article restates an already-scored event with no new target class, no new supply-chain implication, and no new fact — "talks are close" with no third-party confirmation, an analyst note repeating a press release, a senator's comment on an already-scored story, a recap of an already-announced deal, a second strike inside an already-active 48-hour campaign with no new target class. FOLLOW_UP is still relevant: true — it is ingested and scored on the shorter clock, not rejected. Do not fail a FOLLOW_UP item under DECISION 1 or FILTER 2/6 below purely because it restates rather than introduces — that's exactly what makes it FOLLOW_UP, not grounds for rejection.

CONDITION-BASED, NOT URL-BASED: you have no visibility into what this pillar has previously ingested or scored — do not classify something FIRST_PRINT merely because you personally don't recognize an earlier scored instance of it. Judge FIRST_PRINT vs FOLLOW_UP from cues WITHIN THIS ARTICLE'S OWN TEXT instead: does the article itself show it is describing an ongoing, pre-existing condition — references to "days after," "in the latest sign of," "continuing his push/pressure/campaign to," quotes attributed to remarks made earlier rather than fresh statements, or framing that assumes the reader already knows a backstory the article references but does not newly establish? If so, that is FOLLOW_UP even if you cannot identify or recall the specific earlier article — the condition being old is what matters, not whether you have a record of it. FIRST_PRINT requires the article to report a genuinely new discrete action or fact happening now, not merely a new write-up, new interview, or new analysis of a standing situation.

THIS APPLIES TO EVERY TOPIC, NOT JUST POLITICS OR THE FED. Two patterns worth naming explicitly, because they're easy to misread as FIRST_PRINT on a surface read:

  - FEATURE / SYNTHESIS ARTICLES: a "state of the market" or "how X is affecting Y" piece that synthesizes several ALREADY-ESTABLISHED conditions (an existing tariff regime, an already-reported price level, an already-completed rate move, an already-known conflict) into one narrative is FOLLOW_UP, even if this specific combination or headline framing hasn't run before. A new SYNTHESIS or angle on old facts is not itself a new fact. Only ONE of the underlying components being genuinely new — a new tariff announced today, a new price record set today, a new rate decision today — makes it FIRST_PRINT, and only on the strength of that one new component.
  - QUOTES OR DATA TIED TO A PRIOR OCCASION: a quote or statistic attributed to a specific earlier occasion (a speech, an interview, a data release earlier in the week) is a restatement even if the article doesn't explicitly flag it as old, and even if it's reported in the present tense ("X says..."). Present-tense framing of an old remark does not make it new. It is FOLLOW_UP unless the article reports a NEW occasion where that speaker or data source said or showed something not previously disclosed.

RULE OF THUMB, applies everywhere in this FIRST_PRINT/FOLLOW_UP test, not only the FOMC section below: if you cannot state the genuinely new fact — in one sentence, distinct from anything already known before this specific article — it is FOLLOW_UP.

EXAMPLE — FOLLOW_UP, not FIRST_PRINT: "Trump told Putin U.S.-Russia ties could be fully restored with a swift end to the Ukraine war, Kremlin says." This is a primary-actor readout of a call stating a desire — no signed ceasefire, no withdrawal, no treaty, no verified pause. It is the "talks are close" case above, not a new fact: kind=follow_up, 24h. Contrast FIRST_PRINT: a joint statement announcing a dated ceasefire, a signed framework, or third-party (UN/Turkey/an official ministry) confirmation that fighting has actually stopped — that is new terms, 48h.

EXAMPLE — FOLLOW_UP from self-referential cues alone, no known prior article required: an article describing a President publicly pressuring a Fed chair over rate cuts, framed as a "collision course," where the piece itself references the pressure campaign as already ongoing (prior public comments, an established boxed-in dynamic) and reports no new concrete action (no firing, no resignation, no legislation, no formal directive). This is FOLLOW_UP purely from the article's own framing of an existing condition, 24h — regardless of whether an earlier instance of this pressure campaign was ever itself scored by this pillar. (Separately, see the POLITICAL PRESSURE ON THE FED — GATE below: this exact example also fails that gate.)

EXAMPLE — FOLLOW_UP, feature/synthesis pattern: "'It's awful': How tariffs, soaring fuel costs and higher interest rates are squeezing American companies." Every component named — the tariff regime, elevated fuel prices, this week's rate move — is already established; the piece synthesizes known pressures on companies, it doesn't report one new pressure. No new tariff, no new price record, no new rate action stated: kind=follow_up, 24h. Contrast: an article reporting a newly announced tariff today, or a fuel price that just set a new all-time high today — that specific new element would be FIRST_PRINT on its own terms, even inside a similar "squeeze" narrative.

EXAMPLE — FOLLOW_UP, quote tied to a prior occasion: "Rising gas prices frustrate voters. Trump says it's an 'inexpensive price to pay' for the Iran war." If the remark was made at an earlier public appearance in the week and the gas-price figure cited is likewise from an earlier data release restated for a new news cycle, this is FOLLOW_UP even though the headline presents both in the present tense ("Trump says," "rising gas prices") — a quote and a data point both traceable to a specific prior occasion, with no new occasion or new figure reported, is not a new fact. Contrast FIRST_PRINT: a new interview or appearance today where the same remark is made for the first time, or a gas price that sets a new record today.

FOMC / RATE-DECISION RECAP RULE: if the article's only substance is language about or reaction to THIS WEEK'S FOMC meeting, press conference, Summary of Economic Projections, or rate decision — and it introduces no new fact (no new vote, no new number, no new date, no unscheduled statement establishing a genuinely new policy path) — classify it FOLLOW_UP, even on its first appearance in this pillar. Cue phrases suggesting this pattern: "this week's rate increase," "at the press conference," "Warsh's three words," "markets digest the hike," or any similar framing that reacts to or analyzes an already-completed meeting rather than reporting something new. This is the same rule of thumb as above, applied to Fed/rate-decision coverage specifically.

EXAMPLE — FOLLOW_UP, FOMC recap pattern: "Three words from Kevin Warsh have Wall Street wondering how far the Fed will go with rate hikes," published days after this week's FOMC decision and press conference, analyzing a quote made AT that press conference. No new vote, no new number, no new date — this is analysis of an event the Economic Calendar pillar already scored: kind=follow_up, 24h. Contrast a genuine FIRST_PRINT in this category: an unscheduled interview days later where Warsh states a specific new policy path not previously disclosed (e.g. "another hike in November is likely") — that is a new fact from a primary actor, FIRST_PRINT.

Reject an item entirely (relevant: false, no tier, no kind) only if it neither introduces a new fact (FIRST_PRINT) nor restates an identifiable, already-scored, still-live event (FOLLOW_UP) — i.e. it has no traceable connection to anything market-moving, or it fails one of the other filters below on its own terms (source-vs-echo, actor test, market domain, etc.).

M&A/PARTNERSHIP/DEAL ITEMS: apply the DEAL GATE inside the TECH/AI MEGA-DEAL RULES section below FIRST, before FIRST_PRINT/FOLLOW_UP or anything else. If an item fails that gate, set relevant: false and do not assign a tier or a kind.

KNOWN ARTICLE OVERRIDES — if an article matches one of these titles exactly, use the specified tier, direction, and reasoning. Do not apply your normal tiering logic to these articles:
- "U.S.-Iran negotiations postponed as Netanyahu blasts Hezbollah over apparent attacks" → Tier 1, bearish, reasoning: "Collapse of U.S.-Iran negotiations with simultaneous military escalation — direct threat to regional stability and oil supply."
- "U.S. Navy ends blockade of Iran's ports and coastal areas" → Tier 2, bullish, reasoning: "Naval de-escalation removes energy supply disruption risk — positive for risk sentiment."

TECH / AI MEGA-DEAL RULES — applies to any article centered on one of these companies: Nvidia, Apple, Microsoft, Alphabet/Google, Amazon, Meta, Broadcom, AMD, Intel, Taiwan Semiconductor (TSM), or a comparable major AI-infrastructure player (CoreWeave-scale or larger).

STANDARD EXCLUSIONS — CHECK THIS FIRST, BEFORE THE DEAL GATE BELOW OR ANYTHING ELSE IN THIS SECTION. Always reject (relevant: false, no tier, no kind), regardless of company size and regardless of how prominently "AI" appears in the headline: routine product launches, feature announcements, or beta/preview rollouts (a new Siri/Assistant/Copilot feature, a redesigned app or interface, a new device going on sale, an OS update); sub-$1B customer wins; minor earnings beats/misses; and normal single-company operational noise (hiring, office moves, executive changes, minor guidance tweaks). Mentioning "AI" does not exempt a story from this exclusion — only an actual M&A/partnership/capex transaction, or a regulatory/legal outcome, can clear this section at all. If the article describes what a company's product now DOES rather than a transaction, a capex commitment, or a legal/regulatory outcome, it fails here — stop, do not proceed to the DEAL GATE below.

Example — REJECT under this exclusion: "Apple releases test of redesigned Siri AI before iPhone 18 hits stores this week." A product feature rollout ahead of a device launch — no acquisition, no capex figure, no regulatory action. relevant: false, despite naming a priority company and mentioning "AI."

DEAL GATE (replaces the old $2B floor for M&A/partnership/minority-stake items only — a hyperscaler's own capex/guidance print from an earnings call or investor update is a different category, still governed by the locked capex rule elsewhere, and bypasses this gate entirely):

OUT — relevant: false, no tier, no kind: any M&A/partnership/minority-stake commitment under $20B (unless it's a hyperscaler capex/guidance print, which doesn't use this gate at all).

LIVE (goes on to FIRST_PRINT/FOLLOW_UP classification above) requires BOTH a size test AND a confirmation test — a deal that only clears the size threshold via an unconfirmed report (anonymous sources, "people familiar with the matter," analyst speculation, or a single outlet's own reporting with no primary-source citation) does NOT clear this gate, no matter how specific or credible the dollar figure sounds. The dollar figure itself must be confirmed by an actual press release, SEC filing (e.g. an 8-K), or the company's own earnings call/investor update — not merely reported by a news outlet citing unnamed sources. A credible report that clearly identifies a specific pending deal and figure, sourced only to anonymous/unofficial channels with no company or regulatory confirmation yet, does not clear this gate — treat it as an unconfirmed rumor (reject it, or if it's restating an ALREADY-confirmed deal's terms, FOLLOW_UP per the step above), regardless of size.

With that confirmation requirement satisfied, LIVE if EITHER:
(a) the buyer is Nvidia, Microsoft, Alphabet/Google, Amazon, Meta, or Broadcom AND the confirmed disclosed value is $20B or more, OR
(b) the transaction is a compute/foundry/networking/AI-energy/data-center deal of $50B or more, regardless of buyer.

AMD, TSM, Intel, and Apple do NOT get the automatic $20B line — only the $50B-any-buyer line applies to them, unless the deal changes export rules, foundry capacity, or the legal stack in a way statable as an index-level effect in one sentence.

Calibration, not exact-match overrides — reason from the rule, not these specific numbers: a ~$13B software/AI-startup purchase by a single mega-cap buyer is OUT (below $20B, a stock story, not a regime move). A ~$32B or ~$20B confirmed acquisition by one of the six named buyers is LIVE. A ~$40B infrastructure consortium deal is OUT under both tests (no single buyer clears $20B, and $40B misses the $50B infrastructure line). An ~$80B confirmed infrastructure/chip-producer takeout is LIVE under the $50B-any-buyer line regardless of buyer.

SOURCE PRIORITY: Prefer information from a press release or SEC filing first, an earnings call or investor update second, and Tier-1 financial media (Reuters, Bloomberg, WSJ, CNBC breaking coverage) third. Discount unconfirmed reports, analyst speculation, or secondary outlets restating another outlet's story.

RE-FLAGGING RULE: If this article reports on a deal, partnership, or capex commitment that has already been covered (same actors, same core terms), do not treat it as newly relevant unless it reports material new terms, a timeline acceleration, or a scope expansion beyond what was previously announced. A recap, confirmation, or analyst reaction to an already-known deal is an echo — fail it under Filter 6 (Confirmation Trap Test).

TIER FOR DEALS THAT CLEAR THE GATE ABOVE (use in place of the geopolitical tier definitions in DECISION 5 for this category; a deal that fails the gate is never tiered at all — Tier 3 is not a landing spot for a gate failure. A capex beat keeps its own separate tier treatment below, not this section):
Tier 1: an immediate, clear index-level catalyst — a finalized, signed transformative takeout with a stated close path, or a comparable unambiguous done-deal.
Tier 2: material but still contingent — announced but not yet closed, a regulator still ahead, or "getting close" language from a primary actor plus a confirming third party.
Tier 3: the deal itself is confirmed (buyer, target, and size disclosed via a real press release/8-K/earnings call — the gate's confirmation requirement is already satisfied), but the article is otherwise thin — single-outlet coverage of that disclosure with no additional corroboration yet, or the disclosure itself is a brief/preliminary announcement without full deal terms. Tier 3 always means "confirmed but under-specified," never "unconfirmed."
Capex beat (separate from the deal gate — evaluated on its own, not against the $20B/$50B thresholds): Tier 1 for a capex beat >20% over prior guidance paired with strong demand/backlog framing; Tier 2 for a strategic government/industrial partnership or multi-year build-out without near-term capex acceleration.

DIRECTION FOR TECH/AI MEGA-DEALS — DELIBERATELY DIFFERENT FROM THE GEOPOLITICAL CHAINS BELOW: Do not default to a confident bullish or bearish call for this category. Even a large, clearly-covered mega-deal can coincide with a same-day stock move driven by unrelated macro conditions — a confident directional call here risks being wrong for reasons that have nothing to do with the deal's actual merits (real example: the Apple-Broadcom $30B chip deal, August 2026). Default to "neutral" and use the summary/reasoning fields to surface the event, its size, and its terms factually. Only lean bullish or bearish when the article itself contains genuinely one-sided evidence:
- Lean BEARISH only if the article contains explicit margin-pressure language or explicit no-ROI/return-timeline-risk language from the company or credible analysts.
- Lean BULLISH only if there is a capex beat >20% over prior guidance AND an explicit strong-demand, backlog, or monetization link stated in the article (not inferred).
- Otherwise (the item already cleared STANDARD EXCLUSIONS and the DEAL GATE above, but the article itself contains no one-sided bullish/bearish evidence): direction = "neutral", relevant = true, and the summary should surface the deal size/terms/actors so a trader can weigh it themselves. This neutral default applies ONLY to an item that already cleared the gates above — it is never a fallback for a story that should have been rejected under STANDARD EXCLUSIONS.

MEGA-CAP REGULATORY/LEGAL OUTCOME RULES — applies to any article reporting a settlement, fine, verdict, judgment, or forced product/business change resulting from a regulatory or legal proceeding against one or more priority mega-cap companies (Nvidia, Apple, Microsoft, Alphabet/Google, Amazon, Meta, Broadcom, AMD, Intel, Taiwan Semiconductor (TSM), or a comparable index-weight tech/AI-infrastructure company).

TWO SEPARATE, ALWAYS-INDEPENDENT ASSESSMENTS:
1. COMPANY-LEVEL IMPACT — negative, neutral, or positive for the company itself.
2. NQ/TECH-COMPLEX IMPACT — negative, neutral, or positive for broader risk appetite today.
These can disagree — a clearly negative outcome for one company can be a non-event for the index, and vice versa. Only the NQ-level assessment sets this article's direction and tier. The company-level assessment is reported in the reason field but never overrides the NQ-level read.

TIER IS DRIVEN BY MAGNITUDE OF ACTUAL NQ-LEVEL IMPACT, NOT SURFACE PATTERN-MATCHING. Do not tier by simply counting how many companies are involved — count is one input, not the rule. Weigh all of the following together for the specific headline in front of you; none of them is individually decisive:
- Scale of the number relative to the company/companies involved — a fine that's trivial relative to one company's cash flow is not the same magnitude as one that's trivial for a smaller company but material for a larger one, or vice versa. Judge relative to what the entity(ies) involved can actually absorb, not the raw dollar figure alone.
- How many priority mega-caps are affected, and whether the exposure is genuinely shared or coincidental — two unrelated companies fined for two unrelated things on the same day is different from two companies fined for the same underlying practice, which signals a sector-wide pattern regulators are now pursuing broadly.
- Certainty of the number — capped/known/payable-over-time vs. open-ended/uncapped/appealable.
- Whether the market had already priced in a worse outcome — genuine relief vs. genuine surprise.
- Forward-looking business impact — does the remedy change how the company/sector operates going forward (product redesign, engagement limits, structural changes), or does it just resolve past liability with no operational change?
- Confirmed market reaction, if available — did the stock/sector actually move on the print, or is that unknown/pending?
- Whether this looks like an isolated event or part of an emerging pattern — one lawsuit is different from what looks like the third in a developing wave of similar actions against the same sector.

TIER CLASSIFICATION FOR MEGA-CAP REGULATORY/LEGAL OUTCOMES (use in place of the geopolitical tier definitions in DECISION 5 for this category), defined by the combined weight of the factors above, not any single factor alone:
Tier 3: The weighted factors suggest limited, contained NQ-level impact — no matter whether that's one company or several, and no matter the raw dollar size, if the market can plausibly absorb it without a genuine repricing of sector risk.
Tier 2: The weighted factors suggest a real, if not extreme, shift in how the market prices sector-wide legal/regulatory risk — this can be triggered by one company (an unexpected verdict) or several (a coordinated pattern), driven by the substance of the factors above, not a headcount threshold.
Tier 1 (rare): Structural/existential impact to a core NQ business model, or a same-day event significant enough to reprice the sector's risk premium broadly.

DIRECTION FOR MEGA-CAP REGULATORY/LEGAL OUTCOMES IS NOT A LOOKUP TABLE. Do not pre-map specific factor combinations to specific directions. Weigh the factors for the specific headline and reach whatever direction genuinely fits — bearish, neutral, or bullish for NQ specifically (separate from whatever the company-level read is). A "negative for the company" outcome does not automatically mean bearish for NQ, and a "positive for the company" outcome does not automatically mean bullish for NQ — judge the NQ-level factors on their own terms.

REASON FIELD FORMAT FOR THIS CATEGORY: write the reason field as three lines — company-level assessment (one line, exactly one of negative/neutral/positive with a brief cause), NQ-level assessment (one line, exactly one of negative/neutral/positive, naming which factors above weighed most), then tier. Format: "Company: [negative/neutral/positive] — [why]. NQ: [negative/neutral/positive] — [why, citing the deciding factors]. Tier: [1/2/3]." If the two levels disagree, the NQ-level read is what's scored.

Your job is to read each full article, then make five decisions:

DECISION 1 — RELEVANCE
Is this genuinely new, market-moving information that would cause a futures trader to reconsider their directional bias for today's session?

Think like a trader sitting down at 8AM asking: "Does this change anything about how I trade today?"

Pass if it involves: Federal Reserve policy or official commentary, geopolitical escalation or resolution affecting global risk sentiment, major economic data surprises, energy market shocks, trade policy changes with immediate impact, systemic financial risk, or significant government actions with direct market consequences, or major tech/AI infrastructure deals/capex commitments meeting the TECH/AI MEGA-DEAL RULES threshold above, or mega-cap regulatory/legal outcomes meeting the MEGA-CAP REGULATORY/LEGAL OUTCOME RULES above.

Fail if it involves: opinion or commentary on past market moves, investment advice or tips, personal finance stories, single company news unless systemically important, celebrity investor quotes, lifestyle or consumer behavior stories, retail shopping guides or consumer deal/discount roundups (e.g. "back to school savings," "extra deals," holiday shopping tips, or similar listicle-style consumer spending content — even if framed around tariffs or prices), prediction-market or betting-market odds and probability content (e.g. Kalshi, Polymarket, or PredictIt contract prices or probability shifts on a geopolitical or economic outcome) — a market's aggregated probability estimate is not itself a new event, even when the underlying outcome concerns something market-moving like a nuclear deal, election, or rate decision, newsletter recap formats, or anything that describes what already happened rather than new information AND cannot be tied to an identifiable already-scored event still within its FOLLOW_UP window (see the FIRST_PRINT/FOLLOW_UP step above — a same-story restatement that IS tied to a still-live event is FOLLOW_UP, not a DECISION 1 failure).

Before passing any article, run it through these six filters. If it fails any one of them, reject it:

FILTER 1 — SOURCE VS ECHO
Is this the event itself or a reaction to an event that already happened? A SOURCE event is new information the market hasn't priced in yet. It originates from a primary actor — a government, central bank, military, or natural force. An ECHO event is any person, company, or institution RESPONDING to or REPORTING ON a known macro situation.

Critical rule: If a company name appears as the subject of the headline and the headline describes them REACTING to a macro event (adding fees, raising prices, cutting jobs, warning of impacts, adjusting operations) — it is ALWAYS an echo. Reject it.

Exception: a company announcing, signing, or confirming its own new deal, partnership, or capex commitment is the SOURCE of that event, not an echo — they are the primary actor creating new information, not reacting to someone else's action. Reserve "echo" for a company responding to an external event (tariffs, energy prices, a war, a competitor's move, etc.).

A regulatory settlement, fine, verdict, or court judgment against a company is also a SOURCE event, not an echo — the regulator or court is the primary actor delivering new information, even though the company is the named subject.

The presence of macro keywords like "war", "energy", "Iran", "tariff" in a headline does NOT make it a source event. Ask: who is the ACTOR and what ACTION did they take? If the actor is a corporation reacting to an existing situation — it's an echo regardless of the macro language surrounding it.

FILTER 2 — RECENCY TEST
Is this reporting something happening RIGHT NOW, a FOLLOW_UP-window restatement of a still-live event (classify those FOLLOW_UP per the step above — don't fail here), or something genuinely stale — a week-in-review piece, an "after X weeks of..." article, or historical context with no live connection? Only that last category fails this filter.

FILTER 3 — SPECIFICITY TEST
Is this about a specific actionable event or a general mood/sentiment piece? Vibe articles, market psychology pieces, and "how to navigate" content are not tradeable information.

FILTER 4 — ACTOR TEST
Is the person or organization in this headline someone who directly moves markets through their decisions? Federal Reserve officials, heads of state, treasury secretaries, central bank chiefs, and major geopolitical actors = yes. State governors, backbench senators, corporate executives reacting to macro events, NASA, local officials = no, unless their specific action is systemically important to financial markets.

FILTER 5 — MARKET DOMAIN TEST
Does this article exist within the domain of financial markets, geopolitics affecting markets, energy, trade, or monetary policy? Articles about space missions, scientific discoveries, social policy, and non-financial government activity should be rejected even if they use financial language.

FILTER 6 — CONFIRMATION TRAP TEST
Is this article just confirming something the market already knows and has already priced in? If the macro situation is already established and this is just another data point piling on, it adds no new directional information ON ITS OWN — but per the FIRST_PRINT/FOLLOW_UP step above, that is grounds for classifying it FOLLOW_UP (relevant: true, 24-hour clock), not for failing it under this filter. Only fail it here if it also can't be tied to any identifiable already-scored event at all.

POLITICAL PRESSURE ON THE FED — GATE (a special outcome, NOT a DECISION 1 rejection): applies to any article centered on a President, administration official, or member of Congress pressuring, criticizing, or being described as on a "collision course" with the Federal Reserve, its chair, or FOMC members over policy.

An item needs a realistic path to moving NQ/ES risk BY ITSELF to score. Pressure and rhetoric alone are not that path — they stay on the Economic Calendar pillar's own speech card (Neutral unless they change the actual policy path), not here, unless a genuinely new concrete action is confirmed in this article.

FAILS THE GATE (gate_pass: false — still relevant: true, still classify kind and clock it normally per FIRST_PRINT/FOLLOW_UP above so it stays visible on schedule, but direction must be "neutral" and tier must be omitted): renewed or continued pressure, "collision course"/"boxed in" framing, criticism of Fed independence, calls for rate cuts or a chair's removal, speculation about what the Fed chair might do — with NO new concrete action in THIS article. A new interview, a new op-ed, or a new round of the same rhetoric does not clear this on its own.

CLEARS THE GATE (gate_pass: true, or omit the field — proceed to normal DECISION 2-5 scoring): a concrete institutional action has actually occurred and is confirmed in this article — the Fed chair or a governor is fired or resigns, a replacement is confirmed, legislation is introduced or passed that changes the Fed's structure or mandate, or a formal White House directive or executive order is issued to the FOMC.

ANTI-RECURRENCE CHECK: do not score both "if the chair capitulates" and "if the market reads it as politicization" as bearish outcomes for the same article — that is not identifying a catalyst, it is asserting the story matters no matter what happens. If your own reasoning would call every possible outcome of a pressure story bearish, that itself is the signal that this fails the gate.

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
[{{"id": 1, "relevant": true, "confidence": 0.95, "category": "geopolitical", "direction": "bearish", "reason": "Iran war escalation directly affects oil and risk sentiment", "summary": "Your 3-4 sentence market summary here.", "uncertainty_score": 85, "tier": 1, "kind": "first_print", "reasoning": "Active war escalation directly threatens oil supply and broad risk sentiment."}}, {{"id": 2, "relevant": true, "confidence": 0.8, "category": "geopolitical", "direction": "neutral", "reason": "Renewed pressure on the Fed chair with no new concrete action — fails the political pressure gate.", "summary": "Your 3-4 sentence market summary here.", "uncertainty_score": 60, "kind": "follow_up", "gate_pass": false, "reasoning": "Continued rhetoric, no firing/resignation/legislation/directive — not a catalyst by itself."}}]

Use only "bearish", "bullish", or "neutral" for direction.
Use confidence between 0.0 and 1.0.
Use tier as an integer: 1, 2, or 3. Omit tier entirely when gate_pass is false.
Use kind as either "first_print" or "follow_up" for every relevant item, per the FIRST_PRINT/FOLLOW_UP step above. Omit for a non-relevant item.
If relevant is false, still provide a summary field but it can be empty string.
Use gate_pass: false ONLY for an item that fails the POLITICAL PRESSURE ON THE FED — GATE above — it stays relevant: true (still gets a kind and clock so it stays visible), but direction must be "neutral" and tier must be omitted, so it contributes zero score. Omit gate_pass entirely (or use true) for every other item — this field exists solely to mark that one gate-failure case as visible-but-non-scoring rather than dropped.

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

For tech/AI mega-deal articles, use the TIER CLASSIFICATION FOR TECH/AI MEGA-DEALS section above instead of the definitions below.
For mega-cap regulatory/legal outcome articles, use the TIER CLASSIFICATION FOR MEGA-CAP REGULATORY/LEGAL OUTCOMES section above instead of the definitions below.

Tier 1 (±1.7): Active war or escalation between major powers, nuclear threats/incidents, major confirmed peace deals or ceasefires that meaningfully reduce geopolitical risk, or credible major supply disruptions (e.g. Hormuz closure threat).
Tier 2 (±0.75): Significant troop buildups, major diplomatic breakdowns, new meaningful sanctions, or credible energy market threats that are not Tier 1 level.
Tier 3 (±0.35): Minor diplomatic noise, corporate geopolitical news, speculative or secondary headlines with limited immediate market relevance.

Key rules:
- Prioritize actual market impact and context over headline keywords. The presence of words like "ceasefire" or "deal" does not automatically make something Tier 1 — evaluate whether a real, credible development occurred.
- When uncertain between tiers, default to the lower tier.
- De-escalation and peace developments are generally Bullish for US equities. Escalation and conflict are generally Bearish.
- Oil/Energy Rule: Falling oil prices caused by geopolitical de-escalation or peace deals are Bullish for equities. Only classify oil price moves as Bearish when driven by demand destruction, recession fears, or oversupply.
- Major Economic Data Exception — NFP / CPI / GDP only: If the article reports actual data for Non-Farm Payrolls (NFP), CPI (any variant: Core CPI, CPI m/m, CPI y/y), or GDP (any variant: GDP q/q, Final GDP), AND the deviation of actual from consensus forecast is 50% or greater in absolute relative terms (e.g. NFP 57K actual vs 114K expected = 50% miss; CPI 0.6% actual vs 0.3% expected = 100% beat), classify as Tier 1 regardless of other factors. Cite the specific actual vs. forecast figures and the % deviation as the reasoning. This exception does NOT apply to ISM, PMI, Retail Sales, ADP, or any other economic data — those continue to use standard Tier 2/3 judgment.

Articles to classify:
{article_list}"""

    def _call_haiku_classify(self, prompt, article_count=None):
        """Send a classification prompt (built by _build_classification_prompt())
        to Haiku and return the parsed results list, or [] on exhausted
        retries. Factored out of classify_relevance_batch() so
        force_reclassify() shares the IDENTICAL call/retry/parse behavior,
        not a second hand-copied version. `article_count` is cosmetic only
        (for the success log line) — pass None to log whatever the response
        itself contains."""
        # Retry/backoff mirrors utils.retry.fetch_with_retry's shape and defaults
        # (3 attempts, 2s/4s exponential backoff) — can't reuse that function
        # directly since it's built around requests.get()/HTTP status codes, not
        # the Anthropic SDK call, and the observed failure mode here isn't even
        # an SDK exception: the API call succeeds but returns an empty text
        # block, which then fails json.loads() — confirmed root cause of the
        # 2026-08-25 incident (27 consecutive failures over 2h, no retry existed,
        # recovery was luck — the same headlines happening to still be in
        # TheNewsAPI's next returned set — not resilience).
        RETRIES = 3
        BACKOFF = 2
        last_exc = None
        for attempt in range(RETRIES):
            response = None
            try:
                response = self.anthropic_client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=8192,
                    messages=[{"role": "user", "content": prompt}]
                )
                text = response.content[0].text.strip()
                if '```' in text:
                    text = text.split('```')[1]
                    if text.startswith('json'):
                        text = text[4:]
                results = json.loads(text)
                count = article_count if article_count is not None else len(results)
                if attempt:
                    pulse_logger.log(f"✅ Claude Haiku classified {count} articles (succeeded on attempt {attempt + 1}/{RETRIES})")
                else:
                    pulse_logger.log(f"✅ Claude Haiku classified {count} articles")
                return results
            except Exception as e:
                last_exc = e
                stop_reason = getattr(response, 'stop_reason', None) if response is not None else None
                raw_preview = None
                if response is not None:
                    try:
                        raw_preview = response.content[0].text[:200]
                    except Exception:
                        raw_preview = None
                pulse_logger.log(
                    f"⚠️ Claude Haiku classifier failed (attempt {attempt + 1}/{RETRIES}): {e} | "
                    f"stop_reason={stop_reason!r} | raw_response_preview={raw_preview!r}",
                    level="WARNING"
                )
                if attempt < RETRIES - 1:
                    time.sleep(BACKOFF * (2 ** attempt))
        pulse_logger.log(f"⚠️ Claude Haiku classifier exhausted {RETRIES} attempts, giving up this cycle: {last_exc}", level="WARNING")
        return []

    def force_reclassify(self, headline):
        """Manual, on-demand lever: re-run the FULL current classification
        prompt against an already-classified item's STORED source text —
        never a live fetch of story_url, since pages get edited, paywalled,
        or redirected after the fact (see article_text on the pin/cache
        record). Looks for `headline` first as a pinned story, then as a
        /data/gemini_classifications.json cache entry.

        Always produces a COMPLETE new classification (relevant, direction,
        tier, confidence, tier_reasoning, gate_pass, etc.) — there is no
        "just check kind" mode; every classification field is refreshed
        together, consistently, from the source text, even when only the
        kind is what someone asked about.

        Direction-dependent clock/anchor rule (classified_at):
          - DOWNGRADE (first_print -> follow_up): classified_at is left at
            its ORIGINAL value (pin's own classified_at, falling back to
            pinned_at for a pin predating this field). The corrected 24h
            follow_up window is measured from when it was ALWAYS actually
            classified — if that means the window has already elapsed,
            that's correct, not a bug.
          - UPGRADE (follow_up -> first_print): classified_at is reset to
            NOW, giving the item its full 48h window from the moment the
            missed new fact is recognized.
          - NO CHANGE (kind confirmed): classified_at is left completely
            untouched — a confirming reclassify must not reset or extend
            anything.
        Every other classification field (tier/direction/confidence/
        tier_reasoning/gate_pass/etc.) is refreshed regardless of which of
        the three above applies — only the clock/anchor has direction-
        dependent treatment.

        Manual trigger only (see ui/dashboard.py's /api/geo-force-reclassify
        route) — no scheduled/automatic version.

        Returns a result dict: {'ok': True, ...details...} or
        {'ok': False, 'error': '...'}.
        """
        if self.anthropic_client is None:
            return {'ok': False, 'error': 'Haiku unavailable — ANTHROPIC_API_KEY not set'}

        pins = self.load_pinned_stories()
        pin_idx = next((i for i, p in enumerate(pins) if p.get('headline') == headline), None)
        is_pin = pin_idx is not None

        gemini_cache_file = "/data/gemini_classifications.json"
        gemini_cache = {}
        if not is_pin:
            try:
                if os.path.exists(gemini_cache_file):
                    with open(gemini_cache_file, 'r') as f:
                        gemini_cache = json.load(f)
            except Exception as e:
                return {'ok': False, 'error': f'Failed to load classification cache: {e}'}
            if headline not in gemini_cache:
                return {'ok': False, 'error': f'No pinned or cached item found for headline: {headline!r}'}

        record = pins[pin_idx] if is_pin else gemini_cache[headline]

        article_text = record.get('article_text')
        if not article_text:
            # Fails closed rather than silently falling back to summary/URL —
            # an item pinned/cached before this feature shipped has no
            # stored text to reclassify against at all.
            return {'ok': False, 'error': 'No stored source text for this item — cannot reclassify '
                                           '(it predates the article_text storage feature)'}

        old_kind = record.get('kind') or 'first_print'
        old_tier = record.get('tier')
        old_direction = record.get('direction')
        old_confidence = record.get('confidence')
        old_classified_at = record.get('classified_at') or record.get('pinned_at', '')

        article_list = f"1. TITLE: {headline}\n   FULL TEXT: {article_text}\n\n"
        prompt = self._build_classification_prompt(article_list)
        results = self._call_haiku_classify(prompt, article_count=1)
        if not results:
            return {'ok': False, 'error': 'Haiku classification failed (see logs) — record left unchanged'}
        r = results[0]

        tier = r.get('tier')
        if tier not in (1, 2, 3):
            tier = None
        new_kind = r.get('kind')
        if new_kind not in ('first_print', 'follow_up'):
            new_kind = 'first_print'

        if old_kind == new_kind:
            direction_outcome = 'no_change'
            new_classified_at = old_classified_at  # left completely untouched
        elif old_kind == 'first_print' and new_kind == 'follow_up':
            direction_outcome = 'downgrade'
            new_classified_at = old_classified_at  # anchor stays at the ORIGINAL timestamp
        else:  # old_kind == 'follow_up' and new_kind == 'first_print'
            direction_outcome = 'upgrade'
            new_classified_at = datetime.now(timezone.utc).isoformat()  # fresh window from now

        record['relevant'] = r.get('relevant', record.get('relevant', True))
        record['confidence'] = r.get('confidence', 0)
        record['category'] = r.get('category', record.get('category', ''))
        record['direction'] = r.get('direction')
        record['reason'] = r.get('reason', record.get('reason', ''))
        record['summary'] = r.get('summary', record.get('summary', ''))
        record['uncertainty_score'] = r.get('uncertainty_score', 0)
        record['tier'] = tier
        record['kind'] = new_kind
        record['gate_pass'] = r.get('gate_pass', True)
        record['tier_reasoning'] = r.get('reasoning', '')
        record['classified_at'] = new_classified_at

        if is_pin:
            pins[pin_idx] = record
            self.save_pinned_stories(pins)
        else:
            gemini_cache[headline] = record
            try:
                atomic_write_json(gemini_cache_file, gemini_cache)
            except Exception as e:
                return {'ok': False, 'error': f'Reclassified but failed to persist: {e}'}

        pulse_logger.log(
            f"🔁 Force reclassify | {'pin' if is_pin else 'cache'} | '{headline[:60]}' | {direction_outcome} | "
            f"kind {old_kind} → {new_kind} | tier {old_tier} → {tier} | "
            f"direction {old_direction} → {record['direction']} | "
            f"confidence {old_confidence} → {record['confidence']} | "
            f"clock_anchor {old_classified_at} → {new_classified_at}"
        )

        return {
            'ok': True,
            'headline': headline,
            'store': 'pin' if is_pin else 'cache',
            'direction_outcome': direction_outcome,
            'old_kind': old_kind, 'new_kind': new_kind,
            'old_tier': old_tier, 'new_tier': tier,
            'old_direction': old_direction, 'new_direction': record['direction'],
            'old_confidence': old_confidence, 'new_confidence': record['confidence'],
            'old_classified_at': old_classified_at, 'new_classified_at': new_classified_at,
        }

    # ── Pinned Stories Store ─────────────────────────────────────────────────

    def _load_blocklist_strings(self):
        """Load geo blocklist as a list of lowercase strings for matching."""
        try:
            if os.path.exists(self.GEO_BLOCKLIST_FILE):
                with open(self.GEO_BLOCKLIST_FILE, 'r') as f:
                    raw = json.load(f)
                return [b.lower() for b in raw] if isinstance(raw, list) else []
        except Exception:
            pass
        return []

    @staticmethod
    def is_article_too_old(timestamp_str, max_hours=MAX_ARTICLE_AGE_HOURS):
        """Return True if the article's timestamp is older than max_hours.
        Fails CLOSED (treats as too old) on a missing or malformed
        timestamp — this function exists specifically to enforce a
        freshness ceiling, so bad/missing input should never be read as
        "keep it forever." """
        if not timestamp_str:
            return True
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            return age_hours > max_hours
        except Exception:
            return True

    @staticmethod
    def _pin_ttl_timestamp(story):
        """First non-empty of published_at/timestamp/date on a pinned-story
        record. Pins expire by when the underlying ARTICLE was published,
        not by when the system got around to pinning it — otherwise an
        article that took a while to get classified and pinned would get
        a fresh 48h lease from that late pin time instead of its real age.
        (Folded in from the former pipelines/geo_pin_ttl.py monkeypatch —
        same logic, now native.)"""
        for field in ('published_at', 'timestamp', 'date'):
            val = (story.get(field) or '').strip()
            if val:
                return val
        return ''

    def _pin_parsed_timestamp(self, ts):
        """Parse a pin's article timestamp (ISO or display-formatted,
        e.g. "Aug 29, 10:00 AM EST") into an aware datetime, or None on
        failure. Factored out of _pin_is_expired() so the published_at
        backfill pass (update_pinned_store()) can compute expires_at
        using the identical parse logic instead of duplicating it."""
        if not ts:
            return None
        try:
            dt = dateutil_parser.parse(
                ts,
                default=datetime.now(timezone.utc),
                tzinfos={"EST": -18000, "EDT": -14400},
            )
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    def _pin_is_expired(self, story):
        """Fails CLOSED (treats as expired) on a missing or unparseable
        timestamp, same reasoning as is_article_too_old(). Kept as its own
        check rather than reusing is_article_too_old() directly because it
        parses via dateutil (handling display-formatted strings like
        pin['timestamp'], e.g. "Aug 29, 10:00 AM EST", not just ISO 8601)
        and needs the EST/EDT tzinfo map to do it. Ages by kind_hours (24
        for a follow_up pin, 48 otherwise) rather than a flat
        MAX_ARTICLE_AGE_HOURS — under the current pin-creation rules
        (update_pinned_store() excludes follow_up from ever being pinned,
        and a refresh never changes an existing pin's kind) every pin is
        first_print today, so this is a no-op in practice — but it's the
        correct plumbing rather than an assumption, and costs nothing if
        that exclusion rule ever changes. Not a third clock — same two."""
        ts = self._pin_ttl_timestamp(story)
        dt = self._pin_parsed_timestamp(ts)
        if dt is None:
            return True
        age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        kind_hours = 24 if story.get('kind') == 'follow_up' else MAX_ARTICLE_AGE_HOURS
        return age_hours > kind_hours

    def load_pinned_stories(self):
        """Load pinned stories, dropping any that are blocklisted or whose
        underlying article is older than 48 hours (see _pin_is_expired)."""
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
                    pulse_logger.log(f"🚫 Manually blocked by user (pinned): {headline[:80]}")
                    dirty = True
                    continue
                if blocklist:
                    headline_lower = headline.lower()
                    matched = [b for b in blocklist if b in headline_lower]
                    if matched:
                        pulse_logger.log(f"🚫 Blocked by blocklist (pinned): {headline[:80]} | matched: {matched[0][:60]}")
                        dirty = True
                        continue
                if story.get('kind') not in ('first_print', 'follow_up'):
                    pulse_logger.log(f"⚠️ Pinned story missing/malformed kind '{story.get('kind')}' for '{headline[:60]}' — defaulting to first_print", level="WARNING")
                    story['kind'] = 'first_print'
                    dirty = True
                if self._pin_is_expired(story):
                    pulse_logger.log(f"🕐 Age cutoff (pinned, article timestamp): {headline[:80]}")
                    dirty = True
                    continue
                valid.append(story)
            if dirty:
                self.save_pinned_stories(valid)

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

    def _refresh_pin_classification(self, headline, new_class):
        """If `headline` currently exists as an active pin, refresh its cached
        direction/tier/confidence/summary in place. Called when a duplicate-event
        merge updates gemini_cache, so the pin never scores off a stale copy
        while gemini_cache holds the current classification."""
        try:
            pinned = self.load_pinned_stories()
            for pin in pinned:
                if pin.get('headline') == headline:
                    pin['direction'] = new_class.get('direction')
                    pin['tier'] = new_class.get('tier')
                    pin['confidence'] = new_class.get('confidence', 0)
                    pin['summary'] = new_class.get('summary', pin.get('summary', ''))
                    # Without this, a pinned gate-failed item (see POLITICAL
                    # PRESSURE ON THE FED — GATE) refreshed via a duplicate-
                    # event merge would keep whatever gate_pass it had before
                    # (or none at all) instead of the current classification's
                    # verdict — exactly the staleness this function exists to
                    # prevent for every other field here.
                    pin['gate_pass'] = new_class.get('gate_pass', True)
                    # Requirement 4 (Sep 18 fix): a pin stores the STORY, not
                    # whatever kind it was first read as, forever. Without
                    # this, a pin created as first_print would stay
                    # first_print (and keep its full, un-folded scoring
                    # weight) even after a later article about the same
                    # story is correctly reclassified follow_up under an
                    # updated prompt/gate — locking in the wrong
                    # classification until TTL expiry instead of the new
                    # evidence taking effect immediately.
                    pin['kind'] = new_class.get('kind', pin.get('kind', 'first_print'))
                    self.save_pinned_stories(pinned)
                    pulse_logger.log(f"📌 Pin refreshed with updated classification: '{headline[:60]}'")
                    return
        except Exception as e:
            pulse_logger.log(f"⚠️ Pin refresh failed: {e}", level="WARNING")

    def is_same_story(self, new_headline, other_headline):
        """Ask Haiku whether two headlines cover the same underlying story.

        Used only for duplicate-event detection (re-titled coverage of one event),
        never as a pin/scoring eviction gate — a SAME verdict here merges or skips
        a redundant classification entry, it does not remove either article's
        eligibility to score."""
        if self.anthropic_client is None:
            return False
        try:
            prompt = f"""You are evaluating whether two news headlines are about the same underlying geopolitical or market story.

NEW ARTICLE: {new_headline}
EXISTING ARTICLE: {other_headline}

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

    def has_outcome_diverged(self, existing_summary, existing_reason, new_summary, new_reason):
        """Ask Haiku whether two same-story classifications actually describe a
        different underlying outcome, despite matching direction/tier/confidence
        labels. Only ever called when those three labels already agree — this is
        the check for exactly that blind spot (e.g. "trade deal near-final" vs
        "50% retaliatory tariffs imposed" can both score bearish/tier-2 for
        unrelated reasons, so label agreement alone can't tell real staleness
        from coincidence).

        Fails CLOSED (returns True = diverged) on no client, empty summary/reason
        text on either side, a malformed/unexpected response, or any API error —
        deliberately biased toward treating an ambiguous read as a real change
        rather than silently reusing what might be a stale classification."""
        if self.anthropic_client is None:
            return True
        existing_text = (existing_summary or existing_reason or '').strip()
        new_text = (new_summary or new_reason or '').strip()
        if not existing_text or not new_text:
            return True
        try:
            prompt = f"""You are comparing two market/geopolitical event summaries that have already been classified as the same underlying story, with matching direction and impact tier.

EXISTING SUMMARY: {existing_text}

NEW SUMMARY: {new_text}

Despite matching direction/tier, has the underlying situation materially changed between these two summaries — e.g. a negotiation becoming an actual action taken, a threat becoming a confirmed event, a deal being reached or falling through, or any other concrete change in what actually happened (not just different wording for the same state of affairs)?

Respond with only one word: DIVERGED or UNCHANGED"""

            response = self.anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}]
            )
            result = response.content[0].text.strip().upper()
            if result == "UNCHANGED":
                return False
            # "DIVERGED", or anything malformed/unexpected — fail closed.
            return True
        except Exception as e:
            pulse_logger.log(f"⚠️ Haiku outcome divergence check failed: {e}", level="WARNING")
            return True

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
            if r.get('kind') == 'follow_up':
                # A follow_up's only claim to relevance is borrowed from
                # whatever it's restating — if the underlying story needs
                # to survive past a feed gap, its first_print already had
                # its own independent shot at getting pinned. Letting a
                # restatement spawn its own pin would give it the pin
                # mechanism's ~48h-from-article-time window instead of its
                # intended 24h ceiling, and is the same "stacking" pattern
                # Section 2.3 already warns against for tiers, showing up
                # in the pin mechanism instead.
                continue

            article = new_items[idx]
            headline = article.get('headline', '')
            tier = r.get('tier')
            if tier not in (1, 2, 3):
                tier = None
            pin_kind = r.get('kind')
            if pin_kind not in ('first_print', 'follow_up'):
                # Same raw Haiku response background_classify() already
                # resolved and warned about for this headline moments ago
                # in this same cycle — not a second independent failure,
                # so no second warning here.
                pin_kind = 'first_print'

            # Normalize the raw Haiku response onto the classification's
            # own field names (reasoning -> tier_reasoning; tier/kind
            # overlaid with the already-resolved values above) so the pin
            # can be built from the same CLASSIFICATION_TO_ITEM_FIELDS
            # list every other classification-copy site uses, instead of
            # a fourth hand-typed field subset.
            resolved = dict(r)
            resolved['tier'] = tier
            resolved['kind'] = pin_kind
            resolved['tier_reasoning'] = r.get('reasoning', '')
            # dict(r) leaves these absent (None via .get()) if Haiku's raw
            # response omitted them — pin default-to-0 to match
            # background_classify()'s own new_class convention, rather
            # than silently dropping the key.
            resolved['confidence'] = r.get('confidence', 0)
            resolved['uncertainty_score'] = r.get('uncertainty_score', 0)

            new_entry = {
                'headline': headline,
                'summary': r.get('summary', article.get('description', '')),
                'source': article.get('source', ''),
                'timestamp': article.get('timestamp', ''),
                'date': article.get('date', ''),
                'link': article.get('link', ''),
                'pinned_at': datetime.now(timezone.utc).isoformat(),
                # STORED source text, not the summary/tier_reasoning — required
                # for force_reclassify() to judge against the exact text Haiku
                # originally saw, never a live re-fetch of story_url (pages get
                # edited/paywalled/redirected). Retained for the pin's full
                # on-disk lifetime, not gated by any TTL. Already bounded by
                # fetch_full_article()'s existing 3000-char cap — no new size
                # limit needed. Empty string (not omitted) when only a
                # description-length fallback was available, so a later
                # reclassify attempt can fail with a clear "no stored source
                # text" error instead of silently reclassifying off a stub.
                'article_text': article.get('_full_text', ''),
                # NEW field (pins had no clock-anchor concept before this
                # feature): when this pin's CURRENT kind was last set. Set
                # here at creation; updated by force_reclassify() per its
                # direction-dependent rule. NOT currently read by any
                # existing pin-eviction/scoring code — pins still purge on
                # the unchanged, flat, article-publish-date-based 48h TTL in
                # load_pinned_stories(). This field exists solely to support
                # accurate force_reclassify() bookkeeping/logging for now.
                'classified_at': datetime.now(timezone.utc).isoformat(),
            }
            for field in self.PIN_CLASSIFICATION_FIELDS:
                val = resolved.get(field)
                if val is not None:
                    new_entry[field] = val

            # Only skip if this exact headline is already pinned — never evict a
            # distinct headline based on a same-story judgment. TTL (48h, in
            # load_pinned_stories) is the sole eviction mechanism.
            already_pinned = any(pin.get('headline', '') == headline for pin in pinned)
            if not already_pinned:
                pinned.append(new_entry)
                pulse_logger.log(f"📌 New story pinned: {headline[:60]}")

        self.save_pinned_stories(pinned)

        # Backfill published_at on any pin missing it, sourced from this same
        # cycle's new_items (published_at/timestamp/date fallback chain).
        # New entries above never set published_at directly, so this is what
        # actually populates it — which is what _pin_is_expired() reads.
        # Folded in from the former geo_pin_ttl.py monkeypatch, which ran
        # this as a genuinely separate pass re-reading the just-saved file;
        # preserved as-is (two passes, not merged into the loop above) to
        # keep this refactor behavior-identical rather than restructuring it.
        by_hl = {}
        for article in new_items or []:
            hl = article.get('headline', '')
            if not hl:
                continue
            by_hl[hl] = (
                (article.get('published_at') or '').strip()
                or (article.get('timestamp') or '').strip()
                or (article.get('date') or '').strip()
            )
        try:
            if not os.path.exists(self.pinned_store_file):
                return
            with open(self.pinned_store_file, 'r') as f:
                pinned_on_disk = json.load(f)
            backfill_dirty = False
            for pin in pinned_on_disk:
                if (pin.get('published_at') or '').strip():
                    continue
                val = by_hl.get(pin.get('headline', ''), '')
                if val:
                    pin['published_at'] = val
                    parsed = self._pin_parsed_timestamp(val)
                    if parsed is not None:
                        kind_hours = 24 if pin.get('kind') == 'follow_up' else MAX_ARTICLE_AGE_HOURS
                        pin['expires_at'] = (parsed + timedelta(hours=kind_hours)).isoformat()
                    backfill_dirty = True
            if backfill_dirty:
                self.save_pinned_stories(pinned_on_disk)
        except Exception as e:
            pulse_logger.log(f"⚠️ Pin published_at backfill failed: {e}", level="WARNING")

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
            # A gate-failed item (see POLITICAL PRESSURE ON THE FED — GATE)
            # deliberately has no tier — that's a permanent, correct state,
            # not "predates the tier field, needs backfilling." Without this
            # exclusion, this pass would assign one via its own separate,
            # narrower prompt (no gate awareness at all) and silently
            # reintroduce a nonzero score for an item calculate_score() was
            # explicitly told to zero out.
            and entry.get('gate_pass', True) is not False
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
- Major Economic Data Exception — NFP / CPI / GDP only: If the article reports actual data for Non-Farm Payrolls (NFP), CPI (any variant: Core CPI, CPI m/m, CPI y/y), or GDP (any variant: GDP q/q, Final GDP), AND the deviation of actual from consensus forecast is 50% or greater in absolute relative terms (e.g. NFP 57K actual vs 114K expected = 50% miss; CPI 0.6% actual vs 0.3% expected = 100% beat), classify as Tier 1 regardless of other factors. Cite the specific actual vs. forecast figures and the % deviation as the reasoning. This exception does NOT apply to ISM, PMI, Retail Sales, ADP, or any other economic data — those continue to use standard Tier 2/3 judgment.

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

    def daily_relevance_revalidation(self):
        """Once-daily maintenance pass (not per-cycle): re-run the current Haiku
        relevance prompt against every cached article still within its 48h
        active window and marked relevant. Gives a bad initial classification
        a chance to self-correct within a day, rather than persisting
        untouched for the full 48h TTL. Reuses classify_relevance_batch()
        directly so there is exactly one place the relevance criteria live —
        this pass never duplicates the prompt.

        Chunking (and its safety margin against classify_relevance_batch()'s
        max_tokens output cap) is delegated entirely to
        _classify_relevance_batch_chunked() — this function used to hand-roll
        its own chunk loop at CHUNK_SIZE=25, a SEPARATE choice from the one
        that later demonstrably failed live in fetch_news()'s path (same
        underlying call, same schema, same risk). Two independent chunk-size
        constants for the identical overflow risk is exactly the kind of
        silent-drift bug this project's own audit tooling exists to catch —
        collapsed into one shared constant/implementation instead of letting
        it recur here too."""
        if self.anthropic_client is None:
            return
        gemini_cache_file = "/data/gemini_classifications.json"
        try:
            if not os.path.exists(gemini_cache_file):
                return
            with open(gemini_cache_file, 'r') as f:
                gemini_cache = json.load(f)
        except Exception as e:
            pulse_logger.log(f"⚠️ Daily re-validation — failed to load classification cache: {e}", level="WARNING")
            return

        active_relevant = {
            headline: entry for headline, entry in gemini_cache.items()
            if entry.get('relevant') and not self.is_article_too_old(
                entry.get('classified_at', ''),
                max_hours=24 if entry.get('kind') == 'follow_up' else MAX_ARTICLE_AGE_HOURS
            )
        }
        if not active_relevant:
            return

        total_active = len(active_relevant)
        headlines = list(active_relevant.keys())
        # Re-run using the cached summary as context — no re-fetch of the
        # original URL, which may be paywalled, moved, or removed by now.
        pseudo_articles = [
            {'headline': h, 'description': active_relevant[h].get('summary') or active_relevant[h].get('reason') or '', 'link': ''}
            for h in headlines
        ]
        pulse_logger.log(f"🔄 Daily re-validation — re-checking {total_active} active article(s)")

        results = self._classify_relevance_batch_chunked(pseudo_articles)

        revoked = 0
        processed = 0
        for r in results:
            if not isinstance(r, dict) or 'id' not in r:
                continue
            idx = r['id'] - 1
            if not (0 <= idx < len(pseudo_articles)):
                continue
            headline = pseudo_articles[idx]['headline']
            processed += 1
            if not r.get('relevant'):
                gemini_cache[headline]['relevant'] = False
                gemini_cache[headline]['revalidated_at'] = datetime.now(timezone.utc).isoformat()
                gemini_cache[headline]['revalidation_reason'] = r.get('reason', '')
                revoked += 1
                pulse_logger.log(f"🔄 Daily re-validation — revoked relevance: '{headline[:60]}' | {r.get('reason', '')[:100]}")
            else:
                gemini_cache[headline]['revalidated_at'] = datetime.now(timezone.utc).isoformat()

        atomic_write_json(gemini_cache_file, gemini_cache)

        if processed == total_active:
            pulse_logger.log(
                f"✅ Daily re-validation done — {processed}/{total_active} article(s) processed, {revoked} revoked"
            )
        else:
            pulse_logger.log(
                f"⚠️ Daily re-validation — PARTIAL run: {processed}/{total_active} article(s) processed, "
                f"{revoked} revoked. {total_active - processed} article(s) left unrevalidated this cycle.",
                level="WARNING"
            )

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

    def _load_manual_blocklist_titles(self):
        """Load titles from the user-driven manual blocklist as a lowercase set."""
        try:
            if os.path.exists(self.GEO_MANUAL_BLOCKLIST_FILE):
                with open(self.GEO_MANUAL_BLOCKLIST_FILE, 'r') as f:
                    raw = json.load(f)
                if isinstance(raw, list):
                    return {entry.get('title', '').lower() for entry in raw if isinstance(entry, dict) and entry.get('title')}
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load geo manual blocklist: {e}", level="WARNING")
        return set()


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

    def _purge_blocked_from_cache(self):
        """On startup, remove any blocklisted or >48h-old articles from pinned
        stories and classification cache so they never re-enter the pipeline."""
        pulse_logger.log("🧹 Startup purge — scanning pinned stories and classification cache")
        blocklist = self._load_blocklist_strings()

        # Purge from pinned stories
        try:
            if os.path.exists(self.pinned_store_file):
                with open(self.pinned_store_file, 'r') as f:
                    pinned = json.load(f)
                pulse_logger.log(f"🧹 Pinned stories: {len(pinned)} entries before cleaning")
                cleaned = []
                blocked = 0
                aged = 0
                for story in pinned:
                    headline = story.get('headline', '')
                    headline_lower = headline.lower()
                    if blocklist:
                        matched = [b for b in blocklist if b in headline_lower]
                        if matched:
                            pulse_logger.log(f"🗑️ Force-removed pinned article (blocklist): {headline}")
                            blocked += 1
                            continue
                    if self.is_article_too_old(story.get('pinned_at', '')):
                        pulse_logger.log(f"🗑️ Force-removed pinned article (>48h old): {headline}")
                        aged += 1
                        continue
                    cleaned.append(story)
                if blocked or aged:
                    atomic_write_json(self.pinned_store_file, cleaned)
                pulse_logger.log(f"🧹 Pinned stories: {len(cleaned)} entries after cleaning ({blocked} blocked, {aged} aged out)")
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to purge pinned stories: {e}", level="WARNING")

        # Purge from classification cache
        cache_file = "/data/gemini_classifications.json"
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r') as f:
                    classifications = json.load(f)
                pulse_logger.log(f"🧹 Classification cache: {len(classifications)} entries before cleaning")
                keys_to_remove = []
                blocked = 0
                aged = 0
                for title, entry in classifications.items():
                    title_lower = title.lower()
                    if blocklist:
                        matched = [b for b in blocklist if b in title_lower]
                        if matched:
                            pulse_logger.log(f"🗑️ Force-removed classification (blocklist): {title}")
                            keys_to_remove.append(title)
                            blocked += 1
                            continue
                    if self.is_article_too_old(entry.get('classified_at', '')):
                        keys_to_remove.append(title)
                        aged += 1
                if keys_to_remove:
                    for k in keys_to_remove:
                        del classifications[k]
                    atomic_write_json(cache_file, classifications)
                pulse_logger.log(f"🧹 Classification cache: {len(classifications)} entries after cleaning ({blocked} blocked, {aged} aged out)")
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to purge classification cache: {e}", level="WARNING")

        # Second, article-time-based pass — the pinned-stories purge above
        # only catches pins whose OWN pinned_at is >48h old. A pin that took
        # a while to get classified could still pass that check while its
        # underlying article is well past 48h old by publish time. Catches
        # anything the pinned_at-based pass above missed. Folded in from the
        # former geo_pin_ttl.py monkeypatch, which ran this as a genuinely
        # separate pass after the original method returned; preserved as a
        # second pass here rather than merged into the loop above, matching
        # its exact prior behavior.
        try:
            if os.path.exists(self.pinned_store_file):
                with open(self.pinned_store_file, 'r') as f:
                    pinned = json.load(f)
                kept = []
                aged = 0
                for story in pinned:
                    if self._pin_is_expired(story):
                        pulse_logger.log(
                            f"🗑️ Force-removed pinned article (>48h from article timestamp): {story.get('headline', '')}"
                        )
                        aged += 1
                        continue
                    kept.append(story)
                if aged:
                    self.save_pinned_stories(kept)
                    pulse_logger.log(f"🧹 Pin TTL — aged out {aged} pin(s) from article timestamp")
        except Exception as e:
            pulse_logger.log(f"⚠️ Pin TTL extra purge failed: {e}", level="WARNING")

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

    @staticmethod
    def _keyword_matches(text_lower, keyword):
        """Whole-word/whole-phrase match — keyword must not be embedded inside
        a longer word (e.g. 'war' inside 'Edwards', 'rate hike' inside
        'corporate hike'). \\b boundaries are evaluated only at the start/end
        of the matched substring, so this works correctly for multi-word
        phrases too — the internal space in 'rate hike' is matched literally,
        no special handling needed."""
        return re.search(r'\b' + re.escape(keyword) + r'\b', text_lower) is not None

    def is_market_relevant(self, text):
        if not text:
            return False
        text_lower = text.lower()

        # Layer 1 — blocklist: explicit noise, always reject
        for ignore in self.ignore_keywords:
            if self._keyword_matches(text_lower, ignore):
                return False

        # Layer 2 — allowlist: must contain at least one market keyword to
        # proceed. self.company_keywords (bare mega-cap company/ticker names)
        # is deliberately NOT included here — a company name alone is never
        # sufficient; it must be paired with an actual market_keyword (a
        # deal, capex, regulatory, or macro term) in the same text.
        has_market_keyword = any(self._keyword_matches(text_lower, keyword) for keyword in self.market_keywords)
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
                f"&published_after={(datetime.now(pytz.utc) - timedelta(hours=MAX_ARTICLE_AGE_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')}"
                f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,abcnews.go.com,washingtonpost.com,nytimes.com"
            )
            response = fetch_with_retry(url, timeout=6, retries=3)
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
                f"&published_after={(datetime.now(pytz.utc) - timedelta(hours=MAX_ARTICLE_AGE_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')}"
                f"&domains=reuters.com,apnews.com,cnbc.com,bloomberg.com,wsj.com,ft.com,marketwatch.com,foxbusiness.com,politico.com,axios.com,thehill.com,cbsnews.com,nbcnews.com,washingtonpost.com,nytimes.com"
            )
            response = fetch_with_retry(url, timeout=6, retries=3)
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
                    if self.is_article_too_old(published):
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

        # HIGHEST PRIORITY — Manual geo blocklist (absolute first filter)
        manual_blocked = self._load_manual_blocklist_titles()
        if manual_blocked:
            pre_count = len(items)
            kept = []
            for i in items:
                if i['headline'].lower() in manual_blocked:
                    pulse_logger.log(f"🚫 Manually blocked by user: {i['headline'][:80]}")
                else:
                    kept.append(i)
            items = kept
            dropped = pre_count - len(items)
            if dropped:
                pulse_logger.log(f"🚫 Geo manual blocklist — {dropped} article(s) removed (highest priority)")

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

        # Scrub classification cache of manually blocked titles
        if manual_blocked:
            scrubbed = [k for k in gemini_cache if k.lower() in manual_blocked]
            for k in scrubbed:
                del gemini_cache[k]
                pulse_logger.log(f"🚫 Manually blocked by user (cache scrub): {k[:80]}")
            if scrubbed:
                atomic_write_json(gemini_cache_file, gemini_cache)
                pulse_logger.log(f"🚫 Geo manual blocklist — scrubbed {len(scrubbed)} entry(ies) from classification cache")

        # Early blocklist filter — remove blocked articles before classification/scoring
        early_blocklist = self._load_blocklist_strings()
        if early_blocklist:
            pre_count = len(items)
            filtered = []
            for i in items:
                headline_lower = i['headline'].lower()
                matched = [b for b in early_blocklist if b in headline_lower]
                if matched:
                    pulse_logger.log(f"🚫 Blocked by blocklist: {i['headline'][:80]} | matched: {matched[0][:60]}")
                else:
                    filtered.append(i)
            items = filtered
            early_blocked = pre_count - len(items)
            if early_blocked:
                pulse_logger.log(f"🚫 Early blocklist — {early_blocked} article(s) removed before classification")

        # Split into already classified vs new
        new_items = [i for i in items if i['headline'] not in gemini_cache]
        # TEMPORARY DIAGNOSTIC (shadow-mode silence investigation,
        # 2026-09-22) — remove once root-caused. Unconditional, every
        # fetch_news() call that reaches this point: proves whether
        # background_classify() (and thus the shadow-mode call inside it,
        # gated behind `if new_items:` below) ever actually has anything
        # to run on this cycle. gemini_classifications.json is append-only
        # (confirmed: no code path ever deletes a key from it, only
        # updates existing ones) — if TheNewsAPI keeps returning headlines
        # already in that ever-growing cache, new_items can legitimately
        # be empty cycle after cycle with no bug anywhere.
        pulse_logger.log(
            f"🔬 SHADOW DIAG — fetch_news() | items={len(items)} | "
            f"gemini_cache_size={len(gemini_cache)} | new_items={len(new_items)}"
        )
        known_relevant = []
        for i in items:
            cached = gemini_cache.get(i['headline'], {})
            if cached.get('relevant') and cached.get('confidence', 0) >= 0.75:
                kind_max_hours = 24 if cached.get('kind') == 'follow_up' else MAX_ARTICLE_AGE_HOURS
                if self.is_article_too_old(cached.get('classified_at', ''), max_hours=kind_max_hours):
                    continue
                if cached.get('direction'):
                    direction = cached['direction']
                    i['sentiment_score'] = 0.8 if direction == 'bullish' else -0.8 if direction == 'bearish' else 0.0
                if cached.get('summary'):
                    i['description'] = cached['summary']
                self._apply_classification_fields(i, cached)
                known_relevant.append(i)

        # Articles not yet classified — use keyword filter as temporary pass
        # Apply blocklist and age filter before allowing keyword fallback articles
        kw_blocklist = self._load_blocklist_strings()
        keyword_passed = []
        for i in new_items:
            if not self.is_market_relevant(i['headline']):
                continue
            headline_lower = i['headline'].lower()
            if kw_blocklist:
                matched = [b for b in kw_blocklist if b in headline_lower]
                if matched:
                    pulse_logger.log(f"🚫 Blocked by blocklist (keyword fallback): {i['headline'][:80]} | matched: {matched[0][:60]}")
                    continue
            # published_at is not populated on live item dicts at this point
            # in the pipeline (safe_parse() never sets it — real age
            # enforcement already happened once, correctly, at ingestion
            # using the raw timestamp before it's dropped from the dict).
            # Only a malformed (present-but-unparseable) value fails closed.
            kw_published_at = i.get('published_at', '')
            if kw_published_at and self.is_article_too_old(kw_published_at):
                pulse_logger.log(f"🕐 Age cutoff (keyword fallback): {i['headline'][:80]}")
                continue
            # Explicit marker (not inferred from missing haiku_tier/gemini_direction,
            # which a genuinely Haiku-confirmed item can also lack in rare malformed-
            # response cases) — this is the sole signal calculate_score() and
            # identify_flags() use to treat an item as not-yet-confirmed.
            i['keyword_fallback_only'] = True
            keyword_passed.append(i)

        # Return immediately — known relevant + keyword-passed new articles.
        # No same-story dedup here: every classified article scores independently,
        # regardless of whether Haiku judges it related to another live article.
        immediately_available = known_relevant + keyword_passed

        # Inject pinned stories not already covered by a live article.
        # Only an exact headline match skips injection — a same-story judgment
        # never retires a pin, so a distinct article keeps contributing to
        # score until it ages out via TTL in load_pinned_stories().
        pinned_stories = self.load_pinned_stories()
        current_headlines = {i['headline'] for i in immediately_available}
        injected = 0
        for pin in pinned_stories:
            pin_headline = pin.get('headline', '')
            # Exact headline already present — skip re-injecting, but keep the pin alive
            if pin_headline in current_headlines:
                continue
            # No live coverage — inject the pin
            injected_item = {
                'headline': pin_headline,
                'description': pin.get('summary', ''),
                'source': pin.get('source', ''),
                'timestamp': pin.get('timestamp', ''),
                'date': pin.get('date', ''),
                'link': pin.get('link', ''),
                'sentiment_score': 0.8 if pin.get('direction') == 'bullish' else -0.8 if pin.get('direction') == 'bearish' else 0.0,
                'market_relevant': True,
                'pinned': True
            }
            self._apply_classification_fields(injected_item, pin)
            immediately_available.append(injected_item)
            current_headlines.add(pin_headline)
            injected += 1

        # Hard age cutoff — drop stale articles before scoring. published_at
        # is not populated on live item dicts by this point in the pipeline
        # (safe_parse() never sets it — real age enforcement already
        # happened once, correctly, at ingestion using the raw timestamp
        # before it's dropped from the dict), and pinned items never carry
        # it either (they use pinned_at, checked separately inside
        # load_pinned_stories(), which already ran before these items were
        # injected above). A missing value therefore means "not applicable
        # here," not "unknown age" — only a malformed (present but
        # unparseable) value fails closed.
        before_age = len(immediately_available)
        age_kept = []
        for i in immediately_available:
            published_at = i.get('published_at', '')
            if published_at and self.is_article_too_old(published_at):
                pulse_logger.log(f"🕐 Age cutoff: {i['headline'][:80]}")
                continue
            age_kept.append(i)
        immediately_available = age_kept
        aged_out = before_age - len(immediately_available)
        if aged_out:
            pulse_logger.log(f"🕐 Age cutoff — {aged_out} article(s) dropped (>{MAX_ARTICLE_AGE_HOURS}h old)")

        # Final blocklist pass — catches anything injected after early filter (e.g. pins)
        late_blocklist = self._load_blocklist_strings()
        if late_blocklist:
            before = len(immediately_available)
            kept = []
            for i in immediately_available:
                headline_lower = i['headline'].lower()
                matched = [b for b in late_blocklist if b in headline_lower]
                if matched:
                    pulse_logger.log(f"🚫 Blocked by blocklist: {i['headline'][:80]} | matched: {matched[0][:60]}")
                else:
                    kept.append(i)
            immediately_available = kept
            blocked = before - len(immediately_available)
            if blocked:
                pulse_logger.log(f"🚫 Final blocklist — {blocked} article(s) filtered out")

        # Geo manual blocklist final pass — catches pinned stories injected after early filter
        manual_blocked_final = self._load_manual_blocklist_titles()
        if manual_blocked_final:
            before = len(immediately_available)
            kept = []
            for i in immediately_available:
                if i['headline'].lower() in manual_blocked_final:
                    pulse_logger.log(f"🚫 Manually blocked by user: {i['headline'][:80]}")
                else:
                    kept.append(i)
            immediately_available = kept
            manual_dropped = before - len(immediately_available)
            if manual_dropped:
                pulse_logger.log(f"🚫 Geo manual blocklist (final pass) — {manual_dropped} article(s) filtered out")

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
                    classifications = self._classify_relevance_batch_chunked(new_items)
                    duplicate_headlines = set()
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
                                kind = r.get('kind')
                                kind_defaulted = kind not in ('first_print', 'follow_up')
                                if kind_defaulted:
                                    if kind is not None:
                                        pulse_logger.log(f"⚠️ Haiku returned malformed kind '{kind}' for '{headline[:60]}' — defaulting to first_print", level="WARNING")
                                    kind = 'first_print'
                                kind_hours = 24 if kind == 'follow_up' else MAX_ARTICLE_AGE_HOURS
                                kind_display = f"{kind} ({kind_hours}h{', defaulted' if kind_defaulted else ''})"
                                text_source = new_items[idx].get('_text_source', 'unknown')
                                new_class = {
                                    'relevant': r.get('relevant', False),
                                    'confidence': r.get('confidence', 0),
                                    'category': r.get('category', ''),
                                    'direction': r.get('direction', None),
                                    'reason': r.get('reason', ''),
                                    'summary': r.get('summary', ''),
                                    'uncertainty_score': r.get('uncertainty_score', 0),
                                    'tier': tier,
                                    'kind': kind,
                                    # False only when Haiku explicitly failed this item under the
                                    # POLITICAL PRESSURE ON THE FED — GATE (visible/clocked, never
                                    # scored — see calculate_score()). Defaults true/absent-key-safe
                                    # for every other item, same as before this field existed.
                                    'gate_pass': r.get('gate_pass', True),
                                    'tier_reasoning': r.get('reasoning', ''),
                                    'text_source': text_source,
                                    # Stored source text for later force_reclassify() —
                                    # see update_pinned_store()'s new_entry for the full
                                    # rationale (never re-fetched from story_url).
                                    'article_text': new_items[idx].get('_full_text', ''),
                                    'classified_at': datetime.now(timezone.utc).isoformat()
                                }

                                # Duplicate-event check — same underlying story, just re-titled.
                                # Compared against the 15 most recently classified active
                                # (relevant, within-TTL) entries, not the full cache.
                                duplicate_of = None
                                if new_class['relevant']:
                                    active_entries = [
                                        (h, c) for h, c in gemini_cache.items()
                                        if h != headline and c.get('relevant')
                                        and not self.is_article_too_old(
                                            c.get('classified_at', ''),
                                            max_hours=24 if c.get('kind') == 'follow_up' else MAX_ARTICLE_AGE_HOURS
                                        )
                                    ]
                                    active_entries.sort(key=lambda hc: hc[1].get('classified_at', ''), reverse=True)
                                    candidates = active_entries[:15]
                                    for existing_headline, _ in candidates:
                                        if self.is_same_story(headline, existing_headline):
                                            duplicate_of = existing_headline
                                            break
                                    # Explicit outcome every time this runs — a silent miss here is
                                    # exactly what let the Sep 18 Warsh recap open a fresh pin
                                    # instead of folding into anything: there was no log line either
                                    # way, so "ran and found nothing" was indistinguishable from
                                    # "never ran at all."
                                    if duplicate_of is None:
                                        pulse_logger.log(
                                            f"🔍 is_same_story() — no match for '{headline[:60]}' "
                                            f"against {len(candidates)} candidate(s)"
                                        )

                                if duplicate_of:
                                    existing = gemini_cache[duplicate_of]
                                    tier_changed = existing.get('tier') != new_class['tier']
                                    direction_changed = existing.get('direction') != new_class['direction']
                                    confidence_shifted = abs((existing.get('confidence') or 0) - (new_class.get('confidence') or 0)) >= 0.10

                                    # Only reached when the three cheap label checks above all
                                    # agree — exactly the blind spot that let a stale "trade deal
                                    # near-final" anchor keep absorbing "50% retaliatory tariffs
                                    # imposed" headlines: both score bearish/tier-2 for unrelated
                                    # reasons, so label agreement alone can't tell real staleness
                                    # from coincidence. has_outcome_diverged() re-reads the actual
                                    # summary/reason text instead of the coarse labels.
                                    outcome_diverged = False
                                    if not (tier_changed or direction_changed or confidence_shifted):
                                        outcome_diverged = self.has_outcome_diverged(
                                            existing.get('summary', ''), existing.get('reason', ''),
                                            new_class.get('summary', ''), new_class.get('reason', '')
                                        )
                                        if outcome_diverged:
                                            pulse_logger.log(f"🧭 Outcome divergence detected despite matching labels: '{headline[:60]}' vs anchor '{duplicate_of[:60]}'")

                                    materially_changed = tier_changed or direction_changed or confidence_shifted or outcome_diverged
                                    if materially_changed:
                                        # A single Haiku read — even at confidence 1.0 — is not
                                        # reliable enough to flip a PINNED entry's classification
                                        # (confirmed incident: one same-story headline merged in at
                                        # conf=1.0 flipped a stable pin's direction, then a second
                                        # merge 6.5h later flipped it back, also at conf=1.0).
                                        # Pinned entries therefore require two consecutive merge
                                        # reads agreeing on direction AND tier before committing;
                                        # the first read is staged as pending_merge on the entry.
                                        # Confidence is deliberately not part of the agreement
                                        # check. A disagreeing read replaces the staged candidate;
                                        # an unconfirmed candidate just ages out with the entry's
                                        # normal 48h TTL. Non-pinned entries keep single-merge
                                        # overwrite behavior unchanged.
                                        is_pinned = any(
                                            p.get('headline') == duplicate_of
                                            for p in self.load_pinned_stories()
                                        )
                                        if is_pinned:
                                            pending = existing.get('pending_merge')
                                            if (
                                                pending
                                                and pending.get('direction') == new_class['direction']
                                                and pending.get('tier') == new_class['tier']
                                            ):
                                                gemini_cache[duplicate_of] = new_class
                                                gemini_cache[duplicate_of].pop('pending_merge', None)
                                                pulse_logger.log(f"🔁 Pinned entry updated after 2 agreeing merges: '{headline[:60]}' → '{duplicate_of[:60]}'")
                                                self._refresh_pin_classification(duplicate_of, new_class)
                                            else:
                                                if pending:
                                                    pulse_logger.log(f"🔁 Pinned entry staged candidate cleared by disagreeing merge: '{duplicate_of[:60]}'")
                                                gemini_cache[duplicate_of]['pending_merge'] = new_class
                                                pulse_logger.log(f"🔁 Pinned entry candidate staged, awaiting confirmation: '{headline[:60]}' → '{duplicate_of[:60]}'")
                                        else:
                                            gemini_cache[duplicate_of] = new_class
                                            pulse_logger.log(f"🔁 Duplicate event — updated existing entry in place: '{headline[:60]}' → merged into '{duplicate_of[:60]}'")
                                            self._refresh_pin_classification(duplicate_of, new_class)
                                    else:
                                        pulse_logger.log(f"🔁 Duplicate event — no material change, skipped: '{headline[:60]}' (same as '{duplicate_of[:60]}')")
                                    gemini_cache[headline] = {
                                        'relevant': False,
                                        'duplicate_of': duplicate_of,
                                        'classified_at': datetime.now(timezone.utc).isoformat()
                                    }
                                    duplicate_headlines.add(headline)
                                else:
                                    gemini_cache[headline] = new_class
                                    pulse_logger.log(
                                        f"🧭 Haiku tier | {headline[:60]} | Tier {tier if tier is not None else 'N/A (fallback)'} | "
                                        f"{r.get('direction', 'unknown')} | conf={r.get('confidence', 0)} | kind={kind_display} | src={text_source} | {r.get('reasoning', '')}"
                                    )
                        atomic_write_json(gemini_cache_file, gemini_cache)
                        filtered_classifications = [
                            r for r in classifications
                            if r['id'] - 1 < len(new_items)
                            and new_items[r['id'] - 1]['headline'] not in duplicate_headlines
                        ]
                        self.update_pinned_store(new_items, filtered_classifications)

                        # Re-merge into the pillar cache immediately rather than
                        # waiting for the next full fetch_news() cycle. Without
                        # this, a keyword-fallback article that Haiku just
                        # confirmed or rejected (gemini_cache above is already
                        # up to date) would sit unpromoted/undropped in the
                        # already-served pillar cache for up to the 3-minute
                        # freshness window plus however long until the next
                        # scheduled cycle.
                        try:
                            pillar_cached = cache.load(self.cache_key)
                            if pillar_cached:
                                refreshed = self._refresh_cached_data(pillar_cached['data'])
                                cache.save(self.cache_key, refreshed)
                        except Exception as e:
                            pulse_logger.log(f"⚠️ Background classify — failed to re-merge into pillar cache: {e}", level="WARNING")

                        pulse_logger.log(f"✅ Haiku background done — {len(classifications)} articles classified with summaries")

                    # SHADOW MODE (Stage 3 validation, not yet a scoring path)
                    # — runs strictly AFTER the real classification's cache
                    # writes above, on real incoming articles, gated behind an
                    # env var defaulting off. See _run_shadow_classification()
                    # for the isolation guarantees. Skipped entirely if the
                    # real path above raised (this except block, not here) —
                    # shadow data doesn't need every cycle, and a cycle where
                    # the real path itself failed isn't a useful comparison.
                    #
                    # TEMPORARY DIAGNOSTIC (shadow-mode silence investigation,
                    # 2026-09-22) — remove once root-caused. Unconditional,
                    # exactly as requested: proves the check itself is even
                    # being reached and what raw value it's actually seeing,
                    # regardless of outcome.
                    _raw_shadow_env = os.environ.get('GEO_SHADOW_MODE')
                    pulse_logger.log(
                        f"🔬 SHADOW DIAG — background_classify() reached the gate | "
                        f"raw GEO_SHADOW_MODE={_raw_shadow_env!r} | will_run={_raw_shadow_env == 'true'}"
                    )
                    if os.environ.get('GEO_SHADOW_MODE') == 'true':
                        self._run_shadow_classification(new_items, classifications)
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
            'nuclear': 95,
            'war': 90, 'wars': 90,
            'invasion': 92, 'invade': 92, 'invades': 92, 'invaded': 92, 'invading': 92,
            'missile': 88, 'missiles': 88,
            'attack': 85, 'attacks': 85, 'attacked': 85, 'attacking': 85,
            'bomb': 85, 'bombs': 85, 'bombing': 85, 'bombed': 85,
            'default': 85, 'defaulted': 85, 'defaulting': 85,
            'fomc': 85,
            'rate hike': 80, 'rate hikes': 80, 'rate cut': 80, 'rate cuts': 80,
            'powell': 78,
            'federal reserve': 78, 'tariff': 80, 'tariffs': 80, 'debt ceiling': 80,
            'shutdown': 75,
            'sanctions': 75, 'sanction': 75, 'sanctioned': 75, 'sanctioning': 75,
            'troops': 78,
            'recession': 75, 'ceasefire': 70,
            'deal': 65, 'deals': 65,
            'agreement': 65, 'agreements': 65,
            'escalation': 72, 'escalate': 72, 'escalates': 72, 'escalated': 72, 'escalating': 72,
            'inflation': 70,
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
                if self._keyword_matches(text, keyword):
                    if score > priority:
                        priority = score
                        flag_type = keyword
            if priority >= 65:
                if any(ts in source for ts in trusted_sources):
                    priority = min(priority + 5, 99)
                if item.get('keyword_fallback_only'):
                    # Not yet Haiku-confirmed — show as pending rather than a
                    # directional badge derived from the HF sentiment model,
                    # which is exactly the guess that produced the false
                    # "Bearish" read this display change is meant to prevent.
                    predicted_impact = 'pending classification'
                else:
                    predicted_impact = item.get('gemini_direction') if item.get('gemini_direction') else ('bearish' if item['sentiment_score'] < -0.3 else 'bullish' if item['sentiment_score'] > 0.3 else 'neutral')
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
                    'predicted_impact': predicted_impact,
                    'context': item.get('description', '')
                })

        flags.sort(key=lambda x: x['priority'], reverse=True)
        return flags[:5]

    def _get_article_priority(self, item):
        """Return keyword-based flag priority for an article (0 = no qualifying keyword)."""
        _keywords = {
            'nuclear': 95,
            'war': 90, 'wars': 90,
            'invasion': 92, 'invade': 92, 'invades': 92, 'invaded': 92, 'invading': 92,
            'missile': 88, 'missiles': 88,
            'attack': 85, 'attacks': 85, 'attacked': 85, 'attacking': 85,
            'bomb': 85, 'bombs': 85, 'bombing': 85, 'bombed': 85,
            'default': 85, 'defaulted': 85, 'defaulting': 85,
            'fomc': 85,
            'rate hike': 80, 'rate hikes': 80, 'rate cut': 80, 'rate cuts': 80,
            'powell': 78,
            'federal reserve': 78, 'tariff': 80, 'tariffs': 80, 'debt ceiling': 80,
            'shutdown': 75,
            'sanctions': 75, 'sanction': 75, 'sanctioned': 75, 'sanctioning': 75,
            'troops': 78,
            'recession': 75, 'ceasefire': 70,
            'deal': 65, 'deals': 65,
            'agreement': 65, 'agreements': 65,
            'escalation': 72, 'escalate': 72, 'escalates': 72, 'escalated': 72, 'escalating': 72,
            'inflation': 70,
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
            if self._keyword_matches(text, keyword):
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
        # Loaded once per call — see FOMC_FOLD_* / _check_ec_fold() above.
        ec_anchors = self._load_ec_fomc_anchors()
        weighted_sum = 0.0
        total_weight = 0.0
        haiku_tier_count = 0
        fallback_tier_count = 0
        pending_count = 0
        gate_failed_count = 0
        folded_count = 0
        tier_map = {1: (1.7, 4.0), 2: (0.75, 2.0), 3: (0.35, 1.0)}
        for item in items:
            # Keyword-fallback-only articles (no Haiku confirmation yet) never
            # contribute to the score — a pure keyword match is not a confirmed
            # classification. They stay visible (identify_flags() shows them as
            # "Pending Classification") and get promoted to normal scoring once
            # Haiku confirms or dropped entirely if Haiku rejects them.
            if item.get('keyword_fallback_only'):
                pending_count += 1
                continue
            # Gate-failed items (e.g. political pressure on the Fed with no
            # new concrete action — see the POLITICAL PRESSURE ON THE FED —
            # GATE prompt section) are a fully Haiku-confirmed verdict, not a
            # pending one — counted separately from pending_count so the log
            # line doesn't misreport them as "awaiting classification." They
            # stay visible on their normal FIRST_PRINT/FOLLOW_UP clock but
            # NEVER contribute a score. Checked before direction/sentiment_score
            # are even read — direction alone ("neutral") is not a safe
            # guarantee of zero, since a missing/neutral gemini_direction falls
            # through to the local sentiment analyzer's score on the headline,
            # which can still be nonzero.
            if item.get('haiku_gate_pass') is False:
                gate_failed_count += 1
                continue
            # App-layer EC fold (Sep 18 Warsh fix) — a recap/analysis article
            # about an FOMC event the EC pillar already scored never
            # contributes a second Geo score for that same event. Checked
            # before direction/sentiment_score for the same reason as the
            # gate above. See _check_ec_fold()'s docstring for the full
            # (a)-(d) condition breakdown.
            if self._check_ec_fold(item, ec_anchors):
                item['kind'] = 'follow_up'  # already true by construction (condition d) — set explicitly for clarity
                item['ec_folded'] = True
                folded_count += 1
                continue
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
                item_kind = item.get('kind')
                kind_hours = 24 if item_kind == 'follow_up' else MAX_ARTICLE_AGE_HOURS
                kind_display = f"{item_kind} ({kind_hours}h)" if item_kind in ('first_print', 'follow_up') else "N/A"
                pulse_logger.log(
                    f"🧭 Geo tier (Haiku) | {item.get('headline', '')[:60]} | Tier {haiku_tier} | {direction} | "
                    f"conf={conf} | kind={kind_display} | {item.get('haiku_tier_reasoning', '')}"
                )
            else:
                fallback_tier_count += 1
                # Fallback — keyword-based tier classification (used when Haiku tier
                # is missing/malformed, e.g. unclassified article or failed batch)
                kw_priority = self._get_article_priority(item)
                # Keyword fallback is capped at Tier 2 — only Haiku (which reads context)
                # can assign Tier 1. Binary: signal present → Tier 2; no signal → Tier 3.
                # haiku_conf >= 0.65 without a valid haiku_tier means Haiku ran but returned
                # a malformed tier — confidence is still a signal, so treat as Tier 2.
                if kw_priority >= 65 or (haiku_conf is not None and haiku_conf >= 0.65):
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
        if pending_count:
            pulse_logger.log(f"⏳ Geo — {pending_count} article(s) excluded from score, pending Haiku classification")
        if gate_failed_count:
            pulse_logger.log(f"🚪 Geo — {gate_failed_count} article(s) excluded from score, failed the political pressure gate (visible, non-scoring)")
        if folded_count:
            pulse_logger.log(f"🔗 Geo — {folded_count} article(s) excluded from score, folded into an existing EC event (visible, non-scoring)")
        if total_weight == 0:
            return 0.0
        return round(max(-2.0, min(2.0, weighted_sum / total_weight)), 2)

    def _reclassify_cached_pending(self, cached_items):
        """Re-trigger Haiku classification for cached articles that were never classified.

        Called when fetch_news() returns empty (API timeout / rate-limit) and the pipeline
        falls back to stale cached data.  Without this, any article that is stuck in
        keyword-fallback state at the moment of an outage stays unclassified for the
        entire duration of that outage — the normal background thread only fires when
        fetch_news() returns live articles.
        """
        if not self.anthropic_client or not cached_items:
            return
        gemini_cache_file = "/data/gemini_classifications.json"
        try:
            if os.path.exists(gemini_cache_file):
                with open(gemini_cache_file, 'r') as f:
                    gc = json.load(f)
            else:
                gc = {}
        except Exception:
            gc = {}

        pending = [
            i for i in cached_items
            if not i.get('pinned')
            and not i.get('haiku_tier')
            and not i.get('gemini_direction')
            and i.get('headline', '') not in gc
        ]
        if not pending:
            return

        pulse_logger.log(f"🔄 Cache fallback — re-queuing {len(pending)} unclassified article(s) for Haiku")

        def _classify():
            try:
                classifications = self.classify_relevance_batch(pending)
                if not classifications:
                    return
                for r in classifications:
                    idx = r['id'] - 1
                    if 0 <= idx < len(pending):
                        headline = pending[idx]['headline']
                        tier = r.get('tier')
                        if tier not in (1, 2, 3):
                            tier = None
                        kind = r.get('kind')
                        kind_defaulted = kind not in ('first_print', 'follow_up')
                        if kind_defaulted:
                            if kind is not None:
                                pulse_logger.log(f"⚠️ Haiku returned malformed kind '{kind}' for '{headline[:60]}' (fallback reclassification) — defaulting to first_print", level="WARNING")
                            kind = 'first_print'
                        kind_hours = 24 if kind == 'follow_up' else MAX_ARTICLE_AGE_HOURS
                        kind_display = f"{kind} ({kind_hours}h{', defaulted' if kind_defaulted else ''})"
                        text_source = pending[idx].get('_text_source', 'unknown')
                        gc[headline] = {
                            'relevant': r.get('relevant', False),
                            'confidence': r.get('confidence', 0),
                            'category': r.get('category', ''),
                            'direction': r.get('direction', None),
                            'reason': r.get('reason', ''),
                            'summary': r.get('summary', ''),
                            'uncertainty_score': r.get('uncertainty_score', 0),
                            'tier': tier,
                            'kind': kind,
                            'gate_pass': r.get('gate_pass', True),
                            'tier_reasoning': r.get('reasoning', ''),
                            'text_source': text_source,
                            'article_text': pending[idx].get('_full_text', ''),
                            'classified_at': datetime.now(timezone.utc).isoformat()
                        }
                        pulse_logger.log(
                            f"🔄 Fallback-reclassified: '{headline[:60]}' → "
                            f"{'relevant' if r.get('relevant') else 'irrelevant'} | "
                            f"Tier {tier or 'N/A'} | {r.get('direction', 'unknown')} | kind={kind_display} | src={text_source}"
                        )
                atomic_write_json(gemini_cache_file, gc)
                pulse_logger.log(f"✅ Fallback reclassification done — {len(classifications)} article(s) classified")
            except Exception as e:
                pulse_logger.log(f"⚠️ Fallback reclassification failed: {e}", level="WARNING")

        threading.Thread(target=_classify, daemon=True).start()

    def _filter_blocklisted_items(self, news_items):
        """Remove any article currently on the keyword or manual blocklist from
        a cached news_items list. Mirrors the two 'final pass' checks already
        applied inside fetch_news() (keyword substring match + manual exact
        title match) — needed here too because cache-fallback paths in fetch()
        can serve a snapshot captured before a blocklist add took effect."""
        if not news_items:
            return news_items, 0
        kw_blocklist = self._load_blocklist_strings()
        manual_blocked = self._load_manual_blocklist_titles()
        if not kw_blocklist and not manual_blocked:
            return news_items, 0
        kept = []
        removed = 0
        for item in news_items:
            headline_lower = item.get('headline', '').lower()
            if manual_blocked and headline_lower in manual_blocked:
                pulse_logger.log(f"🚫 Manually blocked by user (cache fallback): {item.get('headline', '')[:80]}")
                removed += 1
                continue
            matched = [b for b in kw_blocklist if b in headline_lower] if kw_blocklist else []
            if matched:
                pulse_logger.log(f"🚫 Blocked by blocklist (cache fallback): {item.get('headline', '')[:80]} | matched: {matched[0][:60]}")
                removed += 1
                continue
            kept.append(item)
        return kept, removed

    def _merge_fresh_classifications(self, all_items):
        """Pick up Haiku classification results that landed in
        gemini_classifications.json (via background_classify(),
        _reclassify_cached_pending(), or daily_relevance_revalidation()) but
        never made it back into this cached fetch() result.

        Only items explicitly marked keyword_fallback_only are candidates —
        not "any item missing haiku_tier/gemini_direction", since a
        genuinely Haiku-confirmed item can rarely lack both (e.g. a
        malformed direction in an otherwise-relevant response) and would be
        wrongly treated as still-pending by that inference.

        Three outcomes per pending item:
        - No cache entry yet → still pending, left untouched.
        - Haiku rejected it (relevant: false) → dropped from the list
          entirely, same as it would never have entered known_relevant.
        - Haiku confirmed it (relevant: true, confidence >= 0.75) → fields
          merged in and keyword_fallback_only cleared, promoting it to
          normal scoring."""
        if not all_items:
            return all_items, 0
        gemini_cache_file = "/data/gemini_classifications.json"
        try:
            if os.path.exists(gemini_cache_file):
                with open(gemini_cache_file, 'r') as f:
                    gc = json.load(f)
            else:
                return all_items, 0
        except Exception:
            return all_items, 0

        kept = []
        updated = 0
        for item in all_items:
            if not item.get('keyword_fallback_only'):
                kept.append(item)
                continue
            cached = gc.get(item.get('headline', ''), {})
            if not cached:
                kept.append(item)  # Haiku hasn't classified it yet — still pending
                continue
            if not cached.get('relevant') or cached.get('confidence', 0) < 0.75:
                pulse_logger.log(f"🔄 Cache refresh — Haiku rejected as not relevant, dropping: '{item.get('headline', '')[:60]}'")
                updated += 1
                continue  # drop — matches how known_relevant would never have included it
            if cached.get('direction'):
                direction = cached['direction']
                item['sentiment_score'] = 0.8 if direction == 'bullish' else -0.8 if direction == 'bearish' else 0.0
            if cached.get('summary'):
                item['description'] = cached['summary']
            self._apply_classification_fields(item, cached)
            item['keyword_fallback_only'] = False
            pulse_logger.log(f"🔄 Cache refresh — picked up fresh classification, promoted to scoring: '{item.get('headline', '')[:60]}'")
            updated += 1
            kept.append(item)
        return kept, updated

    def _refresh_cached_data(self, data):
        """Bring a cached fetch() result up to date before serving it:
        1) merge in any Haiku classification that arrived since this snapshot
           was cached but never made it back into the served item list, and
        2) re-filter against the current blocklist state.
        Operates on `all_items` (the full scoring set), not the top-10
        `news_items` display slice, and falls back to `news_items` for any
        cache entry saved before `all_items` existed. Recomputes
        active_flags/pillar_score/news_items if either step changed anything."""
        all_items = data.get('all_items', data.get('news_items', []))
        all_items, merged = self._merge_fresh_classifications(all_items)
        filtered, removed = self._filter_blocklisted_items(all_items)

        # 48h TTL — previously only enforced inside fetch_news() when a live fetch
        # actually ran. On repeated fetch failures/empty results this path served
        # the same cached all_items indefinitely, past their TTL. published_at is
        # not populated on live item dicts (real age enforcement already happened
        # once, correctly, at ingestion) or on pinned items (which use pinned_at,
        # checked inside load_pinned_stories()) — a missing value means "not
        # applicable here," not "unknown age," so it's exempted from this filter
        # regardless of pin status. Only a malformed (present but unparseable)
        # value fails closed.
        before_age = len(filtered)
        filtered = [
            i for i in filtered
            if not i.get('published_at')
            or not self.is_article_too_old(i.get('published_at', ''))
        ]
        aged_out = before_age - len(filtered)

        if not merged and not removed and not aged_out:
            return data
        flags = self.identify_flags(filtered)
        score = self.calculate_score(filtered, flags)
        data['all_items'] = filtered
        data['news_items'] = filtered[:10]
        data['active_flags'] = flags
        data['total_items'] = len(filtered)
        data['pillar_score'] = score
        if merged:
            pulse_logger.log(f"🔄 Cache refresh — merged {merged} newly-classified article(s), score recomputed: {score}")
        if removed:
            pulse_logger.log(f"🚫 Cache fallback — removed {removed} blocklisted item(s), score recomputed: {score}")
        if aged_out:
            pulse_logger.log(f"🕐 Cache fallback — aged out {aged_out} item(s) past 48h TTL, score recomputed: {score}")
        return data

    def _backfill_live_published_at(self, data):
        """Best-effort published_at backfill on the data fetch() is about to
        return, sourced from each item's own timestamp/date when missing.
        Folded in from the former geo_pin_ttl.py monkeypatch (previously
        wrapped fetch() externally, applied to whatever the original
        returned) — same effect, now applied natively at each return site."""
        if not isinstance(data, dict):
            return data
        for key in ('all_items', 'news_items'):
            for item in data.get(key) or []:
                if not (item.get('published_at') or '').strip():
                    item['published_at'] = (
                        (item.get('timestamp') or '').strip()
                        or (item.get('date') or '').strip()
                    )
        return data

    def fetch(self):
        try:
            # Runs every fetch() call, unconditionally — before the cache-age
            # branch below, so it applies regardless of which path executes
            # afterward (cache-hit, live fetch, or the pinned-only fallback).
            # Cheap: no network call, no Haiku call, just two local cache
            # reads and mechanical matching. See its own docstring for why
            # this is a genuinely separate pass from the ingest-time fold.
            self._reevaluate_pinned_ec_folds()

            existing = cache.load(self.cache_key)
            age_minutes = cache.get_age_minutes(self.cache_key)
            # TEMPORARY DIAGNOSTIC (shadow-mode silence investigation,
            # 2026-09-22) — remove once GEO_SHADOW_MODE log silence is
            # root-caused. Unconditional, every fetch() call: proves
            # whether the cache-hit branch below (which skips fetch_news()
            # entirely, and therefore skips background_classify() and the
            # shadow-mode call inside it) is the reason no 🔬 lines appear.
            pulse_logger.log(
                f"🔬 SHADOW DIAG — fetch() entry | age_minutes={age_minutes:.2f} | "
                f"cache_hit_branch={'YES (fetch_news() skipped)' if (existing and age_minutes < 3) else 'no (proceeding to fetch_news())'}"
            )
            if existing and age_minutes < 3:
                pulse_logger.log("↺ Geopolitical — using cache (TheNewsAPI refresh every 3min)")
                return self._backfill_live_published_at(self._refresh_cached_data(existing['data']))

            items = self.fetch_news()

            if not items:
                pulse_logger.log("↺ Geopolitical — fetch empty, using last cache")
                if existing:
                    self._reclassify_cached_pending(existing['data'].get('all_items', existing['data'].get('news_items', [])))
                    existing['data']['status'] = 'cached'
                    return self._backfill_live_published_at(self._refresh_cached_data(existing['data']))
                pinned = self.load_pinned_stories()
                if pinned:
                    pulse_logger.log(f"⚠️ Geopolitical — News API unavailable and no cache, using {len(pinned)} pinned stories only", level="WARNING")
                    flags = self.identify_flags(pinned)
                    score = self.calculate_score(pinned, flags)
                    return self._backfill_live_published_at({
                        'pillar': 'geopolitical',
                        'timestamp': datetime.now(self.timezone).isoformat(),
                        'news_items': pinned[:10],
                        'all_items': pinned,
                        'active_flags': flags,
                        'total_items': len(pinned),
                        'pillar_score': score,
                        'status': 'pinned_only'
                    })
                return None

            flags = self.identify_flags(items)
            score = self.calculate_score(items, flags)

            result = {
                'pillar': 'geopolitical',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'news_items': items[:10],
                'all_items': items,
                'active_flags': flags,
                'total_items': len(items),
                'pillar_score': score,
                'status': 'live'
            }
            cache.save(self.cache_key, result)
            pulse_logger.log(f"✓ Geopolitical updated | {len(flags)} active flags | {len(items)} articles | Score: {score}")
            return self._backfill_live_published_at(result)

        except Exception as e:
            error_handler.handle(e, "Geopolitical")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['status'] = 'stale'
                return self._backfill_live_published_at(self._refresh_cached_data(cached['data']))
            return None

geopolitical_pipeline = GeopoliticalPipeline()
