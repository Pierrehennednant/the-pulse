import hashlib
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


# ── Geo event memory (7-day echo guard) + 14-day after-action cap ─────────
# Deterministic, code-side overrides applied AFTER Haiku proposes kind/tier
# and BEFORE anything persists kind (gemini_classifications.json,
# pinned_stories.json) or scores it (calculate_score()). Not a second
# classifier: no model call, just a small explicit normalizer that turns an
# article into an event key (actor|action|object|time_bucket) plus an
# event-date resolver. See GeopoliticalPipeline._apply_event_rules().
GEO_EVENT_MEMORY_FILE = "/data/geo_event_memory.json"
EVENT_MEMORY_TTL_DAYS = 7
AGE_CAP_DAYS = 14
EVENT_MAX_IDENTITIES = 25
_EVENT_TZ = pytz.timezone(TIMEZONE)

_EVENT_OUTLETS = (
    'bloomberg news', 'bloomberg', 'cnbc', 'reuters', 'the associated press', 'associated press',
    'ap news', 'ap', 'the wall street journal', 'wall street journal', 'wsj', 'financial times', 'ft',
    'marketwatch', 'fox business', 'politico', 'axios', 'the hill', 'cbs news', 'nbc news',
    'abc news', 'the washington post', 'washington post', 'the new york times', 'new york times',
    'nyt', 'yahoo finance', 'cnn', 'bbc news', 'bbc',
)
_EVENT_OUTLET_ALT = '|'.join(re.escape(o) for o in sorted(_EVENT_OUTLETS, key=len, reverse=True))
# " - CNBC", " | Bloomberg", " — Reuters" (optionally ".com") at the very end of a headline.
_EVENT_OUTLET_SUFFIX_RE = re.compile(
    r'(?:\s+[-\u2013\u2014]\s+|\s*\|\s*)(?:' + _EVENT_OUTLET_ALT + r')(?:\.com)?\s*$', re.I)
_EVENT_OUTLET_WORD_RE = re.compile(r'\b(?:' + _EVENT_OUTLET_ALT + r')(?:\.com)?\b')
_EVENT_HEADLINE_PREFIX_RE = re.compile(
    r'^(?:live updates?|live|breaking|exclusive|update|analysis)\s*:\s*'
    r'|^(?:(?:mon|tues|wednes|thurs|fri|satur|sun)day|'
    r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2}(?:,\s*\d{4})?|'
    r'\d{4}-\d{2}-\d{2})\s*[:\-\u2013\u2014|]\s*')

# Obvious synonyms only (spec): exports/shipments, rescission/claw back/
# pocket rescission, intelligence warns/intelligence assessment.
_EVENT_SYNONYMS = (
    (r'\bdanish defen[cs]e intelligence service\b', 'ddis'),
    (r'\b(?:crude|oil) shipments?\b', lambda m: m.group(0).split()[0] + ' exports'),
    (r'\bshipments?\b', 'exports'),
    (r'\bexport(?:ed|ing)\b', 'exports'),
    (r'\bpocket rescissions?\b', 'rescission'),
    (r'\bclaw(?:s|ed|ing)?[\s-]?backs?\b', 'rescission'),
    (r'\brescissions?\b|\brescind(?:s|ed|ing)?\b', 'rescission'),
    (r'\bintelligence (?:agency |service |officials? )?(?:warns|warned|warning|warnings|says|said|assessment|assesses|assessed|reports?|estimates?)\b',
     'intelligence assessment'),
    (r'\bthreat assessment\b', 'intelligence assessment'),
    (r'\bwar-time\b|\bwar time\b', 'wartime'),
    (r'\bair ?strikes?\b', 'strike'),
)

# Canonical actor -> surface forms (applied to normalized, lowercased text;
# "U.S."/"US" are rewritten to united_states case-sensitively beforehand).
_EVENT_ACTORS = (
    ('us', r'united_states|\bamerican?s?\b|\bwhite house\b|\bpentagon\b|\btrump(?:\'s)?\b|\bomb\b|\bstate department\b'),
    ('saudi', r'\bsaudis?\b|\briyadh\b|\baramco\b'),
    ('iran', r'\biran(?:ian)?s?\b|\btehran\b|\birgc\b'),
    ('russia', r'\brussian?s?\b|\bkremlin\b|\bmoscow\b|\bputin\b'),
    ('ukraine', r'\bukrain(?:e|ian)s?\b|\bkyiv\b|\bzelensk(?:y|yy)\b'),
    ('china', r'\bchin(?:a|ese)\b|\bbeijing\b|\bxi jinping\b'),
    ('israel', r'\bisraeli?s?\b|\bidf\b|\bnetanyahu\b'),
    ('denmark', r'\bdenmark\b|\bdanish\b|\bddis\b'),
    ('nato', r'\bnato\b'),
    ('opec', r'\bopec\b'),
    ('cuba', r'\bcuban?s?\b|\bhavana\b'),
    ('venezuela', r'\bvenezuelan?s?\b|\bcaracas\b|\bmaduro\b'),
    ('kuwait', r'\bkuwait(?:i|is)?\b'),
    ('iraq', r'\biraqi?s?\b|\bbaghdad\b'),
    ('syria', r'\bsyrian?s?\b|\bdamascus\b'),
    ('lebanon', r'\blebanon\b|\blebanese\b|\bhezbollah\b'),
    ('yemen', r'\byemen(?:i)?\b|\bhouthis?\b'),
    ('qatar', r'\bqatar(?:i)?\b'),
    ('uae', r'\buae\b|\bemirat(?:es|i)\b'),
    ('taiwan', r'\btaiwan(?:ese)?\b'),
    ('north_korea', r'\bnorth korean?\b|\bpyongyang\b'),
    ('japan', r'\bjapan(?:ese)?\b'),
    ('india', r'\bindian?\b'),
    ('pakistan', r'\bpakistan(?:i)?\b'),
    ('eu', r'\beuropean union\b|\beu\b|\bbrussels\b'),
    ('uk', r'united_kingdom|\bbritain\b|\bbritish\b'),
    ('germany', r'\bgerman(?:y)?\b|\bberlin\b'),
    ('france', r'\bfrance\b|\bfrench\b|\bparis\b'),
    ('poland', r'\bpoland\b|\bpolish\b|\bwarsaw\b'),
    ('mexico', r'\bmexic(?:o|an)\b'),
    ('canada', r'\bcanad(?:a|ian)\b'),
    ('turkey', r'\bturkey\b|\bturkish\b|\bankara\b'),
)
_EVENT_ACTOR_RES = tuple((name, re.compile(pat)) for name, pat in _EVENT_ACTORS)
_EVENT_MONTHS = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
_EVENT_DATE_RE = re.compile(
    r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|'
    r'oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?\s+(\d{1,2})(?:st|nd|rd|th)?\b(?:,?\s+(\d{4})\b)?'
    r'|\b(\d{4})-(\d{2})-(\d{2})\b')
_EVENT_ACTION_NOUN_RE = re.compile(
    r'\b(?:strikes?|attacks?|bombings?|raids?|drones?|missiles?|killed|reports?|assessments?|orders?|deals?|'
    r'agreements?|announcements?|announced|rescission|invasion|incidents?|explosions?|shootings?|sanctions|'
    r'tariffs|memo|ceasefire|offensive|assault|ambush|ddis|signed|launched)\b')
_EVENT_DEADLINE_BEFORE_RE = re.compile(
    r'\b(?:by|until|till|through|thru|no later than|deadline(?: of)?|due(?: on| by)?|scheduled for|'
    r'expected (?:on|by)|slated for|starting|beginning|next)\s*(?:the\s+)?$')
_EVENT_WEEKDAYS = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
_EVENT_WEEKDAY_RE = re.compile(r'\b(' + '|'.join(_EVENT_WEEKDAYS) + r')\b')
_EVENT_RELATIVE_OLD_RE = re.compile(
    r"\blast (?:spring|summer|fall|autumn|winter|year)'?s?\s+(?:[\w$.\-]+\s+){0,3}?"
    r"(?:strikes?|attacks?|bombing|raid|invasion|rescission|deal|agreement|order|memo|incident|shooting|explosion|crash)\b"
    r"|\b(?:\w+|\d+) (?:months?|years?) (?:after|since) (?:the|a|an|that) (?:[\w\-]+\s+){0,2}?"
    r"(?:strike|attack|bombing|raid|invasion|incident|shooting|explosion|crash)\b"
    r"|\b(?:investigation|probe|inquiry|review) into (?:last|the) (?:spring|summer|fall|autumn|winter|year)'?s?\s+"
    r"(?:[\w\-]+\s+){0,2}?(?:strike|attack|bombing|raid|incident)\b"
    r"|\banniversary of (?:the )?(?:[\w\-]+\s+){0,2}?(?:strike|attack|bombing|raid|invasion|war)\b")
_EVENT_WHEN_NOW = r'(?:today|tonight|overnight|this morning|this week|on (?:monday|tuesday|wednesday|thursday|friday|saturday|sunday))'
_EVENT_NEW_VERB = r'(?:launched|launches|struck|strikes|ordered|orders|signed|signs|announced|announces|imposed|imposes|deployed|deploys|fired|attacked|bombed)'
_EVENT_NEW_ACTION_RE = re.compile(
    r'\bnew (?:wave of |round of |series of )?(?:strikes?|attacks?|drone strikes?|missile strikes?|deployment orders?|'
    r'deployments?|orders?|sanctions|restrictions|tariffs|package|aid package|offensive|assault)\b'
    r'|\b' + _EVENT_WHEN_NOW + r'\b[^.;]{0,60}\b' + _EVENT_NEW_VERB + r'\b'
    r'|\b' + _EVENT_NEW_VERB + r'\b[^.;]{0,40}\b' + _EVENT_WHEN_NOW + r'\b'
    r'|\b(?:is|are) (?:now )?(?:canceling|cancelling|freezing|deploying|imposing|launching|striking|ordering|blocking|bombing)\b')
_EVENT_NOW_CUE_RE = re.compile(
    r'\b(?:today|tonight|overnight|this (?:week|morning|afternoon|evening|month)|yesterday|now|currently|latest|'
    r'new|breaking|just|announced|announces|says|said|warns|warned|reports?|reported|hits?|surged?|surges|'
    r'is|are|has|have|will|' + '|'.join(_EVENT_WEEKDAYS) + r')\b')
_EVENT_AMOUNT_RE = re.compile(r'\$\s?(\d+(?:\.\d+)?)\s?(trillion|billion|million|tn|bn|mn|t|b|m)\b')
_EVENT_STOPWORDS = frozenset((
    'the', 'a', 'an', 'of', 'to', 'in', 'on', 'for', 'and', 'or', 'as', 'at', 'by', 'with', 'from', 'is',
    'are', 'was', 'were', 'be', 'been', 'its', 'it', 'that', 'this', 'after', 'over', 'says', 'say', 'said',
    'amid', 'into', 'than', 'but', 'not', 'has', 'have', 'had', 'will', 'would', 'could', 'can', 'may',
    'his', 'her', 'their', 'our', 'we', 'he', 'she', 'they', 'sources', 'report', 'reports',
))
_EVENT_ABBREV_END_RE = re.compile(
    r'\b(?:jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec|mr|mrs|ms|dr|gen|sen|rep|gov|lt|col|st|no|vs|inc|corp|co|jr|sr|u\.s|u\.k)\.$',
    re.I)


def _event_headline_norm(headline):
    """Identity form of a headline: lowercase, outlet suffix stripped
    (" - CNBC", " | Bloomberg"), whitespace collapsed."""
    h = ' '.join((headline or '').split()).lower()
    prev = None
    while prev != h:
        prev = h
        h = _EVENT_OUTLET_SUFFIX_RE.sub('', h).strip()
    return h


def _event_norm_text(text):
    """Key-builder normalization: US forms -> united_states (case-sensitive,
    so the pronoun "us" never becomes an actor), lowercase, curly quotes
    flattened, outlet names stripped, obvious synonyms mapped."""
    t = text or ''
    t = re.sub(r'\bU\.S\.(?:A\.)?|\bUSA\b|\bUS\b', ' united_states ', t)
    t = re.sub(r'\bU\.K\.|\bUK\b', ' united_kingdom ', t)
    t = (t.replace('\u2019', "'").replace('\u2018', "'")
          .replace('\u201c', '"').replace('\u201d', '"'))
    t = t.lower()
    t = _EVENT_OUTLET_WORD_RE.sub(' ', t)
    for pat, rep in _EVENT_SYNONYMS:
        t = re.sub(pat, rep, t)
    return ' '.join(t.split())


def _event_first_sentence(text):
    """First sentence of a summary/description (abbreviation-aware, so
    "Sept. 24" or "Gen. Barnes" don't end it)."""
    text = ' '.join((text or '').split())
    if not text:
        return ''
    parts = re.split(r'(?<=[.!?;])\s+', text)
    out = parts[0]
    i = 1
    while i < len(parts) and _EVENT_ABBREV_END_RE.search(out):
        out = out + ' ' + parts[i]
        i += 1
    return out


def _event_published_date(date_str='', *timestamps):
    """Article publish date as an ET calendar date, or None."""
    ds = (date_str or '').strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}$', ds):
        try:
            return datetime.strptime(ds, '%Y-%m-%d').date()
        except Exception:
            pass
    for ts in timestamps:
        ts = (ts or '').strip()
        if not ts:
            continue
        try:
            dt = dateutil_parser.parse(ts, default=datetime.now(timezone.utc),
                                       tzinfos={"EST": -18000, "EDT": -14400})
            # Display strings ("Sep 25, 01:43 PM EST") are already ET wall time.
            if re.search(r'\b(?:EST|EDT)\b', ts) or dt.tzinfo is None:
                return dt.date()
            return dt.astimezone(_EVENT_TZ).date()
        except Exception:
            continue
    return None


def _event_parse_iso(ts):
    try:
        dt = datetime.fromisoformat(str(ts).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _event_resolve_md(month, day, year, ref):
    """Calendar date for a month/day (year optional, inferred as the
    occurrence nearest to `ref`, the article date)."""
    try:
        if year:
            return datetime(int(year), month, day).date()
        # Nearest plausible date to the article date: "starting Oct. 5" in a
        # Oct. 1 article is 2026-10-05 (a future start, which
        # _event_action_dates() then ignores), never 2025-10-05.
        cands = []
        for y in (ref.year - 1, ref.year, ref.year + 1):
            try:
                cands.append(datetime(y, month, day).date())
            except ValueError:
                continue
        return min(cands, key=lambda d: abs((d - ref).days)) if cands else None
    except Exception:
        return None


def _event_action_dates(text, ref):
    """Explicit calendar dates in `text` that read as an ACTION date: an
    action noun within ~50 chars, not a deadline ("by Sept. 25"), not in
    the future relative to the article. Returned in text order."""
    out = []
    for m in _EVENT_DATE_RE.finditer(text or ''):
        if m.group(1):
            d = _event_resolve_md(_EVENT_MONTHS[m.group(1)[:3]], int(m.group(2)), m.group(3), ref)
        else:
            try:
                d = datetime(int(m.group(4)), int(m.group(5)), int(m.group(6))).date()
            except Exception:
                d = None
        if d is None or d > ref + timedelta(days=1):
            continue
        if _EVENT_DEADLINE_BEFORE_RE.search(text[max(0, m.start() - 30):m.start()]):
            continue
        window = text[max(0, m.start() - 50):m.end() + 50]
        if not _EVENT_ACTION_NOUN_RE.search(window):
            continue
        out.append(d)
    return out


def _event_age_texts(ctx):
    """(subject, full) normalized texts for Rule B. Haiku's reason /
    tier_reasoning are commentary about the article, not the article, so
    they're excluded from event-date resolution."""
    subject = ' '.join([ctx.get('headline') or '',
                        _event_first_sentence(ctx.get('summary')),
                        _event_first_sentence(ctx.get('description'))])
    full = ' '.join([ctx.get('headline') or '', ctx.get('summary') or '', ctx.get('description') or ''])
    return _event_norm_text(subject), _event_norm_text(full)


def _event_has_new_action(full, ref, today, old_date=None):
    """Rule B exception: the article ALSO reports a new action now, dated
    within AGE_CAP_DAYS. A named general, leaked memo, survivors or "how
    the attack unfolded" match none of these."""
    for d in _event_action_dates(full, ref):
        if d != old_date and 0 <= (today - d).days <= AGE_CAP_DAYS:
            return True
    return bool(_EVENT_NEW_ACTION_RE.search(full))


def _event_resolve_age(ctx, today):
    """Rule B event-date resolution. Returns dict(status=old|recent|none,
    event_date=str, age_days=int|None, exception=bool)."""
    pub = ctx.get('published_date')
    ref = pub or today
    subject, full = _event_age_texts(ctx)
    dates = _event_action_dates(subject, ref) or _event_action_dates(full, ref)
    if dates:
        ev = dates[0]
        age = (today - ev).days
        if age > AGE_CAP_DAYS:
            if _event_has_new_action(full, ref, today, old_date=ev):
                return {'status': 'recent', 'event_date': ev.isoformat(), 'age_days': age, 'exception': True}
            return {'status': 'old', 'event_date': ev.isoformat(), 'age_days': age, 'exception': False}
        return {'status': 'recent', 'event_date': ev.isoformat(), 'age_days': age, 'exception': False}
    m = _EVENT_RELATIVE_OLD_RE.search(full)
    if m:
        if _event_has_new_action(full, ref, today):
            return {'status': 'recent', 'event_date': 'relative', 'age_days': None, 'exception': True}
        return {'status': 'old', 'event_date': f'relative:"{m.group(0)}"', 'age_days': None, 'exception': False}
    if pub and _EVENT_NOW_CUE_RE.search(full):
        return {'status': 'recent', 'event_date': pub.isoformat(), 'age_days': (today - pub).days, 'exception': False}
    return {'status': 'none', 'event_date': None, 'age_days': None, 'exception': False}


def _event_actor_hits(text):
    hits = []
    for name, rx in _EVENT_ACTOR_RES:
        m = rx.search(text)
        if m:
            hits.append((m.start(), name))
    hits.sort()
    return [n for _, n in hits]


def _event_amount(text):
    m = _EVENT_AMOUNT_RE.search(text or '')
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2)[0]
    millions = val * {'t': 1_000_000, 'b': 1000, 'm': 1}[unit]
    if millions >= 1000:
        return f"{round(millions / 1000, 2):g}b"
    return f"{round(millions, 1):g}m"


def _event_amount_millions(obj):
    m = re.match(r'^(\d+(?:\.\d+)?)(m|b)$', obj or '')
    if not m:
        return None
    return float(m.group(1)) * (1000 if m.group(2) == 'b' else 1)


# ── Event identity (one event, one identity) ─────────────────────────────
# Every scoring-eligible item gets an identity — there is no action
# whitelist, so every pair of items is comparable. An identity records:
#   KIND    — the core action of the item (attack, call, talks, deal, rate
#             move, tariff, sanction, output decision, pause...), read from
#             the headline + lead sentence + first summary sentence, with
#             negated clauses ("no ceasefire was announced") removed. An item
#             with no action kind is a remark/commentary.
#   ACTORS  — countries/blocs anywhere in the text, and those in the headline.
#   SPEAKER — a named person/institution whose remark IS the headline
#             ("Fed's Kashkari says ...", "Trump told Putin ...").
#   OBJECT  — what the action is about: object concepts (energy grid, oil,
#             inflation, servers ...), named proper nouns and named sites,
#             with the speaker's own name/title removed.
# The CONCEPT lexicon is synonym classes, not event patterns: it never
# decides WHETHER an item has an identity, only how words are spelled for
# comparison ("hits" / "launches massive strikes" / "pounds" / "attack on"
# are all #attack).
# Same event = ALL of:
#   1. same actors   — actor sets overlap when both name actors; headline
#                      actor sets overlap when both headlines name actors;
#                      no actor in the new headline that the row never names.
#   2. same core event — the KIND sets intersect (or both are remarks).
#   3. same speaker  — a headline remark matches only a row in which that
#                      named speaker appears; two remarks by different named
#                      speakers are different events.
#   4. same object   — the OBJECT sets intersect (when both have objects;
#                      otherwise two or more shared actors carry the object).
#   5. score         — weighted token agreement (shared weight / smaller
#                      identity) >= EVENT_IDENTITY_MIN_OVERLAP.
# A match is then checked for a NEW FACT (stage escalation, newly disclosed
# amount, a policy action on a new object, or — for an act of the same kind
# on a different day — a new named site/place or a new-act/new-round cue) — a new fact is its own
# event (first_print), never a fold.
EVENT_CORE_WEIGHT = 3
EVENT_IDENTITY_MIN_OVERLAP = 0.42
EVENT_SAME_ACTION_DAY_WINDOW = 1  # kinetic acts within ±1 day = same barrage/campaign day

_EVENT_CONCEPTS = (
    # Moving forces is its own act, not an attack ("third carrier strike
    # group", "sends 2,000 Marines").
    ('deploy', r'\b(?:(?:re)?deploy(?:s|ed|ing|ments?)?|dispatch(?:es|ed|ing)?|'
               r'(?:aircraft )?carrier (?:strike )?groups?|aircraft carriers?|(?:strike|amphibious ready) groups?|'
               r'military build-?up|(?:send|sends|sending|sent|order(?:s|ed)?)\s+(?:[\w,-]+\s+){0,3}?'
               r'(?:troops|warships|marines|soldiers|destroyers|bombers|fighter jets|carriers?))\b'),
    ('attack', r'\b(?:strikes?(?!\s+(?:groups?|forces?|fighters?|package))|struck|striking|'
               r'(?:hits?|hitting)(?!\s+(?:an?\s+)?(?:\$|\d|(?:(?:new|fresh|record|all-time|multi-year)\s+)*(?:highs?|lows?|records?)\b))|'
               r'pound(?:s|ed|ing)?|attack(?:s|ed|ing)?|'
               r'barrages?|bombard\w*|shell(?:s|ed|ing)|bomb(?:s|ed|ing|ings)?|missiles?|drones?|'
               r'salvos?|assaults?|offensive)\b'),
    ('energy', r'\b(?:energy|power|grid|electric\w*|plants?|substations?|thermal|utilit(?:y|ies))\b'),
    ('outage', r'\b(?:outages?|blackouts?|shutoffs?|power cuts?|emergency cuts?)\b'),
    ('ceasefire', r'\b(?:ceasefires?|cease-fires?|truces?|pauses?|halt(?:s|ed)?|armistice)\b'),
    ('call', r'\b(?:phone calls?|calls? with|by phone|phone|spoke (?:with|to|by)|talked (?:with|to|by)|readout)\b'),
    ('talks', r'\b(?:talks?|negotiat\w*|discuss\w*|weigh(?:s|ing)?(?!\s+on\b)|mediat\w*|breakthrough)\b'),
    ('deal', r'\b(?:deals?|agreements?|accords?|pacts?|framework)\b'),
    ('reopen', r'\b(?:reopen\w*|re-open\w*)\b'),
    ('output', r'\b(?:output|production|barrels (?:a|per) day|bpd)\b'),
    ('oil', r'\b(?:oil|crude|fuel|gas(?:oline)?|petrol|diesel)\b'),
    ('price', r'\b(?:prices?|costs?)\b'),
    ('fed', r'\b(?:fed|federal reserve|fomc|central bank)\b'),
    ('ratemove', r'\b(?:rate (?:hikes?|increases?|cuts?|rises?)|hikes?|hiked|'
                 r'(?:rais|cut|lower|hik)(?:e|es|ed|ing|s)? (?:its |the |their )?(?:benchmark |key |policy )?'
                 r'(?:interest )?rates?|basis points?|quarter(?:-| )point|half(?:-| )point)\b'),
    ('rate', r'\b(?:interest rates?|rates?)\b'),
    ('inflation', r'\b(?:inflation\w*|cpi)\b'),
    ('tariff', r'\b(?:tariffs?|levy|levies|import dut(?:y|ies))\b'),
    ('sanction', r'\b(?:sanction\w*|designat\w*|blacklist\w*)\b'),
    ('tanker', r'\b(?:tankers?|vessels?|ships?)\b'),
    ('jobs', r'\b(?:payrolls?|jobs?|employment|labor market)\b'),
    ('voter', r'\b(?:voters?|electorate)\b'),
    ('company', r'\b(?:companies|company|firms?|businesses|corporate)\b'),
    ('server', r'\b(?:servers?|data centers?|data centres?)\b'),
)
_EVENT_CONCEPT_RES = tuple((n, re.compile(p)) for n, p in _EVENT_CONCEPTS)
# Which concepts name the core ACTION (kind) vs what it is ABOUT (object).
_EVENT_KIND_CONCEPTS = frozenset({'attack', 'ceasefire', 'call', 'talks', 'deal', 'output', 'ratemove',
                                  'tariff', 'sanction', 'deploy'})
# A coercive act named only as a threat, warning or possibility ("warns of
# new Iran strikes", "could impose tariffs") is kind 'threat', not the act.
_EVENT_THREAT_KINDS = frozenset({'attack', 'tariff', 'sanction'})
_EVENT_THREAT_RE = re.compile(
    r"\b(?:warn(?:s|ed|ing)?|threat(?:s|en|ens|ened|ening)?|vow(?:s|ed)?|signal(?:s|ed|ing|led|ling)?|"
    r"could|may(?!\s+\d)|might|would|plans? to|planning to|intends? to|consider(?:s|ing)?|"
    r"ready to|possible|potential)\b")
_EVENT_OBJECT_CONCEPTS = frozenset({'energy', 'outage', 'reopen', 'oil', 'price', 'fed', 'rate', 'inflation',
                                    'tanker', 'jobs', 'server'})
_EVENT_IDENTITY_STOP = frozenset(_EVENT_STOPWORDS | {
    'officials', 'official', 'according', 'statement', 'statements', 'monday', 'tuesday', 'wednesday',
    'thursday', 'friday', 'saturday', 'sunday', 'january', 'february', 'march', 'april', 'june', 'july',
    'august', 'september', 'sept', 'october', 'november', 'december', 'jan', 'feb', 'mar', 'apr', 'jun',
    'jul', 'aug', 'sep', 'oct', 'nov', 'dec', 'today', 'tonight', 'overnight', 'yesterday', 'week',
    'weeks', 'month', 'months', 'year', 'years', 'morning', 'evening', 'night', 'day', 'days', 'new',
    'also', 'more', 'about', 'before', 'since', 'last', 'first', 'people', 'familiar', 'matter', 'which',
    'who', 'what', 'when', 'while', 'where', 'there', 'these', 'those', 'some', 'any', 'all', 'one',
    'two', 'three', 'been', 'being', 'did', 'does', 'told', 'tell', 'telling', 'earlier', 'later',
    'still', 'just', 'only', 'other', 'such', 'very', 'should', 'might', 'must', 'per', 'via', 'among',
    'across', 'around', 'ahead', 'during', 'under', 'between', 'both', 'each', 'few', 'most', 'much',
    'own', 'same', 'then', 'too', 'now', 'here', 'how', 'why', 'out', 'off', 'again', 'once', 'no',
    'est', 'edt', 'gmt', 'utc', 'recap', 'article', 'reported', 'reporting', 'says', 'saying',
    'analysts', 'analyst', 'note', 'interview', 'program', 'show', 'continue', 'continued',
    'expected', 'major', 'large', 'big', 'biggest', 'massive', 'several', 'many', 'including',
    'repeated', 'repeats', 'restates', 'restated', 'reports', 'its', 'it', 'them', 'him', 'she',
    'did', 'make', 'made', 'making', 'take', 'takes', 'took', 'taken', 'give', 'gave', 'like', 'way',
    'ways', 'time', 'times', 'next', 'previous', 'previously', 'already', 'set', 'see', 'seen',
})
# Stage ladder for "new status/stage" facts: 1 = talk/consider/close,
# 2 = announced/disclosed, 3 = signed/agreed/applied/in force/reopened/
# confirmed. Counted only outside negated, hedged or restating clauses.
_EVENT_STAGE3_RE = re.compile(
    r'\b(?:sign(?:s|ed|ing)?|agreed|agrees|appl(?:ied|ies)|impos(?:ed|es)|takes? effect|took effect|'
    r'effective (?:today|immediately)|reopen(?:ed|s)|confirm(?:ed|s)|ratif(?:ied|ies)|enact(?:ed|s)|'
    r'approved|approves)\b')
_EVENT_STAGE2_RE = re.compile(r'\b(?:announc(?:e|es|ed|ing)|disclos(?:e|es|ed)|unveil(?:s|ed)|issued)\b')
_EVENT_STAGE1_RE = re.compile(
    r'\b(?:talks?|negotiat\w*|discuss\w*|weigh(?:s|ing)?|consider\w*|close to|getting close|nearing|'
    r'proposal|proposed|possible|potential)\b')
_EVENT_POLICY_NOUN_RE = re.compile(
    r'\b(?:deals?|agreements?|accords?|pacts?|treat(?:y|ies)|framework|ceasefires?|cease-fires?|truces?|'
    r'pauses?|halt|tariffs?|levy|levies|dut(?:y|ies)|sanctions?|sanctions list|bans?|embargo|restrictions?|'
    r'export controls?|orders?|decrees?|law|bill|reopening|strait|blockade|curfew|quotas?)\b')
_EVENT_NEGATION_RE = re.compile(r"\b(?:no|not|never|without|nor|yet to|denied|den(?:y|ies))\b|n't\b")
_EVENT_HEDGE_RE = re.compile(r"\b(?:could|may|might|would|expected to|poised to|likely to|set to|plans? to)\b")
_EVENT_RESTATE_RE = re.compile(
    r'\b(?:already|earlier|previously|yesterday|last week|days? after|a day after|restat\w*|recap\w*|'
    r'repeat(?:s|ed)?|reiterat\w*|same)\b')
_EVENT_SITE_RE = re.compile(
    r"\b([A-Z][a-z]+(?:[-'][A-Za-z]+)?)\s+(?:(?:thermal|electrical|nuclear|hydroelectric|hydro|power|oil|"
    r"gas|coal|heat|combined)\s+){0,2}(?:power plant|power station|plant|substation|station|refinery|"
    r"terminal|port|air base|airbase|base|airport|dam|depot|facility|oilfield|field|pipeline|bridge)\b")
_EVENT_SITE_EXCLUDE = frozenset({'the', 'a', 'an', 'this', 'that', 'its', 'power', 'thermal', 'nuclear',
                                 'electrical', 'new', 'one', 'another', 'key', 'main', 'major', 'local'})
# Identity-only cue that an action is a NEW ROUND of the same kind of act
# ("a new set of tankers", "another round of strikes", "not previously
# named"). Kept separate from _EVENT_NEW_ACTION_RE, which Rule B uses.
_EVENT_NEW_ROUND_RE = re.compile(
    r'\bnew (?:set|group|batch|tranche|round|wave|series|list|package|slate) of\b|'
    r'\banother (?:set|group|batch|tranche|round|wave|series)\b|\badditional (?:sanctions|tariffs|strikes|designations)\b|'
    r'\bnot (?:previously|already|yet) (?:been )?(?:named|listed|designated|sanctioned|targeted|hit)\b')
# Named speaker: a capitalized name run directly before a speech verb
# ("Minneapolis Fed President Neel Kashkari said", "Fed's Kashkari says",
# "Trump told"). The last word of the run is the speaker; the whole run is
# the speaker's title and is not part of the item's OBJECT.
_EVENT_SPEECH_VERBS = (r'(?i:says|said|say|warns|warned|warn|told|tells|repeated|repeats|reiterated|reiterates|'
                       r'argued|argues|added|adds|noted|notes|insisted|insists|claims|claimed|stated|states)')
_EVENT_SPEAKER_RE = re.compile(
    r"((?:[A-Z][\w'\u2019\-]*\.?\s+){0,5}?([A-Z][\w\-]+))(?:'s|\u2019s)?\s+" + _EVENT_SPEECH_VERBS + r"\b")
# Unnamed / generic speakers are commentary, not a named source.
_EVENT_GENERIC_SPEAKERS = frozenset({
    'analysts', 'analyst', 'economists', 'economist', 'officials', 'official', 'mediators', 'investors',
    'traders', 'executives', 'experts', 'sources', 'source', 'diplomats', 'lawmakers', 'senator', 'senators',
    'report', 'reports', 'poll', 'survey', 'study', 'data', 'markets', 'banks', 'bank', 'he', 'she', 'they',
    'it', 'who', 'this', 'that', 'the', 'a', 'an', 'statement', 'readout', 'spokesman', 'spokeswoman',
    'spokesperson', 'governor', 'people', 'companies', 'company', 'residents', 'witnesses', 'police', 'media',
    'note', 'article', 'administration', 'government', 'ministry', 'department',
})


def _event_clauses(text):
    return [c for c in re.split(r'[.;:!?]+\s+|,\s*but\s+|\s+but\s+', text or '') if c.strip()]


def _event_stage(text_norm):
    """Highest stage asserted in the text, ignoring negated ("no agreement
    has been signed"), hedged ("could be signed") and restating ("announced
    yesterday", "already announced") clauses, and stage verbs with no
    deal/policy noun in their clause."""
    best = 0
    for clause in _event_clauses(text_norm):
        if _EVENT_RESTATE_RE.search(clause):
            continue
        neg = _EVENT_NEGATION_RE.search(clause)
        live = clause[:neg.start()] if neg else clause
        if not _EVENT_POLICY_NOUN_RE.search(live):
            # Stage verbs only mark a new status of a deal/policy ("signed an
            # agreement", "tariff took effect"), not consequences of a
            # kinetic event ("power cuts were imposed").
            if _EVENT_STAGE1_RE.search(clause):
                best = max(best, 1)
            continue
        hedged = bool(_EVENT_HEDGE_RE.search(live))
        if _EVENT_STAGE3_RE.search(live):
            best = max(best, 1 if hedged else 3)
        elif _EVENT_STAGE2_RE.search(live):
            best = max(best, 1 if hedged else 2)
        elif _EVENT_STAGE1_RE.search(clause):
            best = max(best, 1)
    return best


def _event_new_amounts(text_norm):
    """Dollar amounts asserted outside restating clauses ("the $5B deal
    announced yesterday" restates; "agreed to buy $5B" discloses)."""
    out = set()
    for clause in _event_clauses(text_norm):
        if _EVENT_RESTATE_RE.search(clause):
            continue
        for m in _EVENT_AMOUNT_RE.finditer(clause):
            a = _event_amount(m.group(0))
            if a:
                out.add(a)
    return out


def _event_all_amounts(text_norm):
    out = set()
    for m in _EVENT_AMOUNT_RE.finditer(text_norm or ''):
        a = _event_amount(m.group(0))
        if a:
            out.add(a)
    return out


def _event_sites(raw_text):
    """Named sites ("Trypilska thermal power plant", "Novokyivska
    electrical substation") from the article's own sentence-case text."""
    out = set()
    for m in _EVENT_SITE_RE.finditer(raw_text or ''):
        w = m.group(1).lower()
        if w in _EVENT_SITE_EXCLUDE or _event_actor_hits(w):
            continue
        out.add(w)
    return out


_EVENT_PROPER_SKIP = frozenset({
    'president', 'prime', 'minister', 'ministry', 'mr', 'mrs', 'ms', 'dr', 'gen', 'sen', 'senator', 'rep',
    'gov', 'governor', 'mayor', 'chair', 'chairman', 'secretary', 'department', 'officials', 'official',
    'the', 'a', 'an', 'in', 'on', 'at', 'of', 'and', 'but', 'his', 'her', 'its', 'their', 'no', 'not',
    'markets', 'investors', 'traders', 'stocks', 'shares', 'economists', 'executives', 'voters', 'companies',
})


def _event_proper_nouns(raw_text, headline=''):
    """Lowercased proper nouns from the article's sentence-case text (and
    the headline unless it is Title Case). Actor names and concept words
    are left to their own tokens."""
    out = set()
    chunks = [raw_text or '']
    if headline and not headline.istitle():
        chunks.append(headline)
    seen_caps = {}
    cands = []
    for chunk in chunks:
        for sent in re.split(r'(?<=[.!?])\s+', chunk):
            words = re.findall(r"[A-Za-z][A-Za-z'\-]+", sent)
            for pos, w in enumerate(words):
                if w[0].isupper():
                    k = w.lower().replace("'s", '').strip("'-")
                    seen_caps[k] = seen_caps.get(k, 0) + 1
                    cands.append((pos, w))
    for pos, w in cands:
        # Mid-sentence capital, or a sentence-initial word capitalized at
        # least twice in the text ("Apple said ... Apple's deal").
        if w.isupper() and len(w) <= 4:
            continue
        key = w.lower().replace("'s", '').strip("'-")
        if pos == 0 and seen_caps.get(key, 0) < 2:
            continue
        if (len(key) < 3 or key in _EVENT_PROPER_SKIP or key in _EVENT_IDENTITY_STOP
                or _event_actor_hits(key) or any(rx.search(key) for _, rx in _EVENT_CONCEPT_RES)):
            continue
        out.add(key)
    return out


def _event_identity_tokens(text_norm, proper=()):
    toks = set()
    proper = set(proper)
    t = text_norm
    for name, rx in _EVENT_CONCEPT_RES:
        if rx.search(t):
            toks.add('#' + name)
            t = rx.sub(' ', t)
    for name, rx in _EVENT_ACTOR_RES:
        if rx.search(t):
            toks.add('@' + name)
            t = rx.sub(' ', t)
    for w in re.findall(r"[a-z][a-z'\-]+", t):
        w = w.strip("'-")
        if w.endswith("'s"):
            w = w[:-2]
        if len(w) < 3 or w in _EVENT_IDENTITY_STOP:
            continue
        if w in proper:
            toks.add('!' + w)
            continue
        if len(w) > 4 and w.endswith('s') and not w.endswith('ss'):
            w = w[:-1]
        toks.add(w)
    return toks


def _event_token_weight(tok):
    return EVENT_CORE_WEIGHT if tok[:1] in ('#', '@', '!') else 1


def _event_kinds(text_norm):
    """Core-action concepts asserted in the text. Negated parts of a clause
    ("no ceasefire was announced") do not count; inside one clause a phone
    call absorbs its own "discussed" (a call is not a round of talks)."""
    kinds = set()
    for clause in _event_clauses(text_norm):
        neg = _EVENT_NEGATION_RE.search(clause)
        live = clause[:neg.start()] if neg else clause
        found = set()
        t = live
        for name, rx in _EVENT_CONCEPT_RES:
            m = rx.search(t)
            if m:
                if name in _EVENT_KIND_CONCEPTS:
                    threat = _EVENT_THREAT_RE.search(t, 0, m.start())
                    if name in _EVENT_THREAT_KINDS and threat:
                        found.add('threat')
                    else:
                        found.add(name)
                t = rx.sub(' ', t)
        if 'call' in found:
            found.discard('talks')
        kinds |= found
    return kinds


def _event_speakers(raw_text):
    """[(speaker, title_words)] for named speakers in sentence-case text."""
    out = []
    for m in _EVENT_SPEAKER_RE.finditer(raw_text or ''):
        name = m.group(2).lower().replace("'s", '').strip("'-")
        if name in _EVENT_GENERIC_SPEAKERS or name in _EVENT_IDENTITY_STOP or len(name) < 3:
            continue
        title = [w.lower().replace("'s", '').replace('\u2019s', '').strip(".'-")
                 for w in m.group(1).split()]
        out.append((name, [w for w in title if w]))
    return out


def _event_headline_speaker(headline):
    """Named speaker whose remark IS the headline ("Fed's Kashkari says
    ...", "Trump told Putin ..."): the speech verb sits within the first
    few words. A trailing attribution (", Kremlin says") is not a remark
    headline."""
    for m in _EVENT_SPEAKER_RE.finditer(headline or ''):
        if len(headline[:m.end()].split()) > 5:
            break
        name = m.group(2).lower().replace("'s", '').strip("'-")
        if name in _EVENT_GENERIC_SPEAKERS or name in _EVENT_IDENTITY_STOP or len(name) < 3:
            continue
        return name
    return None


def _event_names(raw_text):
    """Every capitalized word in the text (lowercased) — who is named at all."""
    return {w.lower().replace("'s", '').replace('\u2019s', '').strip("'-")
            for w in re.findall(r"\b[A-Z][\w'\u2019\-]+", raw_text or '')}


def _event_identity_date(ctx, today):
    """Day the item's OWN action happened: an explicit action date in the
    headline/first sentence, else 'yesterday', else a weekday named in the
    article, else the publish date. Dates further down the body ("different
    from the Sept. 30 attack") are references, not this item's action day."""
    pub = ctx.get('published_date')
    ref = pub or today
    desc = ctx.get('description') or ''
    subject = _event_norm_text(' '.join([ctx.get('headline') or '', _event_first_sentence(desc)]))
    full = _event_norm_text(' '.join([ctx.get('headline') or '', desc]))
    # "the first strikes since Sept. 1" names a reference day, not this act's.
    subject = re.sub(r'\b(?:since|until|till)\s+(?:the\s+)?[a-z]+\.?\s+\d{1,2}(?:st|nd|rd|th)?\b', ' ', subject)
    dates = _event_action_dates(subject, ref)
    if dates:
        return dates[0]
    if re.search(r'\byesterday\b', subject):
        return ref - timedelta(days=1)
    wm = _EVENT_WEEKDAY_RE.search(subject) or _EVENT_WEEKDAY_RE.search(full)
    if wm:
        back = (ref.weekday() - _EVENT_WEEKDAYS.index(wm.group(1))) % 7
        return ref - timedelta(days=back)
    return ref


def _event_word_stems(words):
    return sorted({w[:5] for w in words if len(w) >= 5})


def _event_identity(ctx, today):
    """Identity record for one item (stored on each memory identity)."""
    headline = _EVENT_HEADLINE_PREFIX_RE.sub('', ' '.join((ctx.get('headline') or '').split()))
    desc = ctx.get('description') or ''
    summary1 = _event_first_sentence(ctx.get('summary'))
    lead = _event_first_sentence(desc)
    raw = ' '.join([headline, desc, summary1])
    norm = _event_norm_text(raw)
    article_norm = _event_norm_text(' '.join([headline, desc]))
    core_raw = ' '.join([headline, lead, summary1])
    proper = _event_proper_nouns(' '.join([desc, summary1]), headline)
    tokens = _event_identity_tokens(norm, proper)
    # Speakers and their titles are WHO talks, not WHAT the event is about.
    speakers = _event_speakers(raw)
    speaker_words = set()
    for name, title in speakers:
        speaker_words.add(name)
        speaker_words.update(title)
    obj_raw = raw
    for m in _EVENT_SPEAKER_RE.finditer(raw):
        if m.group(2).lower() in {n for n, _ in speakers}:
            obj_raw = obj_raw.replace(m.group(1), ' ')
    obj_norm = _event_norm_text(obj_raw)
    obj_tokens = _event_identity_tokens(obj_norm, proper - speaker_words)
    objects = sorted(t for t in obj_tokens
                     if (t[:1] == '#' and t[1:] in _EVENT_OBJECT_CONCEPTS) or t[:1] == '!')
    sites = sorted(_event_sites(' '.join([headline if not headline.istitle() else '', desc])))
    hl_norm = _event_norm_text(headline)
    hl_plain = [t for t in _event_identity_tokens(hl_norm, proper) if t[:1] not in ('#', '@', '!')]
    lead_proper = sorted(_event_proper_nouns(lead, headline) - speaker_words)
    # What the headline itself names as the target/place (for NEW TARGET).
    hl_objects = sorted({t for t in _event_identity_tokens(hl_norm, proper - speaker_words)
                         if (t[:1] == '#' and t[1:] in _EVENT_OBJECT_CONCEPTS) or t[:1] == '!'}
                        | {'!' + s for s in _event_sites(headline if not headline.istitle() else '')})
    return {
        'tokens': sorted(tokens),
        'actors': sorted(_event_actor_hits(norm)),
        'hl_actors': sorted(_event_actor_hits(hl_norm)),
        'kinds': sorted(_event_kinds(_event_norm_text(core_raw))),
        'speaker': _event_headline_speaker(headline),
        'speakers': sorted({n for n, _ in speakers}),
        'names': sorted(_event_names(raw)),
        'objects': sorted(set(objects) | {'!' + s for s in sites}),
        'sites': sites,
        'lead_proper': lead_proper,
        'hl_objects': hl_objects,
        'hl_stems': _event_word_stems(hl_plain),
        'stems': _event_word_stems([t for t in tokens if t[:1] not in ('#', '@')] + [t[1:] for t in tokens if t[:1] == '!']),
        'amounts': sorted(_event_all_amounts(norm)),
        'new_amounts': sorted(_event_new_amounts(article_norm)),
        'stage': _event_stage(article_norm),
        'event_date': _event_identity_date(ctx, today).isoformat(),
        'kinetic': '#attack' in tokens,
        'new_action': bool(_EVENT_NEW_ACTION_RE.search(article_norm) or _EVENT_NEW_ROUND_RE.search(article_norm)),
    }


def _event_identity_of(ident):
    """Stored identity -> comparable identity. Rows without the current
    identity fields (legacy schema: headline_norm/story_url only) are rebuilt
    from the stored headline."""
    if isinstance(ident, dict) and 'tokens' in ident and 'kinds' in ident:
        return ident
    hl = (ident or {}).get('headline_norm', '') if isinstance(ident, dict) else ''
    rebuilt = _event_identity({'headline': hl}, datetime.now(_EVENT_TZ).date())
    rebuilt['event_date'] = None
    rebuilt['legacy'] = True
    return rebuilt


def _event_same_event_gates(a, b):
    """None when `a` (new item) and `b` (stored identity) are the same event
    by actors, core action, speaker and object; else the failing gate."""
    aa, ba = set(a.get('actors') or []), set(b.get('actors') or [])
    if aa and ba and not aa & ba:
        return 'actors'
    ah, bh = set(a.get('hl_actors') or []), set(b.get('hl_actors') or [])
    if ah and bh and not ah & bh:
        return 'headline_actors'
    if ah - ba:
        return 'new_actor'
    ak, bk = set(a.get('kinds') or []), set(b.get('kinds') or [])
    if (ak or bk) and not ak & bk:
        return 'kind'
    for x, y in ((a, b), (b, a)):
        sp = x.get('speaker')
        if sp and sp not in set(y.get('names') or []):
            return 'speaker'
    ao, bo = set(a.get('objects') or []), set(b.get('objects') or [])
    if ao and bo:
        if not ao & bo:
            return 'object'
    elif len(aa & ba) < 2:
        return 'object'
    elif ao or bo:
        # One side names no object (e.g. a legacy row rebuilt from its
        # lowercase headline): an empty object cannot vouch for "same
        # object" — the other side's object must at least appear in it.
        full_objs, other = (ao, b) if ao else (bo, a)
        if not _event_plain(full_objs) & _event_plain(other.get('tokens') or []):
            return 'object'
    return None


def _event_plain(tokens):
    """Tokens without their #/@/! prefix and plural s (for mention checks)."""
    out = set()
    for t in tokens:
        w = t.lstrip('#@!')
        out.add(w[:-1] if len(w) > 4 and w.endswith('s') and not w.endswith('ss') else w)
    return out


def _event_identity_score(a, b):
    """Weighted token agreement after the gates: shared weight over the
    smaller identity (core tokens weigh EVENT_CORE_WEIGHT). An item that only
    ADDS to the row (a new product, region or site) can score high here —
    that is what the NEW FACT check is for."""
    ta, tb = set(a['tokens']), set(b['tokens'])
    if not ta or not tb:
        return 0.0
    shared = sum(_event_token_weight(t) for t in ta & tb)
    return shared / min(sum(_event_token_weight(t) for t in ta), sum(_event_token_weight(t) for t in tb))


def _event_identity_similarity(a, b):
    """(shared core weight, score) between two identities, or None when a
    same-event gate fails."""
    if _event_same_event_gates(a, b) is not None:
        return None
    both = set(a['tokens']) & set(b['tokens'])
    core = sum(_event_token_weight(t) for t in both if t[:1] in ('#', '@', '!'))
    return core, _event_identity_score(a, b)


def _event_identity_matches(ident, row):
    """Best similarity of `ident` against any identity on `row`, or None."""
    best = None
    for stored in row.get('identities') or []:
        sim = _event_identity_similarity(ident, _event_identity_of(stored))
        if sim is None:
            continue
        core, overlap = sim
        if overlap >= EVENT_IDENTITY_MIN_OVERLAP:
            if best is None or overlap > best[1]:
                best = sim
    return best


def _event_new_fact(ident, row):
    """Reason string when `ident` carries a fact the matched row does not
    have (so it is a NEW event, first_print), else None."""
    stored = [_event_identity_of(s) for s in row.get('identities') or []]
    row_stage = max([s.get('stage') or 0 for s in stored] or [0])
    if ident['stage'] >= 2 and ident['stage'] > row_stage:
        return f"stage {row_stage}->{ident['stage']}"
    row_amounts = set()
    for s in stored:
        row_amounts.update(s.get('amounts') or [])
    for amt in ident.get('new_amounts') or []:
        ma = _event_amount_millions(amt)
        close = [r for r in row_amounts
                 if _event_amount_millions(r) and ma
                 and min(ma, _event_amount_millions(r)) / max(ma, _event_amount_millions(r)) >= 0.75]
        if not close:
            return f"new amount {amt}"
    row_stems, row_names = set(), set()
    for s in stored:
        row_stems.update(s.get('stems') or [])
        row_names.update(s.get('names') or [])
        row_names.update(t[1:] for t in s.get('tokens') or [] if t[:1] == '!')
        if s.get('legacy'):
            # A legacy row has only its lowercase headline: every word in it
            # may be a name ("strait of hormuz").
            row_names.update(_event_plain(s.get('tokens') or []))
    # A policy action asserted now (announced/imposed/signed...) on an object
    # the row never mentions ("tariffs on semiconductors" vs a steel row).
    if ident['stage'] >= 2 and ident['stage'] >= row_stage:
        new_obj = [w for w in ident.get('hl_stems') or [] if w not in row_stems]
        if new_obj:
            return f"policy action on new object '{new_obj[0]}'"
    # A kinetic act whose headline names a target CLASS the row never
    # mentions anywhere (tankers vs a carrier-deployment row) is a new event
    # on any day. Named sites/places stay with the day-gap rule below (a
    # same-day rewrite may name the plant the first report did not).
    if 'attack' in (ident.get('kinds') or []):
        row_words = _event_plain(row_names)
        for s in stored:
            row_words |= _event_plain(s.get('tokens') or [])
        new_tgt = sorted(_event_plain([t for t in ident.get('hl_objects') or [] if t[:1] == '#']) - row_words)
        if new_tgt:
            return f"new target {new_tgt[0]}"
    # An act of the same kind on a different day (kinetic strike, sanctions
    # round, tariff action, output decision...) is a new event when it names
    # a new site/place or says it is a new act/round.
    if (ident['kinetic'] or ident.get('kinds')) and ident.get('event_date'):
        row_dates = [s.get('event_date') for s in stored if s.get('event_date')]
        if any(not s.get('event_date') for s in stored):
            # Legacy identities carry no action date: use the row's first_seen day.
            fs = _event_parse_iso(row.get('first_seen'))
            if fs is not None:
                row_dates.append(fs.astimezone(_EVENT_TZ).date().isoformat())
        my_day = datetime.strptime(ident['event_date'], '%Y-%m-%d').date()
        gaps = [abs((my_day - datetime.strptime(d, '%Y-%m-%d').date()).days) for d in row_dates]
        if gaps and min(gaps) > EVENT_SAME_ACTION_DAY_WINDOW:
            row_sites = set()
            for s in stored:
                row_sites.update(s.get('sites') or [])
                if s.get('legacy'):
                    row_sites.update(_event_plain(s.get('tokens') or []))
            new_sites = set(ident.get('sites') or []) - row_sites
            if new_sites:
                return f"new site {sorted(new_sites)[0]} on {ident['event_date']}"
            places = ident.get('lead_proper') or []
            if any(s.get('legacy') for s in stored):
                # A legacy row kept only its headline: compare headline to headline.
                places = [t[1:] for t in ident.get('hl_objects') or [] if t[:1] == '!']
            new_places = [p for p in places if p not in row_names]
            if new_places:
                return f"new place {new_places[0]} on {ident['event_date']}"
            if ident.get('new_action'):
                return f"new act on {ident['event_date']}"
    return None


def _event_is_same_article(row, identity):
    """Same article = headline_norm equals any stored identity's
    headline_norm, OR non-empty story_url equals any stored non-empty
    story_url (exact)."""
    hl = identity.get('headline_norm') or ''
    url = identity.get('story_url') or ''
    for ident in row.get('identities') or []:
        if hl and ident.get('headline_norm') == hl:
            return True
        if url and (ident.get('story_url') or '') == url:
            return True
    return False


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
        # Event-identity fold (see _event_decide() Rule A): the item is the
        # same event as an earlier card — it rides that card's clock and is
        # never a separate vote in calculate_score().
        'event_folded': 'event_folded',
        'clock_anchor': 'clock_anchor',
        'clock_hours': 'clock_hours',
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

    _EC_SURVEY_PHRASES = (
        'consumer confidence',
        'consumer optimism',
        'conference board',
        'present situation index',
        'expectations index',
        'university of michigan',
        'u. of m.',
        'umich',
        'michigan consumer',
        'jolts',
        'jobless claims',
        'initial claims',
        'continuing claims',
        'adp employment',
        'adp non-farm',
        'adp nonfarm',
        'ism manufacturing',
        'ism services',
        'purchasing managers',
    )
    _EC_SURVEY_CUES = (
        'confidence', 'optimism', 'sentiment', 'survey', 'index',
        'actual', 'forecast', 'expected', 'consensus', 'miss', 'beat',
        'points', 'lowest since', 'highest since',
    )

    # Jobs prints (NFP / payrolls) are EC-owned like the survey prints
    # above: never a geo card, never geo identity, never a geo vote. Kept
    # only when the same text reports a NEW geo/policy action the print
    # merely accompanies.
    _EC_JOBS_PRINT_RE = re.compile(
        r'\b(?:non-?farm payrolls?|payrolls?|jobs report|nfp|employment report)\b'
        r'|\b(?:economy|employers|u\.s\.|us) (?:added|shed|lost|created) (?:just |only |a mere )?[\d,.]+k? jobs\b')
    _EC_JOBS_PRINT_VETO_RE = re.compile(
        r'\b(?:tariffs?|sanctions?|ban(?:s|ned)?|strikes?|missiles?|ceasefire|troops|embargo|'
        r'executive order|export controls?|blockade|invasion)\b')

    def _is_ec_survey_recap(self, item):
        if not isinstance(item, dict):
            text = str(item or '').lower()
        else:
            text = ' '.join([
                str(item.get('headline') or ''),
                str(item.get('description') or ''),
                str(item.get('summary') or ''),
                str(item.get('title') or ''),
            ]).lower()
        if not text.strip():
            return False
        if self._EC_JOBS_PRINT_RE.search(text) and not self._EC_JOBS_PRINT_VETO_RE.search(text):
            return True
        if not any(p in text for p in self._EC_SURVEY_PHRASES):
            return False
        return any(c in text for c in self._EC_SURVEY_CUES)

    # Isolated civil-aviation incidents (cockpit fights, foiled hijacks,
    # diverted airliners) are not market domain — Sep 30 Flydubai FZ1073 miss.
    # Word-boundary regex so 'oil' never hits 'turmoil', 'plane' never 'planet'.
    _AVIATION_PHRASE_RE = re.compile(r'\b(?:' + '|'.join((
        r'fly ?dubai',
        r'fz ?1073',
        r'violent incidents? between pilots',
        r'(?:fights?|clash(?:es)?) between pilots',
        r'pilots? (?:was |were )?stabb(?:ed|ing)',
        r'stabb(?:ed|ing) the (?:pilot|captain)',
        r'cockpit stabb?(?:ed|ing|s)?',
        r'cockpit assaults?',
        r'cockpit fights?',
        r'hijack(?:s|ed|er|ers|ing)?',
        r'unlawful interference',
        r'squawk(?:s|ed|ing)? 7500',
        r'7500 squawk',
        r'flights? (?:was |were )?diverted',
        r'diverted flights?',
        r'emergency landings?',
        r'passengers recount(?:s|ed|ing)?',
        r'passengers (?:overcame|subdued)',
        r'(?:attempted|tried) to crash the plane',
    )) + r')\b')
    _AVIATION_CUE_RE = re.compile(
        r'\b(?:pilots?|co-?pilots?|copilots?|captains?|cockpits?|passengers?|'
        r'airlines?|flights?|aircraft|planes?|diverted|landings?|airports?)\b'
    )
    _AVIATION_VETO_RE = re.compile(
        r'\b(?:hormuz|strait of hormuz|oil|pipelines?|tankers?|sanctions|tariffs?|'
        r'ban on imports|ceasefires?|nato|troop deployments?|army expansion|'
        r'mobili[sz]ations?|missile strikes? on infrastructure|'
        r'airspace closed by government order|no-fly zones? declared|'
        # Concept widening of the two phrases above so real war copy
        # ("missile barrage", "airspace closes", "airstrikes") vetoes too.
        r'airspace|missiles?|air ?strikes?|drone strikes?|no-fly)\b'
    )

    def _is_aviation_incident(self, item):
        if not isinstance(item, dict):
            text = str(item or '').lower()
            veto_text = text
        else:
            text = ' '.join([
                str(item.get('headline') or ''),
                str(item.get('description') or ''),
                str(item.get('summary') or ''),
                str(item.get('title') or ''),
            ]).lower()
            # Veto reads the article's own text only, not Haiku's `summary` —
            # the live FZ1073 pin's summary invented "Oil and broad risk
            # sentiment face near-term uncertainty", which would otherwise
            # veto the very card this reject exists to catch.
            veto_text = ' '.join([
                str(item.get('headline') or ''),
                str(item.get('description') or ''),
                str(item.get('title') or ''),
            ]).lower()
        if not text.strip():
            return False
        if not self._AVIATION_PHRASE_RE.search(text):
            return False
        if not self._AVIATION_CUE_RE.search(text):
            return False
        if self._AVIATION_VETO_RE.search(veto_text):
            return False
        return True

    # Late official attribution of an already-public incident (Sep 30 miss:
    # PM Burnham naming Iran on the Sunday RAF Fairford incident, 3 days on)
    # is follow_up 24h max Tier 3, never first_print 48h Tier 2. Applied as
    # Rule C inside _event_decide() so it runs after Haiku, before pin/score.
    _LATE_ATTRIBUTION_SITE_RE = re.compile(
        r'\b(?:raf fairford|fairford|incident near air base|played a part|'
        r'strong indications|believes iran was involved|we believe iran)\b'
    )
    _LATE_ATTRIBUTION_VERB_RE = re.compile(
        r'\b(?:believes|believed|indications|played a part|involved|'
        r'foreign actor|linked to|suspects iran)\b'
    )
    _LATE_ATTRIBUTION_KINETIC_RE = re.compile(
        r'\b(?:struck|bombed|bombing|explosions?|missiles?|drone hits?|killed|'
        r'airspace closed|no-fly|signed|deployed|ban effective)\b'
    )

    def _is_late_attribution(self, item):
        # Reads headline + description + title only. Haiku's `summary` is
        # excluded from match AND veto (same choice as the aviation veto):
        # summaries paraphrase background ("a U.S. air base used for strikes
        # on Iran") and can carry kinetic words like 'missile' that are not
        # new facts in THIS article.
        if not isinstance(item, dict):
            text = str(item or '').lower()
        else:
            text = ' '.join([
                str(item.get('headline') or ''),
                str(item.get('description') or ''),
                str(item.get('title') or ''),
            ]).lower()
        if not text.strip():
            return False
        if not self._LATE_ATTRIBUTION_SITE_RE.search(text):
            return False
        if not self._LATE_ATTRIBUTION_VERB_RE.search(text):
            return False
        if self._LATE_ATTRIBUTION_KINETIC_RE.search(text):
            return False
        return True

    @staticmethod
    def _late_attribution_locked(record):
        return 'late_attribution' in str((record or {}).get('event_rule') or '')

    @staticmethod
    def _late_attribution_tier(direction, tier):
        if direction in ('bullish', 'bearish') or tier in (1, 2):
            return 3
        return tier

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
        # Event memory (see _apply_event_rules()) — one lock serializes the
        # fetch_news() pass and background classify threads on
        # /data/geo_event_memory.json; _event_logged dedupes the per-article
        # skip lines so they print once per process, not every cycle.
        self._event_memory_lock = threading.Lock()
        self._event_logged = set()
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
        Called by classify_relevance_batch()."""
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

FIRST_PRINT (48-hour clock): the article introduces a new fact — new kinetic action, new official restriction, new signed agreement, new disclosed size/terms, or first confirmation from a primary actor. A new kinetic act after silence of 5+ trading days is FIRST_PRINT even if it's the same war or the same underlying conflict. A named person, leaked memo, survivor quote, or second-outlet rewrite of an event that already happened is FOLLOW_UP, not a new event.

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
- An official naming a country on an incident that was already public (arrests, “major incident,” prior foreign-actor comments) is FOLLOW_UP, 24-hour clock, max Tier 3. first_print 48h is only for a new fact the tape did not have: a new strike, a new target class, a new supply break, or the first public report of the incident itself. A PM saying “we believe X played a part” three days later is not the incident happening today.

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
- Isolated civil-aviation crime is not market domain: a cockpit fight, foiled hijack, passenger restraining a pilot, or a diverted airliner — even if the destination is Israel, even if an official calls it terror, even if jets were scrambled — fails FILTER 5 and DECISION 1 unless THIS article also reports a new state military campaign, a government airspace-closure/no-fly order, or an energy/trade disruption. A second write-up of the same landing is the same reject, not a new FIRST_PRINT.

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
- EC PRINTS ARE NOT GEO. Reject as not relevant (relevant: false, no tier, no kind) when the article is primarily a scheduled US data print or household survey recap. This includes Conference Board Consumer Confidence / Expectations / Present Situation, University of Michigan consumer sentiment, JOLTS, Initial or Continuing Jobless Claims, ADP, ISM, PMI, Retail Sales, Housing Starts, Durable Goods, Existing or New Home Sales, Personal Income/Spending, and any "consumer optimism / household mood / confidence slides" write-up of those prints. NFP, CPI (any variant), Core PCE, and GDP are also EC-owned — if the article is only "the number came out," reject it. Geo may keep an article that uses a print as one sentence inside a NEW policy, war, tariff, or supply-chain action (example: "White House announces oil-export ban after CPI print") — the action is the event, not the print. Never use "50% relative deviation" or "Major Economic Data Exception." Those phrases are retired. Percent-miss math lives only in pipelines/economic_calendar.py.

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

    def _clock_expired(self, entry):
        """Score/board clock for a classification entry. A folded follow-up
        (event_folded, see _event_decide() Rule A) has NO clock of its own:
        it expires with the original card — clock_anchor (the original's
        clock start) + clock_hours (the original's window, 48h for a
        first_print). Everything else keeps its own classified_at clock,
        24h for follow_up, 48h otherwise (unchanged)."""
        anchor = entry.get('clock_anchor')
        if anchor:
            return self.is_article_too_old(anchor, max_hours=entry.get('clock_hours') or MAX_ARTICLE_AGE_HOURS)
        return self.is_article_too_old(
            entry.get('classified_at', ''),
            max_hours=24 if entry.get('kind') == 'follow_up' else MAX_ARTICLE_AGE_HOURS)

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
        if story.get('clock_anchor'):
            return self._clock_expired(story)
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
                if self._is_ec_survey_recap(story):
                    pulse_logger.log(
                        f"🚫 Geo EC-survey reject (pinned): {headline[:80]}"
                    )
                    dirty = True
                    continue
                if self._is_aviation_incident(story):
                    pulse_logger.log(
                        f"🚫 Geo aviation-incident reject (pinned): {headline[:80]}"
                    )
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

    # Fields a same-story merge never copies from an incoming follow_up onto
    # an original first_print survivor (see _guarded_duplicate_merge()):
    # kind and classified_at drive the survivor's 48h clock (classified_at is
    # what known_relevant/is_article_too_old age a cache entry from), and the
    # event-rule audit fields describe the echo, not the original.
    MERGE_KIND_GUARD_KEEP_FIELDS = ('kind', 'classified_at', 'event_rule', 'pre_rule_kind', 'pre_rule_tier')

    @staticmethod
    def _merge_kind_guard_applies(existing_kind, incoming_kind):
        """True when an original first_print (missing kind counts as
        first_print — the default everywhere else in this pipeline) would be
        overwritten by an incoming follow_up. Both-follow_up and any
        incoming first_print merge exactly as before."""
        return (existing_kind or 'first_print') == 'first_print' and incoming_kind == 'follow_up'

    def _log_merge_kind_guard(self, original_headline, incoming_headline=''):
        line = f"MERGE KIND GUARD | original kept first_print | incoming follow_up | headline={original_headline[:90]}"
        if incoming_headline:
            line += f" | incoming_headline={incoming_headline[:90]}"
        pulse_logger.log(line)

    def _guarded_duplicate_merge(self, existing, new_class, original_headline, incoming_headline):
        """Value to write over a same-story survivor's gemini_cache entry.
        Unchanged behavior (return new_class) unless the survivor is
        first_print and the incoming read is follow_up: then every field is
        merged EXCEPT kind/classified_at (and the echo's event-rule audit
        fields), so the original keeps first_print and its clock start.
        Not used by the EC fold, which is a separate rule."""
        if self._late_attribution_locked(existing):
            merged = {k: v for k, v in new_class.items() if k not in self.MERGE_KIND_GUARD_KEEP_FIELDS}
            for k in self.MERGE_KIND_GUARD_KEEP_FIELDS:
                if k in existing:
                    merged[k] = existing[k]
            merged['kind'] = 'follow_up'
            merged['tier'] = self._late_attribution_tier(merged.get('direction'), merged.get('tier'))
            pulse_logger.log(
                f"🔧 Geo late-attribution fold held on merge: {original_headline[:90]} | "
                f"incoming {new_class.get('kind')} T{new_class.get('tier')} → follow_up T{merged.get('tier')}"
            )
            return merged
        if not self._merge_kind_guard_applies(existing.get('kind'), new_class.get('kind')):
            return new_class
        merged = {k: v for k, v in new_class.items() if k not in self.MERGE_KIND_GUARD_KEEP_FIELDS}
        for k in self.MERGE_KIND_GUARD_KEEP_FIELDS:
            if k in existing:
                merged[k] = existing[k]
        merged['kind'] = 'first_print'
        self._log_merge_kind_guard(original_headline, incoming_headline)
        return merged

    def _refresh_pin_classification(self, headline, new_class, incoming_headline=''):
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
                    # EXCEPTION (merge kind guard): a same-story echo read as
                    # follow_up (including one forced by event memory) must
                    # not turn the original first_print pin into follow_up —
                    # the pin keeps first_print and its 48h clock (pin TTL is
                    # published_at/timestamp/date + kind; none of those
                    # timestamps are touched here).
                    if self._late_attribution_locked(pin):
                        # Late-attribution pin stays follow_up / max T3.
                        pin['kind'] = 'follow_up'
                        pin['tier'] = self._late_attribution_tier(pin.get('direction'), pin.get('tier'))
                    elif self._merge_kind_guard_applies(pin.get('kind'), new_class.get('kind')):
                        self._log_merge_kind_guard(headline, incoming_headline)
                    else:
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
            if r.get('event_folded') or r.get('kind') == 'follow_up':
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
            if self._is_ec_survey_recap(article):
                pulse_logger.log(
                    f"🚫 Geo EC-survey reject (pin skipped): {headline[:80]}"
                )
                continue
            if self._is_aviation_incident(article):
                pulse_logger.log(
                    f"🚫 Geo aviation-incident reject (pin skipped): {headline[:80]}"
                )
                continue
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
- EC PRINTS ARE NOT GEO. Reject as not relevant (relevant: false, no tier, no kind) when the article is primarily a scheduled US data print or household survey recap. This includes Conference Board Consumer Confidence / Expectations / Present Situation, University of Michigan consumer sentiment, JOLTS, Initial or Continuing Jobless Claims, ADP, ISM, PMI, Retail Sales, Housing Starts, Durable Goods, Existing or New Home Sales, Personal Income/Spending, and any "consumer optimism / household mood / confidence slides" write-up of those prints. NFP, CPI (any variant), Core PCE, and GDP are also EC-owned — if the article is only "the number came out," reject it. Geo may keep an article that uses a print as one sentence inside a NEW policy, war, tariff, or supply-chain action (example: "White House announces oil-export ban after CPI print") — the action is the event, not the print. Never use "50% relative deviation" or "Major Economic Data Exception." Those phrases are retired. Percent-miss math lives only in pipelines/economic_calendar.py.

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
            if entry.get('relevant') and not self._clock_expired(entry)
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

    # ── Event memory + 14-day after-action cap (see module-level helpers) ──

    def _load_event_memory(self):
        """Load /data/geo_event_memory.json, dropping rows whose first_seen
        is older than EVENT_MEMORY_TTL_DAYS. Missing file -> {}. Never
        raises. This file is NOT the classification cache and is never
        touched by _purge_blocked_from_cache()."""
        now = datetime.now(timezone.utc)
        mem, dropped = {}, 0
        try:
            if os.path.exists(GEO_EVENT_MEMORY_FILE):
                with open(GEO_EVENT_MEMORY_FILE, 'r') as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    for key, row in raw.items():
                        first_seen = _event_parse_iso(row.get('first_seen')) if isinstance(row, dict) else None
                        if first_seen is None or now - first_seen > timedelta(days=EVENT_MEMORY_TTL_DAYS):
                            dropped += 1
                            continue
                        mem[key] = row
        except Exception as e:
            pulse_logger.log(f"⚠️ EVENT MEMORY load failed, starting empty: {e}", level="WARNING")
            mem = {}
        return mem, dropped

    def _event_ctx(self, headline, summary, description, reason, tier_reasoning, story_url, dated):
        return {
            'headline': headline or '',
            'summary': summary or '',
            'description': description or '',
            'reason': reason or '',
            'tier_reasoning': tier_reasoning or '',
            'story_url': (story_url or '').strip(),
            'published_date': _event_published_date(
                dated.get('date', ''), dated.get('published_at', ''), dated.get('timestamp', '')),
        }

    def _event_log_once(self, tag, headline_norm, line):
        key = (tag, headline_norm)
        if key in self._event_logged:
            return
        self._event_logged.add(key)
        pulse_logger.log(line)

    def _event_decide(self, entry, mem, now, today):
        """Rule B, Rule C, then Rule A (event identity) for one
        classification. Mutates `mem` only;
        returns a decision dict (or None when nothing changes) that
        _apply_event_rules() applies to the classification afterwards."""
        ctx = entry['ctx']
        target = entry['target']
        now_iso = now.isoformat()
        haiku_kind = target.get('kind') if target.get('kind') in ('first_print', 'follow_up') else 'first_print'
        haiku_tier = target.get('tier')
        kind, tier = haiku_kind, haiku_tier
        headline = ctx['headline']
        identity = {'headline_norm': _event_headline_norm(headline), 'story_url': ctx.get('story_url') or ''}
        short = headline[:90]
        logs, rules = [], []

        # Rule B — 14-day after-action cap (independent of memory).
        age = _event_resolve_age(ctx, today)
        if age['status'] == 'old':
            kind = 'follow_up'
            if tier in (1, 2):
                tier = 3
            if (kind, tier) != (haiku_kind, haiku_tier):
                rules.append('age_cap')
                age_days = age['age_days'] if age['age_days'] is not None else f'>{AGE_CAP_DAYS}'
                logs.append(
                    f"AGE CAP | event_date={age['event_date']} | age_days={age_days} | "
                    f"haiku_kind={haiku_kind} haiku_tier={haiku_tier} -> kind={kind} tier={tier} | headline={short}"
                )
        elif age['status'] == 'none':
            self._event_log_once('age_skip', identity['headline_norm'],
                                 f"AGE CAP skip | reason=no_event_date | headline={short}")

        # Rule C — late official attribution (see _is_late_attribution()).
        # description is passed only when it is the article's own text, not
        # a Haiku summary copied into it (pins/cached items carry summary there).
        la_desc = ctx.get('description') if ctx.get('description') != ctx.get('summary') else ''
        if self._is_late_attribution({'headline': headline, 'description': la_desc}):
            la_tier = self._late_attribution_tier(target.get('direction'), tier)
            if (kind, tier) != ('follow_up', la_tier):
                prev_kind = kind
                kind, tier = 'follow_up', la_tier
                rules.append('late_attribution')
                logs.append(f"🔧 Geo late-attribution fold: {headline} {prev_kind}→follow_up T{tier if tier is not None else '-'}")

        # Rule A — 7-day event identity bank (one event, one identity).
        # EC-owned prints (jobs/data recaps) never enter geo identity.
        touched = False
        fold = None
        ec_owned = self._is_ec_survey_recap({'headline': headline, 'description': ctx.get('description'),
                                             'summary': ctx.get('summary')})
        if ec_owned:
            self._event_log_once('ec_owned', identity['headline_norm'],
                                 f"EVENT MEMORY skip | reason=ec_owned_print | headline={short}")
        else:
            ident = _event_identity(ctx, today)
            same_row = None
            for rkey, row in mem.items():
                if _event_is_same_article(row, identity):
                    same_row = rkey
                    break
            if same_row is not None:
                mem[same_row]['last_seen'] = now_iso
                touched = True
            else:
                folds, new_facts = [], []
                for rkey, row in mem.items():
                    sim = _event_identity_matches(ident, row)
                    if sim is None:
                        continue
                    reason = _event_new_fact(ident, row)
                    if reason is None:
                        folds.append((-sim[1], row.get('first_seen', ''), rkey, sim))
                    else:
                        new_facts.append((rkey, reason))
                full_identity = dict(identity, **ident)
                if folds:
                    _, _, row_key, sim = min(folds)
                    row = mem[row_key]
                    row['last_seen'] = now_iso
                    idents = row.setdefault('identities', [])
                    idents.append(full_identity)
                    if len(idents) > EVENT_MAX_IDENTITIES:
                        del idents[1:len(idents) - EVENT_MAX_IDENTITIES + 1]
                    touched = True
                    clock_hours = row.get('clock_hours') or (
                        24 if row.get('haiku_kind_on_first_seen') == 'follow_up' else MAX_ARTICLE_AGE_HOURS)
                    fold = {'event_key': row_key,
                            'clock_anchor': row.get('clock_start') or row.get('first_seen'),
                            'clock_hours': clock_hours}
                    if kind != 'follow_up':
                        row['forced_follow_up_count'] = int(row.get('forced_follow_up_count', 0)) + 1
                    kind = 'follow_up'
                    rules.append('event_memory')
                    logs.append(
                        f"FORCE FOLLOW_UP | key={row_key} | core={sim[0]} overlap={sim[1]:.2f} | "
                        f"haiku_kind={haiku_kind} | clock=original {fold['clock_anchor']} +{clock_hours}h | "
                        f"matched=\"{row.get('sample_headline', '')[:90]}\" | headline={short}"
                    )
                else:
                    row_key = f"evt|{hashlib.sha1((identity['headline_norm'] + now_iso).encode('utf-8')).hexdigest()[:12]}"
                    mem[row_key] = {
                        'first_seen': now_iso, 'last_seen': now_iso, 'sample_headline': headline,
                        'haiku_kind_on_first_seen': haiku_kind, 'forced_follow_up_count': 0,
                        # The card's own clock — what a later fold inherits.
                        'clock_start': target.get('classified_at') or now_iso,
                        'clock_hours': 24 if kind == 'follow_up' else MAX_ARTICLE_AGE_HOURS,
                        'identities': [full_identity],
                    }
                    touched = True
                    for nk, reason in new_facts:
                        self._event_log_once('new_fact', identity['headline_norm'],
                                             f"NEW FACT | {reason} | vs key={nk} | kept kind={kind} | headline={short}")

        decision = None
        if (kind, tier) != (haiku_kind, haiku_tier) or fold:
            decision = {'kind': kind, 'tier': tier, 'haiku_kind': haiku_kind, 'haiku_tier': haiku_tier,
                        'rules': rules, 'logs': logs, 'fold': fold}
        return decision, touched

    def _apply_event_rules(self, entries, log_loaded=False):
        """Rule B (14-day after-action cap) then Rule A (7-day event memory)
        for each entry {'ctx': ..., 'target': classification dict,
        'store': tag}. Mutates target['kind'] (and target['tier'] only when
        the age cap fires) in place, so the forced values are what get
        persisted/pinned/scored downstream. Any failure is logged and the
        classification keeps Haiku's original kind/tier. Returns the set of
        store tags whose target dicts changed."""
        changed_stores = set()
        try:
            decisions = []
            with self._event_memory_lock:
                mem, dropped = self._load_event_memory()
                if log_loaded:
                    pulse_logger.log(f"EVENT MEMORY loaded | keys={len(mem)} | dropped_expired={dropped}")
                dirty = dropped > 0
                now = datetime.now(timezone.utc)
                today = now.astimezone(_EVENT_TZ).date()
                for entry in entries:
                    try:
                        decision, touched = self._event_decide(entry, mem, now, today)
                        dirty = dirty or touched
                        if decision:
                            decisions.append((entry, decision))
                    except Exception as e:
                        pulse_logger.log(
                            f"⚠️ Event rules failed for '{(entry.get('ctx') or {}).get('headline', '')[:60]}' — "
                            f"keeping Haiku kind/tier: {e}", level="WARNING")
                if dirty:
                    try:
                        atomic_write_json(GEO_EVENT_MEMORY_FILE, mem)
                    except Exception as e:
                        pulse_logger.log(f"⚠️ EVENT MEMORY save failed: {e}", level="WARNING")
            emitted = set()
            for entry, d in decisions:
                target = entry['target']
                target['kind'] = d['kind']
                target['tier'] = d['tier']
                target['event_rule'] = '+'.join(d['rules'])
                target['pre_rule_kind'] = d['haiku_kind']
                target['pre_rule_tier'] = d['haiku_tier']
                if d.get('fold'):
                    # Folded into an existing event: inherits the original
                    # card's clock, never pinned, never a separate vote.
                    target['event_folded'] = True
                    target['event_key'] = d['fold']['event_key']
                    target['clock_anchor'] = d['fold']['clock_anchor']
                    target['clock_hours'] = d['fold']['clock_hours']
                changed_stores.add(entry.get('store', ''))
                for line in d['logs']:
                    # A pin and its own cache entry are the same article; if
                    # both change in one pass, log the identical line once.
                    if line not in emitted:
                        emitted.add(line)
                        pulse_logger.log(line)
        except Exception as e:
            pulse_logger.log(f"⚠️ Event memory rules failed — keeping Haiku kind/tier: {e}", level="WARNING")
            return set()
        return changed_stores

    def _event_rule_entries_for_fresh(self, source_items, classifications):
        """Entries for freshly returned Haiku results (background_classify(),
        _reclassify_cached_pending()). Only scoring-eligible items
        (relevant, confidence >= 0.75) — the same bar known_relevant and
        update_pinned_store() use."""
        entries = []
        for r in classifications or []:
            if not isinstance(r, dict) or 'id' not in r:
                continue
            idx = r['id'] - 1
            if not (0 <= idx < len(source_items)):
                continue
            if not r.get('relevant') or (r.get('confidence') or 0) < 0.75:
                continue
            art = source_items[idx]
            entries.append({
                'ctx': self._event_ctx(art.get('headline', ''), r.get('summary'), art.get('description'),
                                       r.get('reason'), r.get('reasoning'), art.get('link'), art),
                'target': r,
                'store': 'fresh',
            })
        return entries

    def _apply_event_rules_to_known(self, items, gemini_cache, gemini_cache_file, pinned_stories):
        """Every fetch_news() cycle: load event memory (logs EVENT MEMORY
        loaded) and run the rules over already-classified entries for the
        current feed (cache hits) and active pins, oldest first — so the
        first cycle after deploy seeds memory from what's already live and
        caps a cached old after-action recap before it's scored. A re-seen
        article matches its own identity and is left alone. Changed cache
        entries / pins are persisted so the forced kind drives their clock."""
        try:
            entries = []
            for i in items:
                c = gemini_cache.get(i.get('headline', ''))
                if not isinstance(c, dict) or not c.get('relevant') or (c.get('confidence') or 0) < 0.75:
                    continue
                if self._clock_expired(c):
                    continue
                entries.append((c.get('classified_at', ''), {
                    'ctx': self._event_ctx(i.get('headline', ''), c.get('summary'), i.get('description'),
                                           c.get('reason'), c.get('tier_reasoning'), i.get('link'), i),
                    'target': c,
                    'store': 'cache',
                }))
            covered = {i.get('headline', '') for i in items}
            for p in pinned_stories or []:
                ph = p.get('headline', '')
                c = gemini_cache.get(ph)
                if ph in covered or not isinstance(c, dict) or not c.get('relevant') or (c.get('confidence') or 0) < 0.75:
                    continue
                covered.add(ph)
                entries.append((c.get('classified_at', ''), {
                    'ctx': self._event_ctx(ph, c.get('summary'), p.get('description') or c.get('summary'),
                                           c.get('reason'), c.get('tier_reasoning'), p.get('link'), p),
                    'target': c,
                    'store': 'cache',
                }))
            for p in pinned_stories or []:
                entries.append((p.get('classified_at') or p.get('pinned_at', ''), {
                    'ctx': self._event_ctx(p.get('headline', ''), p.get('summary'), p.get('summary'),
                                           p.get('reason'), p.get('tier_reasoning'), p.get('link'), p),
                    'target': p,
                    'store': 'pin',
                }))
            entries.sort(key=lambda e: e[0] or '')
            changed = self._apply_event_rules([e for _, e in entries], log_loaded=True)
            if 'cache' in changed:
                atomic_write_json(gemini_cache_file, gemini_cache)
            if 'pin' in changed:
                self.save_pinned_stories(pinned_stories)
        except Exception as e:
            pulse_logger.log(f"⚠️ Event memory pass on cached/pinned items failed — keeping stored kind/tier: {e}",
                             level="WARNING")

    def fetch_news(self):
        # EC-survey recaps (Conference Board / UMich / JOLTS / claims / ADP /
        # ISM) are EC-owned — neutralize any cached classification for one
        # once per cycle so it can never come back as a Tier 1 Geo card.
        try:
            ec_cache_file = "/data/gemini_classifications.json"
            if os.path.exists(ec_cache_file):
                with open(ec_cache_file, 'r') as f:
                    ec_cache = json.load(f)
                ec_changed = False
                for ec_key, ec_rec in ec_cache.items():
                    if not isinstance(ec_rec, dict):
                        continue
                    if self._is_ec_survey_recap(ec_key) or self._is_ec_survey_recap(ec_rec):
                        ec_reject_log = "🚫 Geo EC-survey reject (cache row)"
                    elif self._is_aviation_incident(ec_key) or self._is_aviation_incident(ec_rec):
                        ec_reject_log = "🚫 Geo aviation-incident reject (cache row)"
                    else:
                        continue
                    if (ec_rec.get('relevant') is False and ec_rec.get('tier') is None
                            and ec_rec.get('kind') is None and ec_rec.get('gate_pass') is False):
                        continue
                    ec_rec['relevant'] = False
                    ec_rec['tier'] = None
                    ec_rec['kind'] = None
                    ec_rec['gate_pass'] = False
                    ec_changed = True
                    pulse_logger.log(f"{ec_reject_log}: '{str(ec_key)[:60]}'")
                if ec_changed:
                    atomic_write_json(ec_cache_file, ec_cache)
        except Exception as e:
            pulse_logger.log(f"⚠️ EC-survey cache purge failed: {e}", level="WARNING")
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

        # Pins are loaded here (rather than just before injection below) so
        # the event-memory pass can see them alongside cache hits.
        pinned_stories = self.load_pinned_stories()

        # Event memory + 14-day after-action cap over already-classified
        # entries (cache hits + pins), BEFORE known_relevant reads kind for
        # its age window and before anything is scored. Persists any change.
        self._apply_event_rules_to_known(items, gemini_cache, gemini_cache_file, pinned_stories)

        # Split into already classified vs new
        new_items = [i for i in items if i['headline'] not in gemini_cache]
        known_relevant = []
        for i in items:
            cached = gemini_cache.get(i['headline'], {})
            if cached.get('relevant') and cached.get('confidence', 0) >= 0.75:
                if self._clock_expired(cached):
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
                        # Event memory + 14-day cap: may overwrite r['kind'] (and
                        # cap r['tier']) in place BEFORE new_class is built, the
                        # cache is written, update_pinned_store() runs (which
                        # already skips kind == follow_up), and anything scores.
                        self._apply_event_rules(self._event_rule_entries_for_fresh(new_items, classifications))
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
                                if r.get('event_folded'):
                                    kind_display = f"{kind} (folded — original clock {r.get('clock_anchor')} +{r.get('clock_hours')}h, no vote)"
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
                                for f in ('event_rule', 'pre_rule_kind', 'pre_rule_tier',
                                          'event_folded', 'event_key', 'clock_anchor', 'clock_hours'):
                                    if f in r:
                                        new_class[f] = r[f]

                                # Duplicate-event check — same underlying story, just re-titled.
                                # Compared against the 15 most recently classified active
                                # (relevant, within-TTL) entries, not the full cache.
                                duplicate_of = None
                                if new_class['relevant']:
                                    active_entries = [
                                        (h, c) for h, c in gemini_cache.items()
                                        if h != headline and c.get('relevant')
                                        and not self._clock_expired(c)
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
                                                merged_class = self._guarded_duplicate_merge(existing, new_class, duplicate_of, headline)
                                                gemini_cache[duplicate_of] = merged_class
                                                gemini_cache[duplicate_of].pop('pending_merge', None)
                                                pulse_logger.log(f"🔁 Pinned entry updated after 2 agreeing merges: '{headline[:60]}' → '{duplicate_of[:60]}'")
                                                self._refresh_pin_classification(duplicate_of, merged_class, incoming_headline=headline)
                                            else:
                                                if pending:
                                                    pulse_logger.log(f"🔁 Pinned entry staged candidate cleared by disagreeing merge: '{duplicate_of[:60]}'")
                                                gemini_cache[duplicate_of]['pending_merge'] = new_class
                                                pulse_logger.log(f"🔁 Pinned entry candidate staged, awaiting confirmation: '{headline[:60]}' → '{duplicate_of[:60]}'")
                                        else:
                                            merged_class = self._guarded_duplicate_merge(existing, new_class, duplicate_of, headline)
                                            gemini_cache[duplicate_of] = merged_class
                                            pulse_logger.log(f"🔁 Duplicate event — updated existing entry in place: '{headline[:60]}' → merged into '{duplicate_of[:60]}'")
                                            self._refresh_pin_classification(duplicate_of, merged_class, incoming_headline=headline)
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
        event_folded_count = 0
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
            # Event-identity fold: same event as an earlier card, which
            # already carries the vote — this one is visible on the
            # original's clock but never votes a second time.
            if item.get('event_folded'):
                event_folded_count += 1
                continue
            if self._check_ec_fold(item, ec_anchors):
                item['kind'] = 'follow_up'  # already true by construction (condition d) — set explicitly for clarity
                item['ec_folded'] = True
                folded_count += 1
                continue
            if self._is_ec_survey_recap(item):
                pulse_logger.log(
                    f"🚫 Geo EC-survey reject (score skipped): {str(item.get('headline') or '')[:80]}"
                )
                continue
            if self._is_aviation_incident(item):
                pulse_logger.log(
                    f"🚫 Geo aviation-incident reject (score skipped): {str(item.get('headline') or '')[:80]}"
                )
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
        if event_folded_count:
            pulse_logger.log(f"🔗 Geo — {event_folded_count} article(s) excluded from score, folded into an earlier card's event (original clock, no second vote)")
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
                self._apply_event_rules(self._event_rule_entries_for_fresh(pending, classifications))
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
                        if r.get('event_folded'):
                            kind_display = f"{kind} (folded — original clock {r.get('clock_anchor')} +{r.get('clock_hours')}h, no vote)"
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
                        for f in ('event_rule', 'pre_rule_kind', 'pre_rule_tier',
                                  'event_folded', 'event_key', 'clock_anchor', 'clock_hours'):
                            if f in r:
                                gc[headline][f] = r[f]
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
            if self._is_ec_survey_recap(item):
                pulse_logger.log(
                    f"🚫 Geo EC-survey reject (cache drop): '{item.get('headline', '')[:60]}'"
                )
                updated += 1  # forces _refresh_cached_data() to recompute flags/score without it
                continue
            if self._is_aviation_incident(item):
                pulse_logger.log(
                    f"🚫 Geo aviation-incident reject (cache drop): '{item.get('headline', '')[:60]}'"
                )
                updated += 1  # forces _refresh_cached_data() to recompute flags/score without it
                continue
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
