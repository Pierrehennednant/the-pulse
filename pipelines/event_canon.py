"""
Event canonicalization — the required design decision from the structural
first_print/follow_up rewrite spec, resolved BEFORE the event store or the
Pass A/B Haiku restructuring is built (per spec: "STOP — RESOLVE THIS
BEFORE WRITING THE STORE").

THE PROBLEM
-----------
event_id = hash(actor + action + place + UTC date of event_time) only
works if actor/action/place are canonicalized before hashing. Two articles
about the identical event can otherwise produce different raw strings —
"Federal Reserve" vs "Kevin Warsh" for the same press conference, "raised
rates" vs "hiked" vs "increased the federal funds rate" for the same
decision — and the hash diverges, silently recreating the exact dedup bug
this rewrite exists to kill, just moved from Haiku's judgment into
extraction inconsistency. Too loose (real distinct events collide) and too
tight (same event never matches twice) are both real failure modes here,
not hypothetical.

THE APPROACH CHOSEN, AND WHY NOT THE ALTERNATIVES
---------------------------------------------------
Three options were on the table: (1) a closed controlled vocabulary Pass A
must pick from, (2) a code-side normalization/alias table, (3) embedding-
similarity matching. This module uses a HYBRID of (1) and (2), NOT (3):

  - ACTION is (1), a closed enum (ACTION_CATEGORIES below) Pass A must
    select from — never freeform verb text. This is what actually kills
    "raised/hiked/increased" drift: the category is chosen at extraction
    time from a fixed small list, the same pattern this codebase already
    uses for `bucket` (geo|macro|ec|drop). There is no verb-alias table to
    maintain because Pass A never emits a raw verb into the hash input at
    all.

  - ACTOR and PLACE are (2), free-text extraction (a closed enum can't
    enumerate every country/institution/person up front — it would be
    either too large to prompt with or would silently reject legitimate
    novel actors) followed by CODE-SIDE normalization: lowercase, strip
    punctuation, strip a small honorific-prefix list ("president ",
    "chairman ", "fed chair ", ...), then an alias-table lookup (ALIASES
    below) collapsing many surface forms to one canonical slug. Unaliased
    input falls back to its own cleaned string — imperfect, but no worse
    than not canonicalizing at all, and the miss is logged (see
    log_canonicalization_miss) so the table grows from real observed
    misses, the same way FOMC_FOLD_PRIMARY_PHRASES and the EC/geo
    blocklists already grow in this project.

  - Embedding-similarity matching (3) was deliberately NOT chosen: it adds
    a new ML dependency this project doesn't otherwise carry, replaces one
    hard-to-tune threshold (the alias table's coverage gaps) with another
    (a similarity cutoff), and is not unit-testable without a live model
    call — the alias-table approach is fully deterministic and testable
    with zero live API access, matching how every other matcher in this
    codebase (FOMC fold, blocklists, tier keyword fallback) already works.

THE SPECIAL CASE THIS DOESN'T TRY TO SOLVE: SCHEDULED/EC EVENTS
-----------------------------------------------------------------
The concrete example that motivated the "Federal Reserve" vs "Kevin
Warsh" warning — an FOMC press conference recap — is a scheduled,
calendar-anchored event that the Economic Calendar pillar already tracks
under a fixed, tiny, already-controlled vocabulary (FOMC_FOLD_EVENT_TITLES
in geopolitical.py: 'Federal Funds Rate', 'FOMC Statement', 'FOMC Press
Conference', 'FOMC Economic Projections'). Asking freeform actor/action/
place extraction to reconstruct that same identity from article prose is
strictly worse than just using the EC's own event_date + title, which is
exactly what the existing EC-fold mechanism (_ec_fold_matches /
_check_ec_fold / _reevaluate_pinned_ec_folds) already does. RECOMMENDATION
for the Stage 3 Pass A/B wiring (not built yet, see the staging note in
the accompanying message): when bucket == 'ec' or an item matches the
existing FOMC_FOLD_PRIMARY_PHRASES pattern, defer to the EC-fold's own
event identity (event_date + EC title) instead of running it through this
module's freeform actor/action/place hash at all. This module intentionally
does not special-case Fed/rate-decision actors in ALIASES for that reason
— that class of event should never reach this hash path in the first
place, not be patched around inside it.

RESIDUAL RISK, STATED PLAINLY
------------------------------
A genuinely novel actor/place spelling not yet in ALIASES will still hash
inconsistently across two differently-worded articles about the same
event — this can only be fully closed by observing real misses and
extending the table, the same maintenance model as every other matcher in
this codebase. It is not eliminated by this design, only bounded and made
visible (logged) instead of silent.
"""
import hashlib
import re
import unicodedata

from utils.logger import pulse_logger

# Closed enum Pass A must select `action` from — never freeform verb text.
# This is the primary defense against verb-phrasing drift ("raised" vs
# "hiked" vs "increased the federal funds rate"): the category is chosen
# at extraction time, not derived from the raw verb after the fact.
ACTION_CATEGORIES = (
    'military_action',       # strikes, attacks, airstrikes, ground offensives
    'missile_test',          # missile/rocket launches, weapons tests
    'troop_movement',        # deployments, buildups, withdrawals
    'sanction_imposed',      # new sanctions, export controls, asset freezes
    'diplomatic_statement',  # official statements, threats, quotes, pressure campaigns
    'ceasefire_or_deal',     # ceasefires, peace deals, agreements signed
    'trade_policy',          # tariffs, trade deals, trade restrictions
    'rate_decision',         # central bank rate decisions/announcements
    'economic_data',         # scheduled data releases (jobs, CPI, PMI, etc.)
    'market_commentary',     # feature/synthesis/analysis/price-tape framing
    'corporate_deal',        # Path 2 only: tech/AI-infrastructure M&A, partnership,
                             # or a hyperscaler's own capex commitment. Needed because
                             # 'other' is never stored as Geo under the family map.
    'corporate_distress',    # bankruptcy/Chapter 11, default, going-concern warning,
                             # accounting fraud, bank failure/deposit run, emergency
                             # government rescue — added for the corporate-events
                             # PATH 1 policy (2026-09-23); no prior category covered
                             # this action space at all.
    'other',
)

# Honorific / title prefixes stripped before alias lookup, so "President
# Trump" and "Trump" resolve the same way without needing every
# honorific+name combination spelled out in ALIASES individually.
_HONORIFIC_PREFIXES = (
    'president ', 'chairman ', 'chair ', 'fed chair ', 'prime minister ',
    'pm ', 'general secretary ', 'secretary ', 'minister ', 'general ',
    'supreme leader ', 'ayatollah ', 'foreign minister ', 'defense minister ',
    'spokesman ', 'spokesperson ', 'governor ',
)

# Seed alias table — deliberately small and scoped to actors/places that
# have actually come up in this project's real incidents so far. Grows
# incrementally from observed canonicalization misses (see
# log_canonicalization_miss), the same maintenance model as
# FOMC_FOLD_PRIMARY_PHRASES and the EC/geo blocklists.
#
# Fed/rate-decision actors are deliberately NOT included here — see the
# module docstring's "special case this doesn't try to solve" section.
# Those should defer to the EC-fold's own event identity, not this table.
ALIASES = {
    # United States
    'white house': 'united_states', 'washington': 'united_states',
    'trump administration': 'united_states', 'trump': 'united_states',
    'donald trump': 'united_states', 'donald trump administration': 'united_states',
    'donald j trump': 'united_states',
    'u.s.': 'united_states', 'us': 'united_states', 'usa': 'united_states',
    'united states': 'united_states', 'america': 'united_states',

    # Russia
    'kremlin': 'russia', 'moscow': 'russia', 'russian government': 'russia',
    'putin': 'russia',

    # Iran — IRGC/Revolutionary Guard collapse into the same 'iran' slug as
    # Tehran, not a separate 'iran_irgc' form. Deliberate, not an oversight:
    # this table already collapses a state's military arm into the state
    # itself elsewhere (see Israel below — 'idf'/'israel defense forces'
    # both map to 'israel', not a separate slug). Splitting Iran alone
    # into two canonical forms broke that same precedent and reproduced
    # the exact class of bug this module exists to prevent (see the module
    # docstring's Federal Reserve/Kevin Warsh example) — two articles
    # about the same event, one saying "Tehran" and the other "IRGC",
    # would canonicalize to different actors and both first_print.
    'tehran': 'iran', 'iran': 'iran',
    'irgc': 'iran', 'islamic revolutionary guard corps': 'iran',
    'revolutionary guard': 'iran', 'revolutionary guards': 'iran',

    # Israel
    'idf': 'israel', 'israel defense forces': 'israel', 'jerusalem': 'israel',
    'netanyahu': 'israel',

    # North Korea
    'pyongyang': 'north_korea', 'dprk': 'north_korea',
    "north korea's kim": 'north_korea', 'kim jong un': 'north_korea',
    'kim regime': 'north_korea',

    # Places seen in real incidents
    'strait of hormuz': 'strait_of_hormuz', 'hormuz': 'strait_of_hormuz',
    'bahrain': 'bahrain',
}

_PUNCT_RE = re.compile(r"[^\w\s]")
_WS_RE = re.compile(r"\s+")


def _clean(raw):
    """Lowercase, strip diacritics/punctuation, collapse whitespace."""
    if not raw:
        return ''
    text = unicodedata.normalize('NFKD', raw).encode('ascii', 'ignore').decode('ascii')
    text = text.lower().strip()
    text = _PUNCT_RE.sub(' ', text)
    text = _WS_RE.sub(' ', text).strip()
    return text


def normalize_entity(raw):
    """Canonicalize an actor or place string for hashing. Deterministic,
    no live calls. Returns '' for empty input (caller decides how to treat
    that — see the "default to follow_up" rule for missing fields)."""
    cleaned = _clean(raw)
    if not cleaned:
        return ''
    for prefix in _HONORIFIC_PREFIXES:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break
    if cleaned in ALIASES:
        return ALIASES[cleaned]
    # Unaliased — fall back to the cleaned string itself (imperfect, but
    # consistent for two articles that happen to phrase it the same way;
    # logged so real misses are visible and the table can grow).
    log_canonicalization_miss(raw, cleaned)
    return cleaned.replace(' ', '_')


def normalize_action(raw_category):
    """Validate `action` is one of the closed ACTION_CATEGORIES. Pass A is
    expected to emit one of these directly (not freeform verb text) — this
    is a defensive check, not the primary normalization step, since the
    enum constraint is meant to happen at extraction time."""
    cleaned = (raw_category or '').strip().lower().replace(' ', '_')
    if cleaned in ACTION_CATEGORIES:
        return cleaned
    pulse_logger.log(
        f"⚠️ Event canon — action category {raw_category!r} not in ACTION_CATEGORIES, "
        f"falling back to 'other'", level="WARNING"
    )
    return 'other'


# =====================================================================
# FOUR-FIELD CANON (cc_v2_four_field_canon, 2026-09-23)
# Coarse key in the hash, tolerance at lookup. The hash is exact-match
# only, so it takes coarse canonical fields; is_same_event() below does the
# tolerant matching (actor subset, +/-1 day, venue vs home place, object
# overlap) that a hash can't.
# =====================================================================

# FIELD 2 — action family. The fine action stays on the record; the family
# goes in the hash. None = never hashed as Geo, never stored.
ACTION_FAMILIES = {
    'military_action': 'kinetic', 'missile_test': 'kinetic', 'troop_movement': 'kinetic',
    'strike': 'kinetic', 'seizure': 'kinetic',
    'diplomatic_statement': 'diplomacy', 'ceasefire_or_deal': 'diplomacy', 'talks': 'diplomacy',
    'sanction_imposed': 'sanction', 'export_control': 'sanction',
    'trade_policy': 'trade',
    'rate_decision': 'monetary',
    'corporate_distress': 'distress',
    'corporate_deal': 'corporate',
    'market_commentary': None, 'economic_data': None, 'other': None,
}

# FIELD 3 — venue/byline places. A place made only of these has no
# country and keys as VENUE_ONLY, never folded into a country.
VENUE_ONLY = '__venue_only__'
VENUES = frozenset({
    'united_nations', 'un', 'united_nations_general_assembly', 'un_general_assembly',
    'unga', 'un_security_council', 'united_nations_security_council',
    'new_york', 'geneva', 'davos',
})

OBJECT_STOPWORDS = frozenset({
    'a', 'an', 'the', 'of', 'on', 'in', 'to', 'for', 'with', 'and', 'or', 'at', 'by',
    'from', 'its', 'their', 'his', 'her', 'over', 'about', 'into', 'new', 'amid',
    'after', 'before', 'between', 'against', 'as', 'is', 'are', 'was', 'be',
})
_DEMONYMS = {
    'iranian': 'iran', 'russian': 'russia', 'israeli': 'israel', 'chinese': 'china',
    'ukrainian': 'ukraine', 'american': 'united_states', 'us': 'united_states',
    'danish': 'denmark', 'greenlandic': 'greenland', 'saudi': 'saudi_arabia',
    'houthi': 'houthis', 'korean': 'korea',
}
OBJECT_MATCH_THRESHOLD = 0.5
DATE_TOLERANCE_DAYS = 1

_SPLIT_RE = re.compile(r',|&|\band\b', re.IGNORECASE)


def _canon_token(raw):
    """normalize_entity() without the miss log — lookup re-canonicalizes
    every stored record on every scan, which would otherwise flood logs."""
    cleaned = _clean(raw)
    if not cleaned:
        return ''
    for prefix in _HONORIFIC_PREFIXES:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
            break
    return ALIASES.get(cleaned, cleaned.replace(' ', '_'))


def action_family(action):
    """FIELD 2. Family for an action, or None if it is never Geo."""
    cleaned = (action or '').strip().lower().replace(' ', '_')
    return ACTION_FAMILIES.get(cleaned)


def actor_set(actor):
    """FIELD 1. (sorted tuple of canonical actor slugs, primary actor).
    Split on commas / 'and' / '&', alias each token, dedupe, sort. Primary
    is the first-listed actor, kept for the lookup's shared-primary rule."""
    tokens = [_canon_token(t) for t in _SPLIT_RE.split(actor or '')]
    tokens = [t for t in tokens if t]
    if not tokens:
        return (), ''
    return tuple(sorted(set(tokens))), tokens[0]


def place_key(place):
    """FIELD 3. Country key(s) for a place, VENUE_ONLY if every part is a
    venue/byline, '' if empty. Raw place stays on the record."""
    parts = [_canon_token(t) for t in _SPLIT_RE.split(place or '')]
    parts = [t for t in parts if t]
    if not parts:
        return ''
    countries = sorted({t for t in parts if t not in VENUES})
    return '+'.join(countries) if countries else VENUE_ONLY


def object_key(obj):
    """Canonical key-noun tokens of the object (what the action is
    about): lowercase, drop stopwords, map demonyms, crude singular.
    Returns a sorted tuple, or None when the object is unknown/empty."""
    cleaned = _clean(obj)
    if not cleaned:
        return None
    toks = set()
    for t in cleaned.split():
        if t in OBJECT_STOPWORDS:
            continue
        t = _DEMONYMS.get(t, t)
        if len(t) > 3 and t.endswith('s') and not t.endswith('ss'):
            t = t[:-1]
        toks.add(t)
    return tuple(sorted(toks)) or None


def object_overlap(a, b):
    """Overlap coefficient |A∩B| / min(|A|,|B|) on object key tokens."""
    if not a or not b:
        return 0.0
    a, b = set(a), set(b)
    return len(a & b) / min(len(a), len(b))


def canonical_fields(actor, action, place, event_time, obj=None, stored_object_key=None):
    """All four coarse fields plus the object key, as one dict. Works the
    same for a fresh Pass A extract and for a stored (possibly legacy)
    record, so lookup can re-canonicalize old records with current rules."""
    actors, primary = actor_set(actor)
    pk = place_key(place)
    ok = tuple(stored_object_key) if stored_object_key else object_key(obj)
    return {
        'actors': actors,
        'primary': primary,
        'family': action_family(action),
        'place_key': pk,
        # The place tells you nothing about WHICH event when it is a venue
        # or just the (primary) actor's own country.
        'uninformative_place': pk == VENUE_ONLY or (bool(pk) and pk == primary),
        'date': (event_time or '').strip()[:10],
        'object_key': ok,
    }


def coarse_key(c):
    """The hashed key: actor set | family | place key | date."""
    if not c['actors'] or not c['family'] or not c['place_key'] or not c['date']:
        return None
    return f"{'+'.join(c['actors'])}|{c['family']}|{c['place_key']}|{c['date']}"


def compute_event_id(actor, action_category, place, event_date_iso, obj=None):
    """event_id = hash(actor set | action family | place key | event_time
    date). Never the URL, the headline, or first_seen; the object is NOT in
    the hash (it's checked at lookup by the over-merge guard).

    Returns None when the key can't be formed — a missing actor/place/date,
    or an action whose family is never Geo (market_commentary,
    economic_data, other). Callers treat None as "no identity": drop."""
    key = coarse_key(canonical_fields(actor, action_category, place, event_date_iso, obj))
    if key is None:
        return None
    return hashlib.sha256(key.encode('utf-8')).hexdigest()[:24]


def _key_discriminates(c):
    """Without an object, can the coarse key tell events apart on its own?
    Not for diplomacy keyed on a venue or the actor's own country — that is
    the spec's own over-merge case ("Trump / diplomacy / united_states /
    Sep 21" would swallow every Trump statement that day)."""
    return not (c['family'] == 'diplomacy' and c['uninformative_place'])


def _dates_within(d1, d2, days):
    from datetime import date
    try:
        a, b = date.fromisoformat(d1), date.fromisoformat(d2)
    except ValueError:
        return False
    return abs((a - b).days) <= days


def is_same_event(new, old):
    """LOOKUP RULE. Is `new` (canonical fields) the same event as the
    stored `old`? Returns (bool, reason).

    Always required: same family, dates within +/-1 day, and actor sets
    compatible (one a subset of the other, or a shared primary actor).

    Both objects known -> places equal, or both uninformative (venue vs the
    actor's home datelines, spec test 4); then the OVER-MERGE GUARD: object
    key overlap must reach OBJECT_MATCH_THRESHOLD.

    Either object unknown (legacy record) -> the guard can't run, so:
    places must be equal and the key must discriminate on its own (see
    _key_discriminates). A new known-object claim merging into an
    unknown-object legacy record additionally needs an EXACT match on actor
    set and date (addendum item 2)."""
    if not new['family'] or new['family'] != old['family']:
        return False, 'family differs'
    if not _dates_within(new['date'], old['date'], DATE_TOLERANCE_DAYS):
        return False, 'date outside +/-1 day'
    na, oa = set(new['actors']), set(old['actors'])
    if not (na and oa and (na <= oa or oa <= na or new['primary'] == old['primary'])):
        return False, 'actors not compatible'

    new_known = new['object_key'] is not None
    old_known = old['object_key'] is not None
    places_equal = new['place_key'] == old['place_key']

    if new_known and old_known:
        if not (places_equal or (new['uninformative_place'] and old['uninformative_place'])):
            return False, 'place differs'
        overlap = object_overlap(new['object_key'], old['object_key'])
        if overlap < OBJECT_MATCH_THRESHOLD:
            return False, f'over-merge guard: object overlap {overlap:.2f}'
        return True, f'object overlap {overlap:.2f}'

    if not places_equal:
        return False, 'place differs (object unknown)'
    if not _key_discriminates(new):
        return False, 'object unknown and key does not discriminate (diplomacy at venue/home)'
    if new_known != old_known and (na != oa or new['date'] != old['date']):
        return False, 'known object vs legacy record needs exact actor+date match'
    return True, 'coarse match, object unknown, discriminating key'


def log_canonicalization_miss(raw, cleaned):
    pulse_logger.log(
        f"🔍 Event canon — no alias for {raw!r} (cleaned: {cleaned!r}), "
        f"using cleaned string as canonical form", level="INFO"
    )
