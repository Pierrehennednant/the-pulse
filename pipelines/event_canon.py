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


def compute_event_id(actor, action_category, place, event_date_iso):
    """event_id = hash(canonical_actor + action_category + canonical_place
    + UTC date of event_time). Never the URL, never the headline.

    Returns None if actor, place, or event_date_iso canonicalizes to
    nothing — callers must treat that as "cannot establish identity" and
    apply the "default to follow_up, first print is earned" rule rather
    than hashing an incomplete key (an incomplete key is exactly the
    "hash too loose" failure mode: two unrelated articles with the same
    missing fields would otherwise collide)."""
    canon_actor = normalize_entity(actor)
    canon_place = normalize_entity(place)
    canon_action = normalize_action(action_category)
    date_str = (event_date_iso or '').strip()[:10]  # YYYY-MM-DD only

    if not canon_actor or not canon_place or not date_str:
        return None

    key = f"{canon_actor}|{canon_action}|{canon_place}|{date_str}"
    return hashlib.sha256(key.encode('utf-8')).hexdigest()[:24]


def log_canonicalization_miss(raw, cleaned):
    pulse_logger.log(
        f"🔍 Event canon — no alias for {raw!r} (cleaned: {cleaned!r}), "
        f"using cleaned string as canonical form", level="INFO"
    )
