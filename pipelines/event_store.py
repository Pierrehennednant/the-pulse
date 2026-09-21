"""
Persistent, code-owned event-identity store — Stage 2 of the structural
first_print/follow_up rewrite. This is the thing Haiku never gets to see:
a real record of what's already been scored, so the dedup decision lives
in code against actual state instead of inside a single article's text.

Schema per record:
  event_id, actor, action, place, event_time, first_seen, record_kind,
  score_applied

NAMING NOTE, added after an audit flagged the collision: this schema's
field was originally named plain 'kind', matching the spec's field list
literally. Renamed to `record_kind` because pipelines/geopolitical.py has
its own, completely different 'kind' concept (an article's first_print/
follow_up classification, read/written in ~20 places, driving scoring and
TTL there) — and this store's field is NOT that. record_first_print() is
the only writer, and it always sets record_kind to the literal constant
'first_print' (a record only ever gets created for a first_print event by
definition); nothing anywhere reads it back to branch on. It exists
purely to document, for a human reading a dumped record, why this entry
exists — not as a live signal to consume. Same field name as the live
article-kind elsewhere in the Geo pillar would have invited a future
reader to assume they're the same thing and try to branch on it.

`actor`/`action`/`place` stored here are the CANONICALIZED forms (see
pipelines/event_canon.py) — this store is the read side of the identity
the hash was built from, not a place to re-derive or reinterpret raw
article text.

TWO-STEP WRITE, NOT ONE — a design point worth stating explicitly rather
than leaving implicit: at the moment code discovers a store-miss (a
genuine new event), Pass B (tier/direction/confidence -> the actual score)
has not run yet — that's the whole point of the two-step Haiku split, Pass
B only runs after kind is already resolved to first_print. So the write
happens in two calls, not one:
  1. record_first_print(event_id, actor, action, place, event_time) —
     called immediately on a store-miss, BEFORE Pass B runs, with
     score_applied left null. This claims the identity right away so a
     second article about the identical event arriving later in the same
     cycle also sees a hit, not a second miss.
  2. update_score_applied(event_id, score) — called once Pass B actually
     produces a score for that first_print item, filling in the field.
A record can therefore legitimately sit with score_applied=null between
those two calls within a single cycle — callers must not treat null as
an error, only as "claimed, not yet scored."

TTL: 48 hours from first_seen, checked lazily at lookup time and pruned
from the persisted file whenever a lookup or write actually finds expired
entries to drop (no separate scheduled sweep — same convention as this
project's other TTL-bearing stores, e.g. pinned stories). Fails CLOSED on
a missing/unparseable first_seen (treated as expired), matching
_pin_is_expired()/is_article_too_old() elsewhere in this project: a
corrupted timestamp must never grant an entry an indefinite free pass to
keep suppressing real new articles as follow_up forever.
"""
import json
import os
from datetime import datetime, timezone

from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger

EVENT_STORE_FILE = '/data/geo_event_store.json'
EVENT_TTL_HOURS = 48


class EventStore:
    def __init__(self, path=EVENT_STORE_FILE, ttl_hours=EVENT_TTL_HOURS):
        self.path = path
        self.ttl_hours = ttl_hours

    def _load_all(self):
        try:
            if not os.path.exists(self.path):
                return {}
            with open(self.path, 'r') as f:
                return json.load(f)
        except Exception as e:
            pulse_logger.log(f"⚠️ Event store — failed to load {self.path}: {e}", level="WARNING")
            return {}

    def _save_all(self, records):
        try:
            atomic_write_json(self.path, records)
        except Exception as e:
            pulse_logger.log(f"⚠️ Event store — failed to write {self.path}: {e}", level="WARNING")

    def _is_expired(self, record):
        ts = record.get('first_seen', '')
        if not ts:
            return True
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return True
        age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        return age_hours > self.ttl_hours

    def _prune_expired(self):
        """Drop expired entries and persist only if something was actually
        removed (a pure-hit lookup never triggers a disk write)."""
        records = self._load_all()
        kept = {eid: r for eid, r in records.items() if not self._is_expired(r)}
        if len(kept) != len(records):
            dropped = len(records) - len(kept)
            self._save_all(kept)
            pulse_logger.log(
                f"🧹 Event store — pruned {dropped} expired entr{'y' if dropped == 1 else 'ies'} (48h TTL)"
            )
        return kept

    def lookup(self, event_id):
        """Return the stored record for event_id if present and still
        within TTL, else None. None means "no matching event on record" —
        the caller still applies its own default-to-follow_up rule for
        missing/failed extraction separately; this method only answers
        "does a matching event already exist in the store."""
        if not event_id:
            return None
        return self._prune_expired().get(event_id)

    def record_first_print(self, event_id, actor, action, place, event_time):
        """Claim a new event identity as first_print. Called immediately
        on a store-miss, BEFORE Pass B has produced a score — see the
        module docstring's "two-step write" note. score_applied starts
        null; fill it in via update_score_applied() once Pass B runs.
        Overwrites any existing entry for the same event_id rather than
        raising — should not happen in normal operation (callers check
        lookup() first) but keeps this method simple and idempotent for
        tests/backfills."""
        if not event_id:
            raise ValueError("event_store.record_first_print() requires a non-empty event_id")
        records = self._load_all()
        records[event_id] = {
            'event_id': event_id,
            'actor': actor,
            'action': action,
            'place': place,
            'event_time': event_time,
            'first_seen': datetime.now(timezone.utc).isoformat(),
            'record_kind': 'first_print',
            'score_applied': None,
        }
        self._save_all(records)
        pulse_logger.log(
            f"📌 Event store — first_print claimed | {event_id} | {actor} / {action} / {place} @ {event_time}"
        )

    def update_score_applied(self, event_id, score):
        """Fill in score_applied once Pass B has actually scored the
        first_print item this event_id belongs to. No-ops (logs a
        warning) if the event_id isn't on record or has since expired —
        this should not happen in normal same-cycle operation, but
        failing loudly-but-non-fatally here is safer than raising mid
        score-calculation."""
        records = self._load_all()
        record = records.get(event_id)
        if not record or self._is_expired(record):
            pulse_logger.log(
                f"⚠️ Event store — update_score_applied() called for unknown/expired event_id {event_id!r}, ignored",
                level="WARNING"
            )
            return
        record['score_applied'] = score
        self._save_all(records)


event_store = EventStore()
