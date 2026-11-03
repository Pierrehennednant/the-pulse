"""Replay 11/2 10:12 PM ET: new Tier 1 card "U.S. strikes Iranian targets"
(evt|e141f0b50870) had the Qeshm Island duplicate folded onto its event, then
the same-story duplicate merge copied the duplicate's event_folded/event_key
onto the survivor -> folded into its own event, Geo 0.0 instead of ~-1.45.
(a) merge never carries EVENT_STATE_FIELDS; survivor votes 1.7*0.85*-1.
(b) a stored self-fold is cleared + EVENT FOLD SELF-CLEARED logged.
(c) a normal fold onto an older live card still never votes.
(d) PINNED CARD NOT SCORED warning for a pinned T1 excluded card.
No /data reads or writes: pure method calls, event memory patched in."""
import os, sys, tempfile, pathlib, threading
from datetime import datetime, timezone, timedelta
os.chdir(tempfile.mkdtemp())
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pipelines.geopolitical as g

P = g.GeopoliticalPipeline.__new__(g.GeopoliticalPipeline)
P._event_logged = set()
P._event_memory_lock = threading.Lock()
LOGGED = []
_orig_log = g.pulse_logger.log
g.pulse_logger.log = lambda m, *a, **k: (LOGGED.append(m), _orig_log(m, *a, **k))
now = datetime.now(timezone.utc); today = now.date()
ORIG = "U.S. strikes Iranian targets"
DUP = "U.S. strikes Iranian targets on Qeshm Island"

def cls(kind, tier=1):
    return {'relevant': True, 'confidence': 0.85, 'direction': 'bearish', 'tier': tier,
            'kind': kind, 'gate_pass': True, 'classified_at': now.isoformat(), 'summary': 's'}

def entry(h, t):
    ctx = P._event_ctx(h, '', h, '', '', 'https://ex.com/' + str(abs(hash(h))), {'published_at': now.isoformat()})
    return {'ctx': ctx, 'target': t}

def fold_onto(target, d):
    # exactly what _apply_event_rules() writes for a fold decision
    target['event_folded'] = True
    target['event_key'] = d['fold']['event_key']
    target['clock_anchor'] = d['fold']['clock_anchor']
    target['clock_hours'] = d['fold']['clock_hours']

def item_from(h, c, pinned=True):
    it = {'headline': h, 'sentiment_score': -0.8, 'market_relevant': True, 'pinned': pinned}
    P._apply_classification_fields(it, c)
    return it

fail = 0
def check(name, ok):
    global fail; fail += not ok; print('PASS' if ok else 'FAIL', name)

mem = {}
survivor = cls('first_print')
P._event_decide(entry(ORIG, survivor), mem, now, today)
key = next(iter(mem))
check('setup: survivor created its own event row', mem[key]['sample_headline'] == ORIG)
P._load_event_memory = lambda: (mem, 0)

# (a) duplicate folds onto the survivor's event, then the merge runs — both
# merge branches (guarded follow_up echo, and unguarded first_print echo).
for echo_kind in ('follow_up', 'first_print'):
    dup = cls(echo_kind)
    d, _ = P._event_decide(entry(DUP, dup), mem, now, today)
    if not (d or {}).get('fold'):
        d = {'fold': {'event_key': key, 'clock_anchor': mem[key]['clock_start'], 'clock_hours': 48}}
    check(f'(a/{echo_kind}) duplicate folded into survivor event', d['fold']['event_key'] == key)
    fold_onto(dup, d)
    dup['reaction_only'] = True  # a price-reaction echo must not silence the event card either
    merged = P._guarded_duplicate_merge(dict(survivor), dup, ORIG, DUP)
    check(f'(a/{echo_kind}) survivor not folded after merge', not merged.get('event_folded'))
    check(f'(a/{echo_kind}) no event_key/clock/reaction_only copied',
          not any(k in merged for k in ('event_key', 'clock_anchor', 'clock_hours', 'reaction_only')))
    check(f'(a/{echo_kind}) survivor keeps first_print', merged.get('kind') == 'first_print')
    LOGGED.clear()
    score = P.calculate_score([item_from(ORIG, merged)], [])
    check(f'(a/{echo_kind}) survivor votes -1.45 (got {score})', abs(score - (-1.7 * 0.85)) < 0.011)
    check(f'(a/{echo_kind}) no NOT SCORED warning', not any('PINNED CARD NOT SCORED' in l for l in LOGGED))

# (b) a stored self-fold (the live bug shape) is cleared and logged once
bad = dict(survivor, event_folded=True, event_key=key, clock_anchor=mem[key]['clock_start'], clock_hours=48)
LOGGED.clear()
n = P._clear_self_folds([(ORIG, bad)], mem)
check('(b) self-fold cleared', n == 1 and not any(k in bad for k in g.GeopoliticalPipeline.EVENT_FOLD_FIELDS))
line = [l for l in LOGGED if l.startswith('EVENT FOLD SELF-CLEARED')]
check('(b) SELF-CLEARED line logged', line == [f"EVENT FOLD SELF-CLEARED | key={key} | headline={ORIG}"])
for l in line: print('  log:', l)
P._event_logged.clear(); LOGGED.clear()
bad_item = item_from(ORIG, dict(survivor, event_folded=True, event_key=key))
score = P.calculate_score([bad_item], [])
check(f'(b) scoring-time heal: votes -1.45 (got {score})', abs(score - (-1.445)) < 0.011)
check('(b) scoring-time SELF-CLEARED logged', any(l.startswith('EVENT FOLD SELF-CLEARED') for l in LOGGED))

# (c) normal fold onto an OLDER live card: stays folded, never votes
older = dict(cls('follow_up'), event_folded=True, event_key=key,
             clock_anchor=(now - timedelta(hours=3)).isoformat(), clock_hours=48)
check('(c) not treated as self-fold', P._clear_self_folds([(DUP, older)], mem) == 0 and older.get('event_folded'))
LOGGED.clear()
score = P.calculate_score([item_from(DUP, older)], [])
check(f'(c) folded card does not vote (got {score})', score == 0.0)

# (d) pinned T1 excluded -> warning; unpinned excluded -> no warning
w = [l for l in LOGGED if 'PINNED CARD NOT SCORED' in l]
check('(d) warning fired for pinned T1 fold', w == [f"⚠️ PINNED CARD NOT SCORED | tier=1 | reason=event_folded | headline={DUP}"])
for l in w: print('  log:', l)
LOGGED.clear()
P.calculate_score([item_from(DUP, older, pinned=False)], [])
check('(d) no warning when not pinned', not any('PINNED CARD NOT SCORED' in l for l in LOGGED))
LOGGED.clear()
P.calculate_score([item_from(DUP, dict(cls('first_print'), reaction_only=True))], [])
check('(d) reaction_only reason', any('reason=reaction_only' in l for l in LOGGED))
# (e) on-disk heal: _apply_event_rules_to_known clears the live bug shape in
# the cache entry and a legacy pin (no event_key) and persists — temp files only.
import json
cache_file = os.path.join(os.getcwd(), 'gc.json')
gc = {ORIG: dict(survivor, event_folded=True, event_key=key, clock_anchor=mem[key]['clock_start'], clock_hours=48)}
pins = [dict(survivor, headline=ORIG, event_folded=True)]
saved = []
P.save_pinned_stories = lambda ps: saved.append([dict(x) for x in ps])
P._apply_event_rules = lambda entries, log_loaded=False: set()
P._event_logged.clear()
P._apply_event_rules_to_known([{'headline': ORIG, 'link': 'https://ex.com/x'}], gc, cache_file, pins)
disk = json.load(open(cache_file)) if os.path.exists(cache_file) else {}
check('(e) cache entry healed + persisted', not disk.get(ORIG, {}).get('event_folded') and ORIG in disk)
check('(e) legacy pin healed + saved', saved and not saved[-1][0].get('event_folded'))
# (f) live 11/2 shape: Tier 1 bearish 0.85 first_print survivor, Tier 3
# neutral follow_up echo ("restates the already-scored Qeshm strike") folded
# onto its event and merged. Survivor keeps its read, is not folded, votes.
orig_read = dict(survivor, tier_reasoning='New kinetic exchange with Iran in the Strait of Hormuz area', text_source='full_text')
echo = dict(cls('follow_up', tier=3), direction='neutral', confidence=0.8,
            tier_reasoning='restates the already-scored Qeshm strike', summary='echo', text_source='description_fallback')
fold_onto(echo, {'fold': {'event_key': key, 'clock_anchor': mem[key]['clock_start'], 'clock_hours': 48}})
m = P._guarded_duplicate_merge(dict(orig_read), echo, ORIG, DUP)
check('(f) keeps Tier 1 bearish 0.85 first_print',
      (m['tier'], m['direction'], m['confidence'], m['kind']) == (1, 'bearish', 0.85, 'first_print'))
check('(f) keeps own reasoning/summary/src', m['tier_reasoning'] == orig_read['tier_reasoning']
      and m['summary'] == 's' and m['text_source'] == 'full_text')
check('(f) not folded', not m.get('event_folded') and 'event_key' not in m)
LOGGED.clear()
score = P.calculate_score([item_from(ORIG, m)], [])
check(f'(f) votes -1.44 (got {score})', score == -1.44)
# non-guarded merge (first_print echo) still refreshes the read (Sep 18 rule)
m2 = P._guarded_duplicate_merge(dict(orig_read), dict(echo, kind='first_print'), ORIG, DUP)
check('(f) unguarded merge still takes the new read, never the fold', m2['tier'] == 3 and not m2.get('event_folded'))

# (g) heal the already-corrupted live entry: cache holds the echo's read +
# self-fold; the pin (created from the survivor's own read) restores it.
gc2 = {ORIG: dict(echo)}
pins2 = [dict(orig_read, headline=ORIG)]
P._event_logged.clear(); LOGGED.clear()
P._apply_event_rules_to_known([{'headline': ORIG, 'link': 'https://ex.com/x'}], gc2, cache_file, pins2)
h = gc2[ORIG]
check('(g) cache entry restored T1 bearish 0.85 first_print, unfolded',
      (h['tier'], h['direction'], h['confidence'], h['kind'], h.get('event_folded')) == (1, 'bearish', 0.85, 'first_print', None))
rl = [l for l in LOGGED if l.startswith('EVENT FOLD SELF-CLEARED')]
check('(g) SELF-CLEARED line with restored=pin', any('restored=pin T1 bearish conf=0.85' in l for l in rl))
for l in rl: print('  log:', l)
score = P.calculate_score([item_from(ORIG, h)], [])
check(f'(g) healed card votes -1.44 (got {score})', score == -1.44)
check('central set feeds merge guard', set(g.GeopoliticalPipeline.EVENT_STATE_FIELDS) <= set(g.GeopoliticalPipeline.MERGE_KIND_GUARD_KEEP_FIELDS))
sys.exit(fail)
