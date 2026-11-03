"""Replay 11/3 ET: Tier 2 card 'Oil prices climb after U.S. reportedly
strikes Iran's Qeshm Island' (reasoning: 'price reaction capped at Tier 2')
voted ~8:04-11:05 AM ET because the reaction_only self-heal ran only at
startup. Now ONE shared path (_reconcile_scoring_state) runs at startup and
on every fetch() refresh, plus _reconcile_served_items on the cached result.
(1) price-reaction card scored on cycle 1 stops voting on the next refresh
    (pins, cache and served items);
(2) the real T1 strike card is untouched and still votes -1.44;
(3) repeat cycles log zero reconcile lines and write nothing;
(4) moved tier-claim reconcile: a mid-session tier mismatch heals on refresh;
(5) startup wrapper keeps its log text + RECONCILE TIER summary.
No /data reads or writes: pins, cache file and writes are all patched."""
import os, sys, io, json, tempfile, pathlib, builtins
from datetime import datetime, timezone
os.chdir(tempfile.mkdtemp())
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pipelines.geopolitical as g

P = g.GeopoliticalPipeline.__new__(g.GeopoliticalPipeline)
LOGGED = []
_orig_log = g.pulse_logger.log
g.pulse_logger.log = lambda m, *a, **k: (LOGGED.append(m), _orig_log(m, *a, **k))
CACHE_FILE = "/data/gemini_classifications.json"
STORE = {'pins': [], 'cache': {}}
WRITES = []
P.load_pinned_stories = lambda: [dict(p) for p in STORE['pins']]
P.save_pinned_stories = lambda ps: (WRITES.append('pins'), STORE.__setitem__('pins', [dict(p) for p in ps]))
def _aw(path, data):
    assert path == CACHE_FILE, path
    WRITES.append('cache'); STORE['cache'] = json.loads(json.dumps(data))
g.atomic_write_json = _aw
_real_exists, _real_open = os.path.exists, builtins.open
g.os.path.exists = lambda p: True if p == CACHE_FILE else _real_exists(p)
def _open(p, *a, **k):
    if p == CACHE_FILE: return io.StringIO(json.dumps(STORE['cache']))
    return _real_open(p, *a, **k)
g.open = _open  # module-level shadow of builtin open, this test only

now = datetime.now(timezone.utc).isoformat()
STRIKE = "U.S. strikes Iranian targets"
OIL = "Oil prices climb after U.S. reportedly strikes Iran's Qeshm Island"
def cls(tier, reasoning, kind='first_print', conf=0.85):
    return {'headline': None, 'relevant': True, 'confidence': conf, 'direction': 'bearish', 'tier': tier,
            'kind': kind, 'gate_pass': True, 'classified_at': now, 'summary': 's',
            'tier_reasoning': reasoning}
STRIKE_C = dict(cls(1, "Active U.S. kinetic strikes on Iranian territory; Tier 1."), headline=STRIKE)
OIL_C = dict(cls(2, "Primary subject is the oil price move; price reaction capped at Tier 2."), headline=OIL)

def item_from(c):
    it = {'headline': c['headline'], 'sentiment_score': -0.8, 'market_relevant': True, 'pinned': True}
    P._apply_classification_fields(it, c)
    return it
def score_pins():
    return P.calculate_score([item_from(p) for p in STORE['pins']], [])

fail = 0
def check(name, ok):
    global fail; fail += not ok; print('PASS' if ok else 'FAIL', name)
def rec_lines():
    return [l for l in LOGGED if 'reconcile' in l.lower() or 'RECONCILE' in l]

# ---- cycle 0: only the strike card ----
STORE['pins'] = [dict(STRIKE_C)]; STORE['cache'] = {STRIKE: dict(STRIKE_C)}
LOGGED.clear(); WRITES.clear()
P._reconcile_scoring_state('Refresh')
check('(2) strike card untouched, no log, no write', not rec_lines() and not WRITES and STORE['pins'][0] == STRIKE_C)
base = score_pins()
check(f'(2) strike card votes -1.44 (got {base})', base == -1.44)

# ---- cycle 1: oil price-reaction card arrives mid-session (no reaction_only) ----
STORE['pins'].append(dict(OIL_C)); STORE['cache'][OIL] = dict(OIL_C)
c1 = score_pins()
check(f'(1) cycle 1: oil card votes (got {c1} != -1.44)', c1 != -1.44)
# served cached result built on cycle 1
served = {'all_items': [item_from(STRIKE_C), item_from(OIL_C)], 'pillar_score': c1}
served['news_items'] = served['all_items'][:10]

# ---- cycle 2: next refresh ----
LOGGED.clear(); WRITES.clear()
checked, changed = P._reconcile_scoring_state('Refresh')
oilpin = [p for p in STORE['pins'] if p['headline'] == OIL][0]
check('(1) pin marked reaction_only, kind follow_up', oilpin.get('reaction_only') and oilpin['kind'] == 'follow_up')
check('(1) cache marked reaction_only', STORE['cache'][OIL].get('reaction_only') and STORE['cache'][OIL]['kind'] == 'follow_up')
check('(2) strike pin + cache untouched', not STORE['pins'][0].get('reaction_only') and STORE['pins'][0]['kind'] == 'first_print'
      and STORE['cache'][STRIKE] == STRIKE_C)
check(f'(1) changed=2 checked=4 (got {changed}/{checked})', (checked, changed) == (4, 2))
check('(1) one pin + one cache write', sorted(WRITES) == ['cache', 'pins'])
c2 = score_pins()
check(f'(1) cycle 2: oil card no longer votes -> -1.44 (got {c2})', c2 == -1.44)
want = ["🧭 Refresh reaction reconcile (pin) — capped price-reaction piece marked reaction_only, kind -> follow_up | '" + OIL[:60] + "'",
        "🧭 Refresh reaction reconcile (cache) — capped price-reaction piece marked reaction_only, kind -> follow_up | '" + OIL[:60] + "'"]
check('(1) exact refresh log lines', rec_lines() == want)
for l in rec_lines(): print('  log:', l)

# served cached result recomputed on the same refresh
P._merge_fresh_classifications = lambda items: (items, 0)
P._filter_blocklisted_items = lambda items: (items, 0)
LOGGED.clear()
out = P._refresh_cached_data(served)
check(f'(1) served cache recomputed to -1.44 (got {out["pillar_score"]})', out['pillar_score'] == -1.44)
srv = [l for l in LOGGED if 'reconcile' in l.lower()]
for l in srv: print('  log:', l)
check('(1) served log lines', len(srv) == 2 and srv[0].startswith('🧭 Refresh reaction reconcile (served)')
      and srv[1].startswith('🧭 Cache refresh — reconciled 1 served item(s), score recomputed: -1.44'))

# ---- (3) repeat cycles are silent no-ops ----
LOGGED.clear(); WRITES.clear()
for _ in range(5):
    P._reconcile_scoring_state('Refresh')
    out = P._refresh_cached_data(out)
check(f'(3) 5 repeat cycles: zero reconcile lines (got {len(rec_lines())})', not rec_lines())
check(f'(3) 5 repeat cycles: zero writes (got {WRITES})', not WRITES)
check('(3) score stable -1.44', score_pins() == -1.44 and out['pillar_score'] == -1.44)

# ---- (4) moved tier-claim reconcile heals mid-session ----
MIS = "Houthis threaten Saudi shipping"
STORE['pins'].append(dict(cls(1, "Escalation but Tier 2 rather than Tier 1."), headline=MIS))
LOGGED.clear(); WRITES.clear()
P._reconcile_scoring_state('Refresh')
mis = [p for p in STORE['pins'] if p['headline'] == MIS][0]
check('(4) tier claim healed 1 -> 2 on refresh', mis['tier'] == 2)
check('(4) exact tier line', rec_lines() == ["🧭 Refresh tier reconcile (pin) — reasoning claims Tier 2, stored=1 | using 2 | '" + MIS + "'"])
LOGGED.clear(); P._reconcile_scoring_state('Refresh')
check('(4) tier heal idempotent', not rec_lines())

# ---- (5) startup wrapper keeps old text + summary ----
STORE['pins'] = [dict(STRIKE_C), dict(OIL_C)]; STORE['cache'] = {}
LOGGED.clear()
P.reconcile_stored_tier_claims()
check('(5) startup line text unchanged', rec_lines()[0] ==
      "🧭 Startup reaction reconcile (pin) — capped price-reaction piece marked reaction_only, kind -> follow_up | '" + OIL[:60] + "'")
check('(5) startup summary line', rec_lines()[-1] == 'RECONCILE TIER ran | checked=2 changed=1')
LOGGED.clear(); P.reconcile_stored_tier_claims()
check('(5) startup no-op still logs only the summary', rec_lines() == ['RECONCILE TIER ran | checked=2 changed=0'])

print('FAILURES:', fail)
sys.exit(1 if fail else 0)
