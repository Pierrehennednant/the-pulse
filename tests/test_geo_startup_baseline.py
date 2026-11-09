"""Startup served-list baseline: exactly one
✓ Geopolitical updated | … | source=startup
per process boot via log_startup_baseline / _commit_served_state.
(1) empty / missing cache → voting=0 pending_haiku=0 flags=0 articles=0;
(2) populated cache (Tier 1 strike, -1.44) → correct voting count + score;
(3) exactly one baseline line per boot; second call is a no-op;
(4) quiet refresh afterward does not re-log an identical fingerprint;
(5) score unchanged; cache not rewritten on a no-op baseline.
No /data writes: patched cache load/save."""
import os, sys, json, tempfile, pathlib
from datetime import datetime, timezone
os.chdir(tempfile.mkdtemp())
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pipelines.geopolitical as g

P = g.GeopoliticalPipeline.__new__(g.GeopoliticalPipeline)
P.timezone = g.pytz.timezone(g.TIMEZONE)
P.cache_key = 'geopolitical'
P._startup_baseline_logged = False

LOGGED = []
_orig_log = g.pulse_logger.log
g.pulse_logger.log = lambda m, *a, **k: (LOGGED.append(m), _orig_log(m, *a, **k))

PILLAR = {'payload': None}
SAVES = []
def _csave(key, data):
    assert key == 'geopolitical'
    SAVES.append(json.loads(json.dumps(data)))
    PILLAR['payload'] = json.loads(json.dumps(data))
def _cload(key):
    if key == 'geopolitical' and PILLAR['payload'] is not None:
        return {
            'data': json.loads(json.dumps(PILLAR['payload'])),
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
    return None
g.cache.save = _csave
g.cache.load = _cload
# Quiet the heal side-paths
P._merge_fresh_classifications = lambda items: (items, 0)
P._filter_blocklisted_items = lambda items: (items, 0)
P._reconcile_served_items = lambda items: 0
P.is_article_too_old = lambda pub: False
P._is_ec_survey_recap = lambda item: False
P._is_aviation_incident = lambda item: False

fail = 0
def check(name, ok):
    global fail
    fail += not ok
    print('PASS' if ok else 'FAIL', name)

def geo_lines():
    return [l for l in LOGGED if l.startswith('✓ Geopolitical updated')]

def startup_lines():
    return [l for l in geo_lines() if 'source=startup' in l]

def strike_item():
    return {
        'headline': 'U.S. strikes Iranian targets',
        'source': 'reuters.com',
        'date': '2026-11-09',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'link': 'https://example.com/strike',
        'sentiment_score': -0.8,
        'description': 'Kinetic strikes.',
        'keyword_fallback_only': False,
        'market_relevant': True,
        'gemini_direction': 'bearish',
        'haiku_tier': 1,
        'haiku_confidence': 0.85,
        'haiku_gate_pass': True,
        'kind': 'first_print',
        'haiku_tier_reasoning': 'Active U.S. kinetic strikes on Iranian territory; Tier 1.',
        'pinned': True,
    }

# ---- (1) empty / missing cache ----
PILLAR['payload'] = None
P._startup_baseline_logged = False
LOGGED.clear(); SAVES.clear()
P.log_startup_baseline()
lines = startup_lines()
check('(1) one startup line on empty cache', len(lines) == 1)
check('(1) 0/0/0 counts',
      'voting=0' in lines[0] and 'pending_haiku=0' in lines[0]
      and 'flags=0' in lines[0] and 'articles=0' in lines[0])
check('(1) Score: 0.0 | source=startup',
      'Score: 0.0' in lines[0] and lines[0].endswith('| source=startup'))
check('(1) no cache rewrite on empty baseline', SAVES == [])
print('  empty:', lines[0])

# ---- (3) second call same boot is no-op ----
LOGGED.clear(); SAVES.clear()
P.log_startup_baseline()
check('(3) second call: no extra Geopolitical updated', geo_lines() == [])
check('(3) second call: no save', SAVES == [])

# ---- (2) populated cache: Tier 1 strike ----
strike = strike_item()
# Pre-seed pillar cache as a prior live serve would have left it
seed = {
    'pillar': 'geopolitical',
    'timestamp': '2026-11-09T08:00:00-05:00',
    'status': 'live',
}
score_before = P.calculate_score([dict(strike)], [])
P._commit_served_state(seed, [dict(strike)], 'live', persist=True, always_log=True)
check(f'(2) seed score -1.44 (got {seed["pillar_score"]})', seed['pillar_score'] == -1.44 == score_before)
# Reset boot guard; keep payload
P._startup_baseline_logged = False
LOGGED.clear(); SAVES.clear()
P.log_startup_baseline()
lines = startup_lines()
check('(2) one startup line on populated cache', len(lines) == 1)
check('(2) voting=1 pending_haiku=0 articles=1',
      'voting=1' in lines[0] and 'pending_haiku=0' in lines[0] and 'articles=1' in lines[0])
check('(2) Score: -1.44 | source=startup',
      'Score: -1.44' in lines[0] and 'source=startup' in lines[0])
check('(2) score unchanged vs seed', PILLAR['payload']['pillar_score'] == -1.44)
check('(2) no-op heal → no cache rewrite', SAVES == [])
print('  populated:', lines[0])

# ---- (4) quiet refresh does not re-log identical fingerprint ----
LOGGED.clear(); SAVES.clear()
out = P._refresh_cached_data(PILLAR['payload'], persist=True)
check('(4) quiet refresh: no Geopolitical updated', geo_lines() == [])
check('(4) quiet refresh: no save', SAVES == [])
check('(4) score still -1.44', out['pillar_score'] == -1.44)

# ---- (5) heal-at-startup folds into source=startup (no duplicate reconciled line) ----
P._startup_baseline_logged = False
PILLAR['payload'] = json.loads(json.dumps(seed))  # restore voting strike seed
# Make reconcile flip reaction_only so the served list changes at baseline
def _recon(items):
    for it in items:
        it['reaction_only'] = True
        it['kind'] = 'follow_up'
    return 1
P._reconcile_served_items = _recon
LOGGED.clear(); SAVES.clear()
P.log_startup_baseline()
lines = startup_lines()
recon_geo = [l for l in geo_lines() if 'source=reconciled' in l]
check('(5) heal folded into one startup line', len(lines) == 1 and 'source=startup' in lines[0])
check('(5) no separate source=reconciled Geopolitical updated', recon_geo == [])
check('(5) after heal voting=0 Score: 0.0',
      'voting=0' in lines[0] and 'Score: 0.0' in lines[0])
check('(5) changed heal DID persist', len(SAVES) == 1)
print('  healed-startup:', lines[0])

print('FAILURES:', fail)
sys.exit(1 if fail else 0)
