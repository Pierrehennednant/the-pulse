"""Replay 11/2 4:53 PM ET: a fresh oil-slide price-reaction article matched
event evt|437d6ee998f4 whose clock (2026-10-29T08:09:23Z +24h) had expired.
It must not fold (own fresh clock, not clock_expired next cycle); a live-clock
event must still fold exactly as before. No /data writes: pure method calls."""
import os, sys, tempfile, pathlib
from datetime import datetime, timezone, timedelta
os.chdir(tempfile.mkdtemp())
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pipelines.geopolitical as g

P = g.GeopoliticalPipeline.__new__(g.GeopoliticalPipeline)
P._event_logged = set()
LOGGED = []
_orig_log = g.pulse_logger.log
g.pulse_logger.log = lambda m, *a, **k: (LOGGED.append(m), _orig_log(m, *a, **k))
now = datetime.now(timezone.utc); today = now.date()
ORIG = "Oil prices fall as renewed U.S.-Iran talks hopes build, benchmark drops 3%"
NEW = "Oil prices fall as renewed U.S.-Iran talks hopes build, benchmark drops 7%"

def entry(h):
    t = {'headline': h, 'kind': 'follow_up', 'tier': 2, 'direction': 'bullish',
         'reaction_only': True, 'relevant': True, 'confidence': 0.75,
         'classified_at': now.isoformat()}
    ctx = P._event_ctx(h, '', h, '', '', 'https://ex.com/' + str(abs(hash(h))), {'published_at': now.isoformat()})
    return {'ctx': ctx, 'target': t}

def run(anchor):
    mem = {}
    P._event_decide(entry(ORIG), mem, now, today)
    assert mem, 'original did not create an event row'
    for row in mem.values():
        row['clock_start'] = anchor; row['first_seen'] = anchor
    e = entry(NEW)
    d, _ = P._event_decide(e, mem, now, today)
    return d, e['target']

fail = 0
def check(name, ok):
    global fail; fail += not ok; print('PASS' if ok else 'FAIL', name)

# Case: event clock expired on 10/30
d, t = run((now - timedelta(days=4)).isoformat())
logs = (d or {}).get('logs', [])
check('expired: no fold', not (d or {}).get('fold'))
check('expired: SKIPPED line logged', any('EVENT MEMORY FOLD SKIPPED | event clock expired' in l for l in LOGGED))
for l in LOGGED:
    if 'SKIPPED' in l: print('  log:', l[:170])
LOGGED.clear()
check('expired: own fresh clock, not clock_expired next cycle', not P._clock_expired(t))
check('expired: still reaction_only (no vote)', t.get('reaction_only') is True)
kind, _ = g._geo_campaign_apply({}, NEW, '', 'follow_up', 2, 'x', reaction_only=True)
check('expired: reaction_only forced follow_up (no pin)', kind == 'follow_up')

# Control: live clock (1h old) still folds as before
d, t = run((now - timedelta(hours=1)).isoformat())
logs = (d or {}).get('logs', [])
check('live: folds', bool((d or {}).get('fold')))
check('live: EVENT MEMORY FOLD (kind kept) line', any('EVENT MEMORY FOLD (kind kept)' in l for l in logs))
check('live: no SKIPPED line', not any('SKIPPED' in l for l in logs + LOGGED))
for l in logs: print('  log:', l[:160])
sys.exit(fail)
