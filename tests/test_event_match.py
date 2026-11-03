"""Replay 11/2-11/3 ET event-match mismatch: the Tier 1 strike card "U.S.
strikes Iranian targets" matched the Qatar diplomacy row evt|6df169093f6e
(sample 'Qatar pushes for U.S.-Iran deal...'), was refused, and got its own
row evt|e141f0b50870; Qeshm/Hormuz repeats then folded into e141.
(1) one EVENT MATCH DECISION line per article; refusals are EVENT CANDIDATE
    REFUSED lines (never read as the outcome).
(2) kind compatibility: a kinetic headline never folds into a talks/deal row
    (even a scored one) and vice versa; unknown/mixed kinds keep old behavior.
(3) second-vote window: the T1 fold guard sees the live in-memory scored set,
    so a repeat strike folds into the original card while it is unscored on disk.
No /data reads or writes: pure method calls, disk + event memory patched."""
import os, sys, tempfile, pathlib, threading
from datetime import datetime, timezone
os.chdir(tempfile.mkdtemp())
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pipelines.geopolitical as g

P = g.GeopoliticalPipeline.__new__(g.GeopoliticalPipeline)
P._event_logged = set()
P._event_memory_lock = threading.Lock()
LOGGED = []
_orig_log = g.pulse_logger.log
g.pulse_logger.log = lambda m, *a, **k: (LOGGED.append(m), _orig_log(m, *a, **k))
g.atomic_write_json = lambda *a, **k: None          # never write event memory
DISK = []                                           # patched disk snapshot
P._event_scored_disk_records = lambda: list(DISK)
now = datetime.now(timezone.utc); today = now.astimezone(g._EVENT_TZ).date()

QATAR = "Iran War Updates: Qatar pushes for U.S.-Iran deal under shadow of Trump's threats to resume strikes"
STRIKE = "U.S. strikes Iranian targets"
QESHM = "U.S. carries out strikes on Iran’s Qeshm Island"
HORMUZ = "U.S. launches strikes on Iranian targets in Strait of Hormuz, officials say"
# Live card text named the places (its row later held the Qeshm folds too).
DESC = {STRIKE: "The U.S. struck Iranian targets on Qeshm Island and near the Strait of Hormuz, officials said."}
QK, SK = 'evt|6df169093f6e', 'evt|e141f0b50870'

def cls(kind='first_print', tier=1):
    return {'relevant': True, 'confidence': 0.85, 'direction': 'bearish', 'tier': tier,
            'kind': kind, 'gate_pass': True, 'classified_at': now.isoformat(), 'summary': 's'}

def entry(h, t, store='fresh'):
    ctx = P._event_ctx(h, '', DESC.get(h, h), '', '', 'https://ex.com/' + str(abs(hash(h))), {'published_at': now.isoformat()})
    return {'ctx': ctx, 'target': t, 'store': store}

def ident(h, desc=None):
    return dict(g._event_identity({'headline': h, 'description': desc or h}, today), headline_norm=g._event_headline_norm(h))

def qatar_row():
    # Diplomacy row that also carries a strike-shaped identity (as live): the
    # identity match alone would accept a strike candidate.
    return {'first_seen': now.isoformat(), 'last_seen': now.isoformat(), 'sample_headline': QATAR,
            'haiku_kind_on_first_seen': 'follow_up', 'forced_follow_up_count': 0,
            'clock_start': now.isoformat(), 'clock_hours': 48,
            'identities': [ident(QATAR), ident("U.S. strikes Iranian targets again, Trump says", DESC[STRIKE])]}

def item_from(h, c):
    it = {'headline': h, 'sentiment_score': -0.8, 'market_relevant': True, 'pinned': True}
    P._apply_classification_fields(it, c)
    return it

fail = 0
def check(name, ok):
    global fail; fail += not ok; print('PASS' if ok else 'FAIL', name)

# ---- (2) kind table ----
check('(2) strike headline = kinetic', g._event_kind_class(STRIKE) == 'kinetic')
check('(2) Qatar deal headline = diplomacy (threat ignored)', g._event_kind_class(QATAR) == 'diplomacy')
check('(2) mixed headline = unknown', g._event_kind_class("Strikes continue as talks stall") is None)
check('(2) no-kind headline = unknown', g._event_kind_class("Oil output rises in Texas") is None)
check('(2) strike vs Qatar row incompatible', g._event_kind_incompatible(STRIKE, {'sample_headline': QATAR}) == 'kinetic-vs-diplomacy')
check('(2) talks vs strike row incompatible (symmetric)',
      g._event_kind_incompatible("U.S. and Iran resume talks in Doha", {'sample_headline': STRIKE}) == 'diplomacy-vs-kinetic')
check('(2) unknown row -> allowed', g._event_kind_incompatible(STRIKE, {'sample_headline': 'Iran war live'}) is None)
check('(2) sanctions vs strike row incompatible',
      g._event_kind_incompatible("U.S. imposes new sanctions on Iran", {'sample_headline': STRIKE}) == 'economic-vs-kinetic')

# ---- (2)+(1) strike can't fold into a SCORED talks row ----
mem = {QK: qatar_row()}
P._load_event_memory = lambda: (mem, 0)
DISK[:] = [(QATAR, cls('first_print', 1))]           # Qatar row scored, Tier 1
check('setup: identity match alone accepts Qatar row', g._event_identity_matches(ident(STRIKE, DESC[STRIKE]), mem[QK]) is not None)
check('setup: Qatar row is scored', P._event_scored_today(mem, today, disk_records=DISK).get(QK) == 1)
LOGGED.clear()
strike = cls()
P._apply_event_rules([entry(STRIKE, strike)])
check('(2) strike not folded into scored talks row', not strike.get('event_folded') and strike.get('event_key') != QK)
own = [k for k in mem if k != QK]
check('(2) strike kept its own row', len(own) == 1 and mem[own[0]]['sample_headline'] == STRIKE)
ref = [l for l in LOGGED if l.startswith('EVENT CANDIDATE REFUSED')]
check('(1) refusal labeled EVENT CANDIDATE REFUSED kind-incompatible',
      len(ref) == 1 and 'reason=kind-incompatible kinetic-vs-diplomacy' in ref[0] and f'key={QK}' in ref[0])
dec = [l for l in LOGGED if l.startswith('EVENT MATCH DECISION')]
check('(1) one DECISION line, outcome=kept_own_card refused=1',
      len(dec) == 1 and 'outcome=kept_own_card' in dec[0] and f'key={own[0]}' in dec[0] and 'refused=1' in dec[0])
check('(1) old T1 FIRST_PRINT KEEP line gone', not any('T1 FIRST_PRINT KEEP' in l for l in LOGGED))
for l in ref + dec: print('  log:', l[:200])

# ---- tonight's live shape + (3) second-vote window ----
# Strike card owns evt|e141f0b50870, is live in memory but NOT yet on disk.
mem = {QK: qatar_row()}
P._load_event_memory = lambda: (mem, 0)
P._event_logged = set(); DISK[:] = []
strike = cls()
P._apply_event_rules([entry(STRIKE, strike)])
k0 = next(k for k in mem if k != QK); mem[SK] = mem.pop(k0)
# Control: the disk-only snapshot (old behavior) leaves the window open.
qe_ctrl = cls()
d, _ = P._event_decide(entry(QESHM, qe_ctrl), {k: dict(v, identities=list(v['identities'])) for k, v in mem.items()},
                       now, today, lambda: P._event_scored_today(mem, today, disk_records=[]))
check('control: disk-only snapshot -> Qeshm would get its own vote', not (d or {}).get('fold'))
P._event_logged = set(); LOGGED.clear()
qeshm, hormuz = cls(), cls()
P._apply_event_rules([entry(STRIKE, strike, 'cache'), entry(QESHM, qeshm), entry(HORMUZ, hormuz)])
check('(3) Qeshm folds into evt|e141f0b50870', qeshm.get('event_folded') and qeshm.get('event_key') == SK)
check('(3) Hormuz folds into evt|e141f0b50870', hormuz.get('event_folded') and hormuz.get('event_key') == SK)
check('no new rows (no second card)', set(mem) == {QK, SK})
check('strike card itself not folded', not strike.get('event_folded'))
dec = [l for l in LOGGED if l.startswith('EVENT MATCH DECISION')]
check('(1) one DECISION per repeat, outcome=folded key=e141',
      len(dec) == 2 and all('outcome=folded' in l and f'key={SK}' in l for l in dec))
check('(1) no T1 FIRST_PRINT KEEP lines', not any('T1 FIRST_PRINT KEEP' in l for l in LOGGED))
for l in LOGGED:
    if l.startswith(('EVENT MATCH', 'EVENT CANDIDATE')): print('  log:', l[:200])
alone = P.calculate_score([item_from(STRIKE, strike)], [])
allc = P.calculate_score([item_from(STRIKE, strike), item_from(QESHM, qeshm), item_from(HORMUZ, hormuz)], [])
check(f'strike card votes -1.44 (got {alone})', alone == -1.44)
check(f'no double count: with repeats still -1.44 (got {allc})', allc == alone)

# ---- (3) a fold decided this pass is not a vote ----
sc = P._event_scored_today(mem, today, live_records=[(QESHM, dict(cls(), event_folded=True))], disk_records=[])
check('(3) folded in-memory record does not score a row', SK not in sc)

print('FAILURES:', fail)
sys.exit(1 if fail else 0)
