"""Nov 16 chip-loophole double count (9:35 AM ET): the NYT 'Trump
Administration Moves to Close A.I. Chip Loophole Ahead of China Trip' was
SAME-story as the pinned CNBC 'Nvidia fallout: Commerce cracks down on
third-country loophole...', but has_outcome_diverged() returned True and the
item became a second Tier 2 pin with its own vote; Rule A never matched them.
(1) has_outcome_diverged: tolerant parse, one line per call, every
    non-answer (empty / unparseable / error / no client) = not diverged.
(2) a divergence keep never adds a second vote on the same event: no state
    change -> folded into the anchor's event (no vote); a real state change
    (talks -> strike) still keeps its own card; the T1 live fold is unchanged.
(3) Rule A: the chip pair folds (us/Trump not a new actor, Title Case
    headline, export-control kind); near-miss rows count in refused=.
Haiku is STUBBED everywhere (no network). No /data reads or writes."""
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
g.atomic_write_json = lambda *a, **k: None
P._event_scored_disk_records = lambda: []
now = datetime.now(timezone.utc); today = now.astimezone(g._EVENT_TZ).date()

class _Blk:
    type = 'text'
    def __init__(self, t): self.text = t
class _Resp:
    def __init__(self, t): self.content = [_Blk(t)]
class StubHaiku:
    """Stub Anthropic client: returns `reply` (or raises it if an Exception)."""
    def __init__(self, reply): self.reply, self.calls = reply, 0; self.messages = self
    def create(self, **kw):
        self.calls += 1
        if isinstance(self.reply, Exception):
            raise self.reply
        return _Resp(self.reply)

fail = 0
def check(name, ok):
    global fail; fail += not ok; print('PASS' if ok else 'FAIL', name)

# Logged headlines (CNBC is the 90-char log text) and the logged Haiku
# reasoning as summary stand-ins (summaries themselves are not logged).
CNBC = "Nvidia fallout: Commerce cracks down on third-country loophole that allowed China to use c"
CNBC_S = ("New official export-control restriction from a primary actor (BIS) affecting AI chip flows; "
          "material to the tech complex but not a war or systemic shock.")
NYT = "Trump Administration Moves to Close A.I. Chip Loophole Ahead of China Trip"
NYT_S = ("New official export-control restriction affecting AI chip sales to China and related data-center "
         "flows; a material negative for the semiconductor complex, with the China summit creating possible reversal risk.")
IRAN = "Iran launches new drone strikes toward U.S. base"
RU_A = "Russia and Ukraine begin U.S.-backed peace talks in Abu Dhabi"
RU_N = "Russia and Ukraine hold nearly 4 hours of U.S.-mediated talks"
TALKS = "U.S. and Iran hold talks in Oman"
STRIKE = "U.S. launches strikes on Iranian targets"

def cls(summary, tier=2, kind='first_print', direction='bearish', conf=0.75):
    return {'relevant': True, 'confidence': conf, 'direction': direction, 'tier': tier, 'kind': kind,
            'gate_pass': True, 'classified_at': now.isoformat(), 'summary': summary, 'reason': summary}

def entry(h, t, summary=''):
    ctx = P._event_ctx(h, summary, '', '', '', 'https://ex.com/' + str(abs(hash(h))), {'published_at': now.isoformat()})
    return {'ctx': ctx, 'target': t, 'store': 'fresh'}

def lines(prefix):
    return [l for l in LOGGED if l.startswith(prefix)]

# ---- (1) parser + one line per call + fallbacks ----
for reply, want in (('UNCHANGED', 'unchanged'), ('Unchanged.', 'unchanged'), ('**DIVERGED**', 'diverged'),
                    ('diverged\n', 'diverged'), ('', None), ('maybe', None), ('DIVERGED or UNCHANGED', None)):
    check(f'(1) parse {reply!r} -> {want}', g._parse_divergence_reply(reply) == want)
cases = [('UNCHANGED', 'a', 'b', False, 'result=unchanged'),
         ('Unchanged.', 'a', 'b', False, 'result=unchanged'),
         ('DIVERGED', 'a', 'b', True, 'result=diverged'),
         ('maybe', 'a', 'b', False, 'result=fallback-unparseable'),
         ('', 'a', 'b', False, 'result=fallback-unparseable'),
         (TimeoutError('read timed out'), 'a', 'b', False, 'result=fallback-error'),
         ('DIVERGED', '', 'b', False, 'result=fallback-empty'),
         ('DIVERGED', 'a', '', False, 'result=fallback-empty')]
for reply, ex, nw, want, tag in cases:
    P.anthropic_client = StubHaiku(reply); LOGGED.clear()
    got = P.has_outcome_diverged(ex, '', nw, '', headline=NYT, anchor=CNBC)
    ln = lines('OUTCOME DIVERGENCE CHECK')
    check(f'(1) reply={reply!r} ex={ex!r} new={nw!r} -> {want} ({tag})',
          got is want and len(ln) == 1 and tag in ln[0] and f"treated_as={'diverged' if want else 'same'}" in ln[0])
P.anthropic_client = None; LOGGED.clear()
check('(1) no client -> not diverged, logged fallback-error',
      P.has_outcome_diverged('a', '', 'b', '') is False and 'result=fallback-error' in lines('OUTCOME DIVERGENCE CHECK')[0])
print('  log:', [l for l in LOGGED][-1][:200])

# ---- (3) Rule A: the chip pair folds; refused counts near misses ----
mem = {}
P._load_event_memory = lambda: (mem, 0)
P._event_logged = set(); LOGGED.clear()
cnbc = cls(CNBC_S)
P._apply_event_rules([entry(CNBC, cnbc, CNBC_S)])
ck = next(iter(mem))
iran = cls('Iranian drone strikes on a U.S. base.', tier=1, conf=0.85)
P._apply_event_rules([entry(IRAN, iran, 'Iranian drone strikes on a U.S. base.')])
LOGGED.clear(); P._event_logged = set()
nyt = cls(NYT_S)
P._apply_event_rules([entry(NYT, nyt, NYT_S)])
dec = lines('EVENT MATCH DECISION')
check('(3) chip pair: NYT folds into the CNBC event', nyt.get('event_folded') and nyt.get('event_key') == ck)
check('(3) DECISION line outcome=folded key=CNBC', len(dec) == 1 and 'outcome=folded' in dec[0] and f'key={ck}' in dec[0])
check('(3) no second event row', len(mem) == 2)
for l in LOGGED: print('  log:', l[:220])
ia = g._event_identity({'headline': NYT, 'summary': NYT_S}, today)
check('(3) Title Case headline words are not ! objects', not any(t.startswith('!') for t in ia['objects']))
check('(3) export_control kind', 'export_control' in ia['kinds'])
check('(3) us/Trump is not a new actor', g._event_same_event_gates(ia, g._event_identity({'headline': CNBC, 'summary': CNBC_S}, today)) is None)
check('(3) sentence-case headline still yields proper-noun objects',
      '!qeshm' in g._event_identity({'headline': "U.S. carries out strikes on Iran's Qeshm Island"}, today)['objects'])
def I(h, summ=''):
    return g._event_identity({'headline': h, 'summary': summ}, today)
check('(3) kinetic: U.S. stays a new actor', g._event_same_event_gates(
    I('U.S. and Israel strike Iranian missile sites'), I('Israel strikes Iranian missile sites')) == 'new_actor')
check('(3) U.S. alone (no other shared headline actor) stays a new actor', g._event_same_event_gates(
    I('Trump continues to back Ramaswamy attack ad in Florida'), I('Man dies after alligator attack in Florida')) is not None)
check('(3) shared sanction kind needs a shared non-U.S. actor', g._event_same_event_gates(
    I('Bank of America to pay $1.21 billion over sanctions violations'), I('Trump waives new Russia sanctions')) is not None)
# near miss now counted: a chip story vs ONLY the Iran row
mem2 = {k: v for k, v in mem.items() if k != ck and v['sample_headline'] == IRAN}
P._load_event_memory = lambda: (mem2, 0)
P._event_logged = set(); LOGGED.clear()
P._apply_event_rules([entry(NYT + ' (copy)', cls(NYT_S), NYT_S)])
dec = lines('EVENT MATCH DECISION'); sk = lines('EVENT CANDIDATE SKIPPED')
check('(3) silent gate skip counted: refused=1', len(dec) == 1 and 'outcome=kept_own_card' in dec[0] and 'refused=1' in dec[0])
check('(3) one SKIPPED line with reason', len(sk) == 1 and 'reason=gate:' in sk[0] and 'score=' in sk[0])
for l in sk + dec: print('  log:', l[:220])
P._load_event_memory = lambda: (mem, 0)

# ---- (2) divergence resolve at the duplicate check ----
def resolve(headline, anchor, existing, new, r, reply):
    P.anthropic_client = StubHaiku(reply); LOGGED.clear()
    out = P._outcome_divergence_resolve(headline, anchor, existing, new, r)
    return out, P.anthropic_client.calls

# (a) chip pair where Rule A did NOT fold it (pre-fix memory): DIVERGED -> fold into CNBC event, no vote
r = cls(NYT_S); new = dict(r)
out, calls = resolve(NYT, CNBC, cnbc, new, r, 'DIVERGED')
check('(2a) chip pair stays on the board (not dropped)', out is None)
check('(2a) folded into the CNBC event, no vote', r.get('event_folded') and r.get('event_key') == ck and new.get('event_key') == ck)
keep = lines('GEO KEEP')
check('(2a) GEO KEEP line says no state change + folded, no vote',
      len(keep) == 1 and 'reason=outcome-divergence' in keep[0] and 'no state change' in keep[0] and 'no vote' in keep[0])
for l in LOGGED: print('  log:', l[:220])
it_c = {'headline': CNBC, 'sentiment_score': -0.8, 'market_relevant': True, 'pinned': True}
P._apply_classification_fields(it_c, cnbc)
it_n = {'headline': NYT, 'sentiment_score': -0.8, 'market_relevant': True, 'pinned': True}
P._apply_classification_fields(it_n, new)
def voting(items):
    sc = P.calculate_score(items, [])
    return sc, int((P._last_score_stats or {}).get('voting') or 0)
it_old = {'headline': NYT, 'sentiment_score': -0.8, 'market_relevant': True, 'pinned': True}
P._apply_classification_fields(it_old, cls(NYT_S))
alone, both, old = voting([it_c]), voting([it_c, it_n]), voting([it_c, it_old])
check(f'control: pre-fix (NYT unfolded) = 2 votes (got {old})', old[1] == 2)
check(f'(2a) no second vote: CNBC alone {alone} == with folded NYT {both}', alone == both and both[1] == 1)
# (b) Nov 10 Russia/Ukraine UNCHANGED -> duplicate; and "Unchanged." no longer fails closed
for reply in ('UNCHANGED', 'Unchanged.'):
    r = cls('talks', tier=3, direction='neutral', conf=0.6); new = dict(r)
    out, calls = resolve(RU_N, RU_A, cls('talks', tier=3, direction='neutral', conf=0.6), new, r, reply)
    check(f'(2b) Russia/Ukraine {reply!r} stays a duplicate of the anchor', out == RU_A and not r.get('event_folded'))
# (c) real state change: talks -> strike keeps its own card and vote
talks_row = cls('talks')
mem['evt|talks000000'] = {'first_seen': now.isoformat(), 'last_seen': now.isoformat(), 'sample_headline': TALKS,
                          'haiku_kind_on_first_seen': 'first_print', 'clock_start': now.isoformat(), 'clock_hours': 48,
                          'identities': [{'headline_norm': g._event_headline_norm(TALKS)}]}
r = cls('strike'); new = dict(r)
out, calls = resolve(STRIKE, TALKS, talks_row, new, r, 'DIVERGED')
keep = lines('GEO KEEP')
check('(2c) talks -> strike KEEPS own card (not folded)', out is None and not r.get('event_folded'))
check('(2c) GEO KEEP names the state change', len(keep) == 1 and 'state_change=class diplomacy->kinetic' in keep[0] and 'kept as own card' in keep[0])
print('  log:', keep[0][:220] if keep else None)
# (d) T1 live fold unchanged: never calls Haiku, handled as duplicate
r = dict(cls('x', tier=1), event_folded=True, event_key='evt|42e12334ee8c', t1_live_fold=True); new = dict(r)
out, calls = resolve(IRAN, 'Iran targets Bahrain', cls('x', tier=1), new, r, 'DIVERGED')
check('(2d) T1 live fold: duplicate path, no Haiku call', out == 'Iran targets Bahrain' and calls == 0
      and lines('GEO T1 LIVE FOLD | outcome-divergence keep skipped'))
# (e) fallback never keeps: Haiku error -> duplicate
r = cls(NYT_S); new = dict(r)
out, calls = resolve(NYT, CNBC, cnbc, new, r, RuntimeError('overloaded'))
check('(2e) Haiku error -> SAME stands (duplicate), no KEEP', out == CNBC and not lines('GEO KEEP'))
# (f) no anchor event row -> duplicate, never a second vote
r = cls(NYT_S); new = dict(r)
MISSING = "Commerce tightens chip export rules"
out, calls = resolve(NYT, MISSING, cls('x'), new, r, 'DIVERGED')
check('(2f) anchor event missing -> handled as duplicate', out == MISSING and lines('GEO KEEP skipped'))
# (g) a vocabulary gap is not a state change: unknown-class anchor vs economic item
check('(2g) unknown -> economic is not a state change', g._event_state_change('Chip stocks slide on China report', NYT) is None)
check('(2g) unknown -> kinetic IS a state change', g._event_state_change('Trump says Iran war at status quo', STRIKE) == 'class unknown->kinetic')
check('(2g) talks -> sanctions IS a state change', g._event_state_change(TALKS, 'U.S. imposes new sanctions on Iran') == 'class diplomacy->economic')
check('(2g) new escalation class IS a state change', g._event_state_change(STRIKE, 'U.S. strikes Iranian nuclear sites') == 'escalation nuclear')

print(f'FAILURES: {fail}')
sys.exit(1 if fail else 0)
