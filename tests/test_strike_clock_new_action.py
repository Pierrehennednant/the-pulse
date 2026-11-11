"""Strike-clock new-action gate — replay of the 11/11 Hormuz tanker card.

The 11/6 Iran card's 48h timer expired 11/8. On 11/11 a follow_up tanker
story matched no live same-event row, and the strike clock upgraded it to
first_print (pin-eligible) purely off board state: no same-wave strike
live. The outcome was right (the story reported NEW strikes), but the
upgrade keyed on timer expiry + board occupancy, which say nothing about
whether anything new happened. The gate now also requires Haiku's own
new-action judgment: the reasoning claim ("New action: ..." / "No new
action") wins over the structured new_action field (the tier-claim
precedence); with neither, no upgrade.

(1) follow_up WITH a new strike after the prior card expired -> upgrades
    (today's case, preserved).
(2) follow_up with NO new action (reaction/assessment) after expiry ->
    stays follow_up, no pin; (2b) the same fixture WITH a new action
    upgrades, proving (2)'s block was the gate alone.
(3) follow_up with no new action while the same-wave card is still live
    -> folds into it exactly as before (strike clock never reached).
(4) a genuine first_print -> unchanged.
(5) precedence at the gate: reasoning claim beats the structured field
    in both directions.
No /data reads or writes: pure method calls, logger captured."""
import os, sys, tempfile, pathlib
from datetime import datetime, timezone
os.chdir(tempfile.mkdtemp())
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import pipelines.geopolitical as g

P = g.GeopoliticalPipeline.__new__(g.GeopoliticalPipeline)
P._event_logged = set()
LOGGED = []
_orig_log = g.pulse_logger.log
g.pulse_logger.log = lambda m, *a, **k: (LOGGED.append(m), _orig_log(m, *a, **k))

NOV6 = datetime(2026, 11, 6, 15, 0, tzinfo=timezone.utc)
NOV11 = datetime(2026, 11, 11, 14, 0, tzinfo=timezone.utc)
TODAY6 = NOV6.astimezone(g._EVENT_TZ).date()
TODAY11 = NOV11.astimezone(g._EVENT_TZ).date()

OLD = "U.S. strikes Iranian targets"
OLD_DESC = "The U.S. struck Iranian targets on Qeshm Island and near the Strait of Hormuz, officials said."
NEW_STRIKE = "Iranian missiles strike oil tanker in Strait of Hormuz"
NEW_STRIKE_DESC = ("Iranian missiles struck an oil tanker transiting the Strait of Hormuz "
                   "on Wednesday, U.S. officials said, setting the vessel ablaze.")
REACTION = "Oil climbs as traders assess damage from strike on tanker in Strait of Hormuz"
REACTION_DESC = ("Oil prices climbed Wednesday as traders assessed the damage from a strike "
                 "on a tanker in the Strait of Hormuz and officials condemned the attack.")
QESHM = "U.S. carries out strikes on Iran's Qeshm Island"


def cls(kind='follow_up', tier=2, new_action=None, reasoning='', at=NOV11):
    t = {'relevant': True, 'confidence': 0.9, 'direction': 'bearish', 'tier': tier,
         'kind': kind, 'gate_pass': True, 'classified_at': at.isoformat(), 'summary': 's'}
    if new_action is not None:
        t['new_action'] = new_action
    return t


def entry(h, desc, t, reasoning='', pub=NOV11):
    ctx = P._event_ctx(h, '', desc, '', reasoning, 'https://ex.com/' + str(abs(hash(h))),
                       {'published_at': pub.isoformat()})
    return {'ctx': ctx, 'target': t, 'store': 'fresh'}


def seed_old_mem():
    """The 11/6 first-print Iran card; its 48h clock expired 11/8."""
    mem = {}
    P._event_logged = set()
    d, _ = P._event_decide(entry(OLD, OLD_DESC, cls('first_print', 1, True, at=NOV6),
                                 pub=NOV6), mem, NOV6, TODAY6)
    assert mem, 'old card did not create an event row'
    row = next(iter(mem.values()))
    assert P._clock_expired({'clock_anchor': row['clock_start'],
                             'clock_hours': row['clock_hours']}), 'old card clock must be expired by 11/11'
    return mem


def fresh(mem):
    return {k: dict(v, identities=list(v['identities'])) for k, v in mem.items()}


fail = 0
def check(name, ok):
    global fail; fail += not ok; print('PASS' if ok else 'FAIL', name)


# ---- (0) claim parser + resolver (tier-claim test style) ----
f = g.GeopoliticalPipeline._reasoning_new_action_claim
for text, want in [
    ("New action: Iranian missiles struck a tanker in Hormuz.", True),
    ("No new action — damage assessment and official reaction only.", False),
    ("This is not a new action, only a recap of the earlier strike.", False),
    ("Tier 2 catalyst; escalation is bearish via oil.", None),
    ("", None),
    (None, None),
    ("Officials reported no new action overnight; a new action — a fresh strike — followed at dawn.", True),
]:
    check(f'(0) claim {want} <- {str(text)[:45]!r}', f(text) == want)
r = g.GeopoliticalPipeline._resolve_new_action
check('(0) resolver: claim beats field (False field, claim True)', r("New action: a tanker was seized.", False) is True)
check('(0) resolver: claim beats field (True field, claim False)', r("No new action.", True) is False)
check('(0) resolver: field alone', r("some reasoning", True) is True and r("some reasoning", False) is False)
check('(0) resolver: silence never upgrades', r("", None) is False)

base_mem = seed_old_mem()
old_key = next(iter(base_mem))

# ---- (1) follow_up WITH a new strike after expiry -> upgrades (today's case) ----
mem = fresh(base_mem); P._event_logged = set(); LOGGED.clear()
t = cls('follow_up', 2, True)
d, _ = P._event_decide(entry(NEW_STRIKE, NEW_STRIKE_DESC, t,
                             "New action: Iranian missiles struck an oil tanker in the Strait of Hormuz. "
                             "Follow-up framing to the standing war, Tier 2."), mem, NOV11, TODAY11)
check('(1) upgraded to first_print', (d or {}).get('kind') == 'first_print')
check('(1) strike_clock rule fired', 'strike_clock' in ((d or {}).get('rules') or []))
check('(1) no fold into the expired card', not (d or {}).get('fold'))
check('(1) STRIKE CLOCK log line', any('STRIKE CLOCK | haiku_kind=follow_up -> first_print' in l
                                      for l in (d or {}).get('logs', [])))
new_rows = [k for k in mem if k != old_key]
check('(1) own new card at the 48h clock', len(new_rows) == 1 and mem[new_rows[0]]['clock_hours'] == 48)

# ---- (2) follow_up with NO new action after expiry -> stays follow_up ----
mem = fresh(base_mem); P._event_logged = set(); LOGGED.clear()
t = cls('follow_up', 3, False)
d, _ = P._event_decide(entry(REACTION, REACTION_DESC, t,
                             "No new action — damage assessment, market reaction and official "
                             "condemnation of the earlier strike only. Tier 3."), mem, NOV11, TODAY11)
check('(2) not upgraded', (d or {}).get('kind') != 'first_print')
check('(2) no strike_clock rule', 'strike_clock' not in ((d or {}).get('rules') or []))
check('(2) BLOCKED | no new action logged', any('STRIKE CLOCK BLOCKED | no new action' in l for l in LOGGED))
new_rows = [k for k in mem if k != old_key]
check('(2) own card stays on the 24h follow_up clock',
      len(new_rows) == 1 and mem[new_rows[0]]['clock_hours'] == 24)
for l in LOGGED:
    if 'BLOCKED' in l: print('  log:', l[:200])

# ---- (2b) control: the SAME reaction fixture WITH a new action upgrades ----
mem = fresh(base_mem); P._event_logged = set(); LOGGED.clear()
t = cls('follow_up', 3, True)
d, _ = P._event_decide(entry(REACTION, REACTION_DESC, t,
                             "New action: a second strike hit another tanker in the strait. "
                             "Tier 3 piece, but the act is new."), mem, NOV11, TODAY11)
check('(2b) same fixture + new action -> upgraded', (d or {}).get('kind') == 'first_print'
      and 'strike_clock' in ((d or {}).get('rules') or []))

# ---- (3) no new action, same-wave card still LIVE -> folds as before ----
mem = {}; P._event_logged = set(); LOGGED.clear()
P._event_decide(entry(OLD, OLD_DESC, cls('first_print', 1, True), pub=NOV11), mem, NOV11, TODAY11)
live_key = next(iter(mem))
t = cls('follow_up', 2, False)
d, _ = P._event_decide(entry(QESHM, QESHM, t, "No new action — same wave, repeat reporting."),
                       mem, NOV11, TODAY11)
check('(3) folds into the live card', bool((d or {}).get('fold'))
      and (d or {}).get('fold', {}).get('event_key') == live_key)
check('(3) kind kept follow_up on the fold', (d or {}).get('kind') == 'follow_up')
check('(3) no strike_clock rule on a fold', 'strike_clock' not in ((d or {}).get('rules') or []))
check('(3) no new row created', set(mem) == {live_key})

# ---- (4) genuine first_print -> unchanged ----
mem = {}; P._event_logged = set(); LOGGED.clear()
t = cls('first_print', 1, True)
d, _ = P._event_decide(entry(OLD, OLD_DESC, t, "New action: U.S. strikes on Iranian targets."),
                       mem, NOV11, TODAY11)
check('(4) no decision (Haiku kind/tier untouched)', d is None)
row = next(iter(mem.values()))
check('(4) own card at the 48h clock', row['clock_hours'] == 48 and row['haiku_kind_on_first_seen'] == 'first_print')
check('(4) no STRIKE CLOCK lines', not any('STRIKE CLOCK' in l for l in LOGGED))

# ---- (5a) precedence: reasoning claim True beats structured False ----
mem = fresh(base_mem); P._event_logged = set(); LOGGED.clear()
t = cls('follow_up', 2, False)
d, _ = P._event_decide(entry(NEW_STRIKE, NEW_STRIKE_DESC, t,
                             "Structured field aside — New action: Iranian missiles struck a "
                             "second tanker in the strait."), mem, NOV11, TODAY11)
check('(5a) affirmative claim upgrades despite new_action=false', (d or {}).get('kind') == 'first_print'
      and 'strike_clock' in ((d or {}).get('rules') or []))

# ---- (5b) precedence: reasoning denial beats structured True ----
mem = fresh(base_mem); P._event_logged = set(); LOGGED.clear()
t = cls('follow_up', 2, True)
d, _ = P._event_decide(entry(NEW_STRIKE, NEW_STRIKE_DESC, t,
                             "No new action: this piece only assesses the earlier strike and "
                             "collects official reaction."), mem, NOV11, TODAY11)
check('(5b) denial blocks despite new_action=true', (d or {}).get('kind') != 'first_print')
check('(5b) BLOCKED | no new action logged', any('STRIKE CLOCK BLOCKED | no new action' in l for l in LOGGED))

print('FAILURES:', fail)
sys.exit(1 if fail else 0)
