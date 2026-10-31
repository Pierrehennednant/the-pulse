import re, importlib.util, pathlib, sys
# Load only the regexes + method source, no pipeline imports / no /data access.
src = pathlib.Path(__file__).resolve().parents[1].joinpath('pipelines/geopolitical.py').read_text()
ns = {'re': re}
claim = re.search(r"    _TIER_CLAIM_RE = .*?\n    _TIER_NEG_RE = .*?\n", src, re.S).group(0)
meth = re.search(r"    def _reasoning_tier_claim\(reasoning\):.*?return claimed\n", src, re.S).group(0)
code = "class GeopoliticalPipeline:\n" + claim + "    @staticmethod\n" + meth
exec(code, ns)
f = ns['GeopoliticalPipeline']._reasoning_tier_claim
TANKER = ('Ongoing Hormuz shipping campaign framed as continuing escalation ("in recent weeks," "continued"), '
          'so it is a follow-up to the standing war. Primary subject is oil price and flow reaction, so tier is '
          'capped at 2. Escalation is bearish via oil and inflation.')
cases = [
  (TANKER, 2),
  ("Price-Reaction Cap applies; capped at Tier 2.", 2),
  ("Tier 1 catalyst", 1),
  ("Not Tier 1 because no active kinetic conflict", None),
  ("Tier 2 rather than Tier 1", 2),
  ("downgraded from Tier 1 to Tier 2", 2),
  ("Active kinetic action on a critical energy chokepoint plus a second front.", None),
  ("not capped at 2 since a new attack is reported; Tier 1", 1),
]
fail = 0
for text, want in cases:
    got = f(text); ok = got == want; fail += not ok
    print(('PASS' if ok else 'FAIL'), want, got, '|', text[:70])
# Replay: the 5457 consistency block on a fixture r (as the recheck left it)
r = {'id': 1, 'tier': 1, 'reasoning': TANKER, 'event_folded': True}
tier = r['tier']; c = f(r['reasoning'])
if c is not None and c != tier: r['tier'] = c
print('replay stored tier:', r['tier']); fail += r['tier'] != 2
sys.exit(fail)
