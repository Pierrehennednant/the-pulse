"""Reason-tense tests for the Day Probability Calendar.

Covers Pierre's 2026-11-17 catch: the ratings move at checkpoints but
the reason strings did not — after Tuesday's Retail Sales print landed,
Wednesday's card still said "the print is still being counted" and
Thursday/Friday still said "still counting ... from Tuesday", as if
the print were pending. Release-linked reason text now keys off
whether the print has landed (past date, or the day itself at/after
the 8:30 AM ET print time). Scores and tiers must be identical in
every state — this is a wording fix only.

Run: python3 tests/test_calendar_reason_tense.py
"""
import importlib.util
import os
import sys
import types
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
MODULE_PATH = os.path.join(HERE, "..", "pipelines", "probability_calendar.py")
if not os.path.exists(MODULE_PATH):
    MODULE_PATH = os.path.join(HERE, "probability_calendar.py")


def _load_module():
    yf = types.ModuleType("yfinance")
    sys.modules.setdefault("yfinance", yf)
    utils = types.ModuleType("utils")
    utils.__path__ = []
    cache_mod = types.ModuleType("utils.cache")

    class _Cache:
        def load(self, key):
            return None

        def save(self, key, data):
            pass

    cache_mod.Cache = _Cache
    lock_mod = types.ModuleType("utils.file_lock")
    lock_mod.atomic_write_json = lambda path, data: None
    logger_mod = types.ModuleType("utils.logger")

    class _Logger:
        def log(self, *a, **k):
            pass

    logger_mod.pulse_logger = _Logger()
    sys.modules["utils"] = utils
    sys.modules["utils.cache"] = cache_mod
    sys.modules["utils.file_lock"] = lock_mod
    sys.modules["utils.logger"] = logger_mod

    spec = importlib.util.spec_from_file_location(
        "probability_calendar_under_test", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


pc = _load_module()


class Facts(dict):
    def __missing__(self, key):
        return None


class FakeCache:
    def __init__(self, payload):
        self.payload = payload

    def load(self, key):
        return self.payload

    def save(self, key, data):
        pass


def ev(dstr, title, impact="High"):
    return {"event_date": dstr, "title": title, "impact": impact}


def run_case(frozen_dt, events):
    class FrozenDateTime(datetime):
        FIXED = frozen_dt

        @classmethod
        def now(cls, tz=None):
            return cls.FIXED

    pc.datetime = FrozenDateTime
    cal = pc.ProbabilityCalendar.__new__(pc.ProbabilityCalendar)
    cal.cache = FakeCache({"data": {"events": events}})
    cal._mem = None
    cal._mem_ts = None
    cal._market_facts = lambda today, now: Facts(
        {"travel_pct": None, "tonight_travel_pct": None})
    cal._pulse_confidence = lambda: None
    cal._load_history = lambda: {}
    cal._save_history = lambda history: None
    cal._load_earnings_markers = lambda: []
    return cal._compute()


def day(payload, dstr):
    for week in payload["weeks"]:
        for block in week["days"]:
            if block["date"] == dstr:
                return block
    raise AssertionError(f"no block for {dstr}")


def ratings(payload):
    return [b["rating"] for w in payload["weeks"] for b in w["days"]]


results = []


def check(name, cond):
    results.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), "-", name)


# The real week of Nov 16-20, 2026: Retail Sales lands Tuesday 8:30 AM.
WEEK = [ev("2026-11-17", "Retail Sales m/m"),
        ev("2026-11-17", "Core Retail Sales m/m")]

# Case A — Sunday baseline: everything prospective.
pA = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK)
tue, wed, fri = (day(pA, "2026-11-17"), day(pA, "2026-11-18"),
                 day(pA, "2026-11-20"))
check("Sunday: release day prospective",
      any("the day's range is built around the print" in r
          for r in tue["reasons"]))
check("Sunday: day-after says the print lands Tuesday",
      any("Day after the" in r and "the print lands Tuesday" in r
          for r in wed["reasons"]))
check("Sunday: week context still counting from Tuesday",
      any("Week context — still counting" in r and "from Tuesday" in r
          for r in fri["reasons"]))
check("Sunday: no landed wording anywhere",
      not any("landed" in r for w in pA["weeks"] for b in w["days"]
              for r in b["reasons"]))

# Case B — Tuesday 7:00 AM: print day, not yet landed.
pB = run_case(datetime(2026, 11, 17, 7, 0, tzinfo=pc.ET), WEEK)
tue, wed = day(pB, "2026-11-17"), day(pB, "2026-11-18")
check("Tue 7 AM: release day still prospective",
      any("the day's range is built around the print" in r
          for r in tue["reasons"]))
check("Tue 7 AM: day-after still says lands Tuesday",
      any("the print lands Tuesday" in r for r in wed["reasons"]))

# Case C — Tuesday 10:00 AM: the print has landed.
pC = run_case(datetime(2026, 11, 17, 10, 0, tzinfo=pc.ET), WEEK)
tue, wed = day(pC, "2026-11-17"), day(pC, "2026-11-18")
check("Tue 10 AM: release day reads landed",
      any("landed at 8:30 AM ET" in r and "trades the repricing" in r
          for r in tue["reasons"]))
check("Tue 10 AM: day-after reads landed this morning",
      any("Day after the" in r and "landed this morning" in r
          for r in wed["reasons"]))

# Case D — Wednesday 9:00 AM: Pierre's screenshot, a day later.
pD = run_case(datetime(2026, 11, 18, 9, 0, tzinfo=pc.ET), WEEK)
wed, fri = day(pD, "2026-11-18"), day(pD, "2026-11-20")
check("Wed: day-after reads landed Tuesday",
      any("Day after the" in r and "landed Tuesday" in r
          for r in wed["reasons"]))
check("Wed: week context reads landed Tuesday, state kept",
      any("Week context" in r and "landed Tuesday" in r
          and "keeps the state that print set" in r
          for r in fri["reasons"]))
check("Wed: no pending wording survives anywhere",
      not any("still being counted" in r or "still counting" in r
              for w in pD["weeks"] for b in w["days"] for r in b["reasons"]))

# Wording only: tiers identical in all four states.
check("Tiers unchanged across checkpoint states",
      ratings(pA) == ratings(pB) == ratings(pC) == ratings(pD))

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
