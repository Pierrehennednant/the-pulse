"""Event-tense tests for the Day Probability Calendar.

Covers the completion of Pierre's 2026-11-17 checkpoint-tense fix:
the release lines learned to say a print landed, but the Fed Chair
speech line and the FOMC decision-day line kept their pending wording
after the event — Monday's card still said the noon speech "hangs
over the session until it lands" at 4:46 PM. Both lines now key off
whether the event has landed (speech: its scheduled start; FOMC: the
2:00 PM announcement), with an unparseable speech time keeping the
prospective wording as the safe fallback. Scores and tiers must be
identical in every state — wording only.

Run: python3 tests/test_calendar_event_tense.py
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


def speech(dstr, title, speaker, time_est, impact="High"):
    return {"event_date": dstr, "title": title, "impact": impact,
            "is_speech": True, "speaker_type": speaker,
            "time_est": time_est}


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


results = []


def check(name, cond):
    results.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), "-", name)


WARSH = speech("2026-11-16", "Fed Chairman Warsh Speaks", "Fed Chair",
               "Mon Nov 16, 12:00 PM EST")
WEEK = [ev("2026-11-17", "Retail Sales m/m"),
        ev("2026-11-17", "Core Retail Sales m/m"), WARSH]

# Speech — Sunday baseline and Monday before the start: prospective.
for label, frozen in [("Sun", datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET)),
                      ("Mon 11 AM", datetime(2026, 11, 16, 11, 0, tzinfo=pc.ET))]:
    p = run_case(frozen, WEEK)
    mon = day(p, "2026-11-16")
    check(f"Speech {label}: prospective wording",
          any("The Fed Chair speaks at 12:00 PM" in r
              and "hangs over the session until it lands" in r
              for r in mon["reasons"]))
    check(f"Speech {label}: rating DEFENSIVE", mon["rating"] == "DEFENSIVE")

# Speech — Monday after the start: landed wording, same rating.
p = run_case(datetime(2026, 11, 16, 13, 0, tzinfo=pc.ET), WEEK)
mon = day(p, "2026-11-16")
check("Speech Mon 1 PM: landed wording",
      any("The Fed Chair spoke at 12:00 PM" in r
          and "the speech has landed" in r for r in mon["reasons"]))
check("Speech Mon 1 PM: no pending wording left",
      not any("hangs over the session until it lands" in r
              for r in mon["reasons"]))
check("Speech Mon 1 PM: rating still DEFENSIVE",
      mon["rating"] == "DEFENSIVE")

# Speech — unparseable time keeps the prospective wording (fallback).
notime = speech("2026-11-16", "Fed Chairman Warsh Speaks", "Fed Chair", "")
p = run_case(datetime(2026, 11, 16, 13, 0, tzinfo=pc.ET),
             [ev("2026-11-17", "Retail Sales m/m"), notime])
mon = day(p, "2026-11-16")
check("Speech unknown time: prospective fallback at 1 PM",
      any("The Fed Chair speaks at time not listed" in r
          for r in mon["reasons"]))

# FOMC — decision day Wed Dec 9, 2026, before and after 2 PM.
FOMC_WK = [speech("2026-12-09", "Fed Chairman Warsh Speaks", "Fed Chair",
                  "Wed Dec 9, 2:00 PM EST")]
p = run_case(datetime(2026, 12, 9, 10, 0, tzinfo=pc.ET), FOMC_WK)
wed_am = day(p, "2026-12-09")
check("FOMC 10 AM: prospective wording",
      any("morning is usually dead ahead of the 2 PM announcement" in r
          for r in wed_am["reasons"]))
p = run_case(datetime(2026, 12, 9, 15, 0, tzinfo=pc.ET), FOMC_WK)
wed_pm = day(p, "2026-12-09")
check("FOMC 3 PM: landed wording",
      any("the 2 PM announcement has landed" in r
          for r in wed_pm["reasons"]))
check("FOMC: rating identical before and after the announcement",
      wed_am["rating"] == wed_pm["rating"])

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
