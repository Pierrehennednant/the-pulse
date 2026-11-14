"""Week-rollover regression tests for the Day Probability Calendar.

Covers the 2026-11-14 bug Pierre caught on a Saturday screenshot: the
page had rolled to the week of Nov 16 while the EC cache still held
the week of Nov 9-13, so Monday inherited Friday's PPI as a day-after
boost and every day was stamped "quiet week" from an empty event set.

Run: python3 tests/test_calendar_week_rollover.py
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
    # Stub the environment-only dependencies before import.
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


def run_case(frozen_dt, events, markers=()):
    """Compute one calendar payload with time frozen and the EC cache
    holding exactly `events`."""
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
    cal._load_earnings_markers = lambda: list(markers)
    return cal._compute()


def day(payload, dstr):
    for week in payload["weeks"]:
        for block in week["days"]:
            if block["date"] == dstr:
                return block
    raise AssertionError(f"no block for {dstr}")


def all_reasons(payload):
    out = []
    for week in payload["weeks"]:
        for block in week["days"]:
            out.extend(block["reasons"])
    return out


NVDA = [{"symbol": "NVDA", "name": "NVIDIA", "date": "2026-11-17",
         "session": "after_close"}]
LAST_WEEK = [ev("2026-11-10", "CPI m/m"), ev("2026-11-10", "Core CPI m/m"),
             ev("2026-11-13", "PPI m/m"), ev("2026-11-13", "Core PPI m/m"),
             ev("2026-11-12", "Unemployment Claims", "Medium")]
NEXT_WEEK = [ev("2026-11-17", "Retail Sales m/m"),
             ev("2026-11-19", "Unemployment Claims", "Medium")]

results = []


def check(name, cond):
    results.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), "-", name)


# Case 1 — the bug itself: Saturday Nov 14, cache still holds last week.
p = run_case(datetime(2026, 11, 14, 9, 41, tzinfo=pc.ET), LAST_WEEK, NVDA)
ratings = [b["rating"] for w in p["weeks"] for b in w["days"]]
check("Sat rollover: all five days PENDING", ratings == ["PENDING"] * 5)
joined = " | ".join(all_reasons(p))
check("Sat rollover: no PPI inheritance", "PPI" not in joined)
check("Sat rollover: no invented quiet week", "Quiet week" not in joined)
wed = day(p, "2026-11-18")
check("Pending Wednesday still shows the NVDA marker",
      any("Earnings marker" in r and "NVDA" in r for r in wed["reasons"]))

# Case 2 — Sunday Nov 15, cache has rolled (both weeks present): Monday
# must NOT inherit Friday's PPI even though PPI sits in the cache.
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET),
             LAST_WEEK + NEXT_WEEK, NVDA)
mon = day(p, "2026-11-16")
check("Covered Monday: not PENDING", mon["rating"] != "PENDING")
check("Covered Monday: no day-after PPI across the week boundary",
      not any("Day after" in r for r in mon["reasons"])
      and not any("PPI" in r for r in mon["reasons"]))
tue = day(p, "2026-11-17")
check("Covered Tuesday: release day rated PROBABLE",
      tue["rating"] == "PROBABLE"
      and any("Retail Sales" in r for r in tue["reasons"]))

# Case 3 — same-week day-after still works: Wed Nov 4 after Tue Nov 3 CPI.
week = [ev("2026-11-03", "CPI m/m"), ev("2026-11-06", "Nonfarm Payrolls")]
p = run_case(datetime(2026, 11, 4, 10, 0, tzinfo=pc.ET), week)
wed = day(p, "2026-11-04")
check("Same-week day-after boost intact",
      any("Day after the CPI m/m release" in r for r in wed["reasons"]))

# Case 4 — a genuinely quiet week, once its calendar has landed, still
# earns the quiet-week factor (coverage must not mute real quiet).
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), NEXT_WEEK, NVDA)
thu = day(p, "2026-11-19")
check("Covered quiet week still applies the quiet-week factor",
      any("Quiet week" in r for r in thu["reasons"]))

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
