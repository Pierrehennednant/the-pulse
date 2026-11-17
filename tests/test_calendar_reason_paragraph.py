"""Reason-paragraph tests for the Day Probability Calendar.

Covers Pierre's 2026-11-17 display call: the day blocks stacked each
reason on its own line, which read like bullet points; he wants the
reasons as one paragraph. The module now derives `reasons_paragraph`
from the reasons list at render time — same words, same order, each
line a sentence — and both templates render that single paragraph.
The reasons list, the ratings and the frozen history record are
untouched.

Run: python3 tests/test_calendar_reason_paragraph.py
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


def run_case(frozen_dt, events, history=None):
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
    cal._load_history = lambda: dict(history or {})
    cal._save_history = lambda h: None
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


WEEK = [ev("2026-11-17", "Retail Sales m/m"),
        ev("2026-11-17", "Core Retail Sales m/m")]

# Case 1 — live week, Wednesday after the print.
p = run_case(datetime(2026, 11, 18, 9, 0, tzinfo=pc.ET), WEEK)
blocks = [b for w in p["weeks"] for b in w["days"]]
check("Every block carries a paragraph",
      all(isinstance(b.get("reasons_paragraph"), str)
          and b["reasons_paragraph"] for b in blocks))
wed = day(p, "2026-11-18")
para = wed["reasons_paragraph"]
check("Paragraph is a single line", "\n" not in para)
check("Every reason survives into the paragraph, in order",
      all(r.strip().rstrip(".") in para for r in wed["reasons"])
      and [para.index(r.strip().rstrip(".")[:24]) for r in wed["reasons"]]
      == sorted(para.index(r.strip().rstrip(".")[:24])
                for r in wed["reasons"]))
check("Sentences are stop-separated",
      "stays in play. Quiet week" in para)
check("Paragraph ends with a full stop", para.endswith("."))
check("Ratings untouched (Tue PROBABLE, Wed MID on stubbed facts)",
      day(p, "2026-11-17")["rating"] == "PROBABLE"
      and wed["rating"] == "MID")
check("Reasons list still present and unchanged in shape",
      isinstance(wed["reasons"], list) and len(wed["reasons"]) >= 2)

# Case 2 — a frozen past day: the stored record has no paragraph; the
# payload derives one and the record itself is not modified.
stored = {"date": "2026-11-16", "weekday": "Monday", "rating": "DEFENSIVE",
          "reasons": ["First stored reason",
                      "Second stored reason — with a dash; and a semicolon"],
          "is_today": True}
p2 = run_case(datetime(2026, 11, 18, 9, 0, tzinfo=pc.ET), WEEK,
              history={"2026-11-16": stored})
mon = day(p2, "2026-11-16")
check("Frozen block gains a derived paragraph",
      mon["reasons_paragraph"] ==
      "First stored reason. Second stored reason — with a dash; "
      "and a semicolon.")
check("Frozen record itself unmodified",
      "reasons_paragraph" not in stored
      and mon["rating"] == "DEFENSIVE"
      and mon["reasons"] == stored["reasons"])

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
