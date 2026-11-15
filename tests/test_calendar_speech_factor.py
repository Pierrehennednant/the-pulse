"""Fed Chair speech-overhang tests for the Day Probability Calendar.

Pierre's design (2026-11-15): a scheduled Fed Chair speech before the
close is an event overhang worth -1 — the Chair is the one speaker
whose speeches measurably move markets, and the morning trades into
it. Presidential speeches are NOT rated: they count in his design
only when topic and time connect, and no advance topic signal exists.

Run: python3 tests/test_calendar_speech_factor.py
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


def run_case(frozen_dt, events, markers=(), facts=None):
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
        facts if facts is not None
        else {"travel_pct": None, "tonight_travel_pct": None})
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


results = []


def check(name, cond):
    results.append((name, bool(cond)))
    print(("PASS" if cond else "FAIL"), "-", name)


WARSH = speech("2026-11-16", "Fed Chairman Warsh Speaks", "Fed Chair",
               "Mon Nov 16, 12:00 PM EST")
WEEK = [ev("2026-11-17", "Retail Sales m/m"),
        ev("2026-11-17", "Core Retail Sales m/m"),
        ev("2026-11-19", "Unemployment Claims", "Medium")]

# Case 1 — the live week replayed: Sun Nov 15, Warsh Mon noon, retail Tue.
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK + [WARSH])
mon = day(p, "2026-11-16")
check("Warsh Monday: DEFENSIVE, not DON'T LOOK",
      mon["rating"] == "DEFENSIVE")
check("Warsh Monday: speech overhang reason present",
      any("Fed Chair speaks at 12:00 PM" in r for r in mon["reasons"]))
check("Warsh Monday: quiet week still applies (speech is not a red day)",
      any("Quiet week" in r for r in mon["reasons"]))
tue = day(p, "2026-11-17")
check("Retail Tuesday unaffected: PROBABLE", tue["rating"] == "PROBABLE")

# Case 2 — Chair speech stacks against a release day (+2 -1 = +1, MID).
wk = [ev("2026-11-03", "CPI m/m"),
      speech("2026-11-03", "Fed Chairman Warsh Speaks", "Fed Chair",
             "Tue Nov 3, 10:00 AM EST"),
      ev("2026-11-06", "Nonfarm Payrolls")]
p = run_case(datetime(2026, 11, 2, 8, 0, tzinfo=pc.ET), wk)
tue = day(p, "2026-11-03")
check("Speech on a release day stacks to MID",
      tue["rating"] == "MID"
      and any("Fed Chair speaks at 10:00 AM" in r for r in tue["reasons"]))

# Case 3 — another official's speech is ignored entirely.
barr = speech("2026-11-18", "Fed Governor Barr Speaks", "Other",
              "Wed Nov 18, 2:00 PM EST")
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK + [barr])
wed = day(p, "2026-11-18")
check("Non-Chair speech ignored",
      not any("speaks at" in r for r in wed["reasons"])
      and wed["rating"] == "MID")

# Case 4 — an evening Chair speech (after the close) does not tax the day.
late = speech("2026-11-16", "Fed Chairman Warsh Speaks", "Fed Chair",
              "Mon Nov 16, 7:00 PM EST")
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK + [late])
mon = day(p, "2026-11-16")
check("Evening Chair speech: no overhang factor",
      not any("Fed Chair speaks" in r for r in mon["reasons"]))

# Case 5 — a Chair speech with no listed time still counts.
notime = speech("2026-11-16", "Fed Chairman Warsh Speaks", "Fed Chair", "")
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK + [notime])
mon = day(p, "2026-11-16")
check("Untimed Chair speech counts (time not listed)",
      any("Fed Chair speaks at time not listed" in r for r in mon["reasons"]))

# Case 6 — a Presidential speech is not rated (no advance topic signal).
pres = speech("2026-11-16", "President Trump Speaks", "Presidential",
              "Mon Nov 16, 11:00 AM EST")
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK + [pres])
mon = day(p, "2026-11-16")
check("Presidential speech not rated",
      not any("speaks at" in r for r in mon["reasons"])
      and mon["rating"] == "DEFENSIVE")

# Case 7 — no speech factor on an FOMC decision day.
fomc_wk = [ev("2026-12-09", "Federal Funds Rate"),
           speech("2026-12-09", "Fed Chairman Warsh Speaks", "Fed Chair",
                  "Wed Dec 9, 10:00 AM EST")]
p = run_case(datetime(2026, 12, 6, 10, 0, tzinfo=pc.ET), fomc_wk)
wed = day(p, "2026-12-09")
check("FOMC day: decision only, no speech double-count",
      wed["rating"] == "DEFENSIVE"
      and not any("Fed Chair speaks" in r for r in wed["reasons"]))

# Case 8 — rollover regression: stale Saturday cache still renders PENDING.
stale = [ev("2026-11-13", "PPI m/m"),
         speech("2026-11-13", "Fed Chairman Warsh Speaks", "Fed Chair",
                "Fri Nov 13, 12:00 PM EST")]
p = run_case(datetime(2026, 11, 14, 9, 41, tzinfo=pc.ET), stale)
ratings = [b["rating"] for w in p["weeks"] for b in w["days"]]
check("Stale Saturday still PENDING everywhere", ratings == ["PENDING"] * 5)

# Case 9 - audit D2: a speech must NOT act as a hard factor for the
# earnings-marker exemption. Marker morning with NO prior-day release
# (so no day-after +1): quiet -1 + speech -1 + prior-range-96th -1 = -3
# would lock the morning out; the exemption must still lift it.
marker_mon = [{"date": "2026-11-23", "company": "NVDA", "name": "Nvidia",
               "note": "After the close"}]
wk2 = [ev("2026-11-25", "Retail Sales m/m"),
       speech("2026-11-24", "Fed Chairman Warsh Speaks", "Fed Chair",
              "Tue Nov 24, 12:00 PM EST")]
p = run_case(datetime(2026, 11, 23, 17, 0, tzinfo=pc.ET),
             wk2, markers=marker_mon,
             facts={"travel_pct": None, "tonight_travel_pct": None,
                    "prev_rank": 96})
tue = day(p, "2026-11-24")
check("D2: post-earnings morning with speech stacks to DEFENSIVE, not locked out",
      tue["rating"] == "DEFENSIVE"
      and any("Earnings exemption" in r for r in tue["reasons"])
      and any("Fed Chair speaks at 12:00 PM" in r for r in tue["reasons"]))

# Case 10 - audit D3: a pre-open (08:00) Chair speech does not count.
early = speech("2026-11-16", "Fed Chairman Warsh Speaks", "Fed Chair",
               "Mon Nov 16, 08:00 AM EST")
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK + [early])
mon = day(p, "2026-11-16")
check("D3: 08:00 Chair speech - no factor, no suspect note",
      not any("Fed Chair speaks" in r for r in mon["reasons"])
      and not any("not classified" in r for r in mon["reasons"])
      and mon["rating"] == "DEFENSIVE")

# Case 11 - audit Q4: a surname-only title the EC didn't classify as a
# Chair event surfaces as a 0-point tripwire note, never as points.
suspect = {"event_date": "2026-11-16", "title": "Warsh Speaks",
           "impact": "High", "is_speech": True, "speaker_type": "Other",
           "time_est": "Mon Nov 16, 10:00 AM EST"}
p = run_case(datetime(2026, 11, 15, 10, 0, tzinfo=pc.ET), WEEK + [suspect])
mon = day(p, "2026-11-16")
check("Q4: unclassified Chair-named event - tripwire note, no points",
      any("Possible Fed Chair event not classified" in r
          and "Warsh Speaks" in r for r in mon["reasons"])
      and not any("Fed Chair speaks at" in r for r in mon["reasons"])
      and mon["rating"] == "DEFENSIVE")

failed = [n for n, ok in results if not ok]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(1 if failed else 0)
