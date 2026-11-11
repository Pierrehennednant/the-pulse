"""
Day Probability Calendar — a ratings-only day planner (replaces the old
daily narrative panel).

WHAT IT IS
----------
Each trading day gets ONE rating, computed in advance from the week's recorded
facts — never from a morning-of feel:

    PROBABLE    a catalyst or follow-through day; the day is likely worth trading
    MID         baseline / conditional — no boost, no positive evidence of quiet
    DEFENSIVE   low — positive evidence the day runs quiet or two-sided
    DON'T LOOK  don't even open the charts

Every rating carries short reason strings naming the factors that produced it.

GUARDRAIL (the reason this page may live OUTSIDE the dashboard lockdown):
the output contains day ratings and reason strings ONLY. No bias, no confidence
percentages, no pillar directions, no scores — ever. The Pulse's state is read
internally at checkpoints (a 55% gate crossing is a checkpoint fact) but is
never rendered. Treat any directional field in the output as a bug.

MODEL — SUNDAY BASELINE + CHECKPOINT REVISIONS
----------------------------------------------
Ratings are computed at read time from the facts recorded so far, which is
what the settled design's checkpoints mean mechanically:

  * The week's shape (red-folder days, FOMC position, thin sessions) is known
    on Sunday — that is the baseline.
  * After each overnight, the travel fact lands and today's rating can flip
    (the locked overnight-travel rule forces DON'T LOOK, precedence #1).
  * After each session closes, its range percentile becomes a recorded fact
    for the next morning (range exhaustion: >=p95 downgrade, >=p97 strong).
  * After each red-folder print, the next day inherits "day after a release".
  * A Pulse 55% gate crossing at read time is a checkpoint fact for today.
  * Once a day ends, its rating is FROZEN: today's block is written to the
    history store on every compute, and a past day renders its frozen
    block verbatim — never a recomputation from today's facts (the rating
    that governed the day is the rating on record; Friday grading reads
    it). A past day with no frozen record is recomputed WITHOUT today's
    live facts (no range block, no today-only block).

Precedence: (1) locked hard rules (overnight travel) beat everything;
(2) the calendar is the plan — ratings move only when a recorded fact lands;
(3) "no red-folder event that day" is the ABSENCE of a boost, never by itself
a downgrade — LOW requires positive evidence of quiet.

DATA SOURCES (all already used by this codebase)
------------------------------------------------
  * Red-folder events: the EC pipeline's cache — this week only. The
    calendar works week to week (Pierre, 2026-11-09: the next-week
    baseline was all-MID noise and was removed).
  * FOMC decision dates + US market calendar: static tables below (2026–2027).
  * Ranges / overnight travel / weekend gap: Yahoo bars via yfinance, the
    pattern used by pipelines/market_structure.py. The travel lock judges
    the FULL overnight session range (18:00 → 08:30 ET) against the locked
    2% line (TRAVEL_LOCK_PCT — Pierre's settled measure, 2026-11-06; the
    window's endpoint moved from 09:30 to 08:30 on 2026-11-11 — the 8:30 AM
    ET data hour is where Pierre's overnight ends). The adaptive
    p94 line is NOT used here: it judges the NET overnight move for the
    morning-read flag, a different rule (an earlier build applied it to
    travel by mistake — corrected 2026-11-10).
  * Pulse strength: processors.snapshot_generator latest snapshot — only the
    confidence number is compared against the 55% gate, internally.
  * Earnings markers: data/earnings_markers.json — top-weight Nasdaq-100
    earnings nights, marked like election markers (never scored, beat/miss
    never predicted). A marker night EXEMPTS the next morning from a
    DON'T LOOK built on quietness alone; the overnight checkpoint decides.
  * Futures roll: derived — the third Friday of Mar/Jun/Sep/Dec and its week.
    A reason note only; never a standalone downgrade (modest in ES/NQ).

Factors deliberately NOT built (evidence said no): day-before-release
compression, weekday direction, pre-FOMC drift, options-expiry positioning.
"""

import json
import os
from datetime import date, datetime, time, timedelta

import pytz
import yfinance as yf

from utils.cache import Cache
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger

ET = pytz.timezone("America/New_York")

CACHE_FILE = "/data/probability_calendar_cache.json"
CACHE_TTL_S = 240  # same cadence as pipelines/market_structure.py

# Frozen record of past days' blocks (date ISO -> the day block exactly as
# it rendered while the day was live). A past day's rating never changes.
HISTORY_FILE = "/data/probability_calendar_history.json"

EARNINGS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "earnings_markers.json",
)

# ── FOMC decision days (scheduled meetings ending in a rate decision) ──────
# Verify against federalreserve.gov/monetarypolicy/fomccalendars.htm before
# extending past 2027.
FOMC_DECISION_DATES = {
    # 2026
    date(2026, 1, 28), date(2026, 3, 18), date(2026, 4, 29), date(2026, 6, 17),
    date(2026, 7, 29), date(2026, 9, 16), date(2026, 10, 28), date(2026, 12, 9),
    # 2027
    date(2027, 1, 27), date(2027, 3, 17), date(2027, 4, 28), date(2027, 6, 16),
    date(2027, 7, 28), date(2027, 9, 15), date(2027, 10, 27), date(2027, 12, 8),
}

# ── US market calendar, 2026–2027 ───────────────────────────────────────────
# Full closures: no session at all (the day simply isn't rated).
FULL_CLOSE = {
    date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16), date(2026, 4, 3),
    date(2026, 5, 25), date(2026, 6, 19), date(2026, 7, 3), date(2026, 9, 7),
    date(2026, 11, 26), date(2026, 12, 25),
    date(2027, 1, 1), date(2027, 1, 18), date(2027, 2, 15), date(2027, 3, 26),
    date(2027, 5, 31), date(2027, 6, 18), date(2027, 7, 5), date(2027, 9, 6),
    date(2027, 11, 25), date(2027, 12, 24),
}
# Bond market closed while equities/futures trade: structurally thin sessions.
BOND_CLOSED_THIN = {
    date(2026, 10, 12): "Columbus Day",
    date(2026, 11, 11): "Veterans Day",
    date(2027, 10, 11): "Columbus Day",
    date(2027, 11, 11): "Veterans Day",
}
# Shortened equity sessions (early close) — futures follow in the evening.
HALF_DAYS = {
    date(2026, 11, 27), date(2026, 12, 24), date(2027, 11, 26),
}

# FOMC-day machinery on the Forex Factory feed (mirrors the EC pipeline's
# INDEPENDENT_RED_FOLDER_EVENTS / exclusions semantics).
FOMC_TITLES = {
    "Federal Funds Rate", "FOMC Press Conference",
    "FOMC Economic Projections", "FOMC Statement",
}
EC_EXCLUSIONS = {"FOMC Meeting Minutes", "FOMC Statement"}

TIER_PROBABLE = "PROBABLE"
TIER_MID = "MID"
TIER_DEFENSIVE = "DEFENSIVE"
TIER_DONT_LOOK = "DON'T LOOK"

# ── One-time seed for the week the freeze ships (week of 2026-11-09) ────
# Monday and Tuesday were already past when the freeze landed, so their
# governing ratings are written once from the verified live record:
# Monday was a travel lock (overnight travel 2.7% vs the locked 2% line,
# on top of exhaustion from Friday Nov 6's outsized range); Tuesday was
# the CPI release day, PROBABLE, with 1.9% overnight travel — no lock.
# String forms match exactly what the live code rendered those mornings.
SEED_WEEK_MONDAY = date(2026, 11, 9)
SEED_HISTORY = {
    "2026-11-09": {
        "date": "2026-11-09",
        "weekday": "Monday",
        "rating": TIER_DONT_LOOK,
        "reasons": [
            "LOCKED RULE — overnight travel 2.7% crossed the locked 2% "
            "travel line (full overnight range, round trips included). "
            "Extended overnight travel makes this a no-trade morning, "
            "whatever else the week holds",
            "Range exhaustion — Friday Nov 6 printed a range at the 97th "
            "percentile or higher; days after huge ranges run wide but "
            "inefficient and reverse about two times in three",
        ],
        "is_today": False,
    },
    "2026-11-10": {
        "date": "2026-11-10",
        "weekday": "Tuesday",
        "rating": TIER_PROBABLE,
        "reasons": [
            "Red-folder release day — CPI m/m, Core CPI m/m at 8:30 AM "
            "ET; the day's range is built around the print",
            "Overnight travel 1.9% — under the locked 2% travel line, so "
            "no travel lock. The path: a one-way slide to -1.5%; net -1.2%.",
        ],
        "is_today": False,
    },
}


def _is_trading_day(d):
    return d.weekday() < 5 and d not in FULL_CLOSE


def _next_trading_day(d):
    d = d + timedelta(days=1)
    while not _is_trading_day(d):
        d += timedelta(days=1)
    return d


def _prior_trading_day(d):
    d = d - timedelta(days=1)
    while not _is_trading_day(d):
        d -= timedelta(days=1)
    return d


def _third_friday(year, month):
    d = date(year, month, 1)
    first_friday_offset = (4 - d.weekday()) % 7  # weekday(): Mon=0 … Fri=4
    return d + timedelta(days=first_friday_offset + 14)


def _roll_week_monday(d):
    """Monday of the week containing a quarterly expiry Friday, else None."""
    for month in (3, 6, 9, 12):
        expiry = _third_friday(d.year, month)
        monday = expiry - timedelta(days=expiry.weekday())
        if monday <= d <= expiry + timedelta(days=2):
            return monday
    return None


def _pct_rank(distribution, value):
    if not distribution:
        return None
    return 100.0 * sum(1 for x in distribution if x <= value) / len(distribution)


def _percentile(sorted_vals, q):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * (q / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


# ── Locked overnight-travel line (Pierre, settled 2026-11-06) ──
# The travel lock fires when the FULL overnight session range (18:00 → 08:30
# ET, round trips included) reaches 2% of the prior close. Pierre settled
# this measure himself on 2026-11-06 ("over 2% is the better measure"),
# judging the full range against the dip-excluded floor figure (~1.85% that
# night). The window's endpoint moved from 09:30 to 08:30 on 2026-11-11
# (Pierre: the 8:30 AM ET data hour is where his overnight ends). NOT the
# adaptive p94 line — that one judges the NET overnight move for the
# morning-read flag, a separate rule.
TRAVEL_LOCK_PCT = 2.0


def _travel_story(facts):
    """One-line overnight path story: the extremes vs the prior close, which
    extreme came first (round trip vs one-way), and the net move. Detail by
    Pierre's request (2026-11-10): the travel number alone doesn't say
    whether the overnight was violent or a drift."""
    hi = facts.get("travel_high_pct")
    lo = facts.get("travel_low_pct")
    net = facts.get("travel_net_pct")
    if hi is None or lo is None or net is None:
        return ""
    if lo <= -0.75 and hi >= 0.75:
        if facts.get("travel_low_first"):
            path = (f"dipped to {lo:+.1f}% then ripped back to {hi:+.1f}% "
                    f"— a round trip")
        else:
            path = (f"ran to {hi:+.1f}% then sold back to {lo:+.1f}% "
                    f"— a round trip")
    elif abs(lo) >= abs(hi):
        path = f"a one-way slide to {lo:+.1f}%"
    else:
        path = f"a one-way run to {hi:+.1f}%"
    return f"The path: {path}; net {net:+.1f}%."


# ── Summer mode (Pierre's LOCKED next-summer plan, 2026-11-09) ──
SUMMER_MONTHS = (6, 7, 8)

SUMMER_RULES_LINES = [
    "Red-folder windows only. The window opens on the print day and carries "
    "through the rest of that week. Before the week's first print, you're out.",
    "Neutral Pulse — ignore the day. A green light is permission to trade, "
    "not a bigger target.",
    "Print day with a Pulse edge — full target. Carry days — 2R cap, even on "
    "a green light.",
    "Friday print — Friday only. Nothing carries into next week.",
    "FOMC — no FOMC morning. The window opens the next day.",
    "Size — half size as normal.",
]

SUMMER_RULES_WHY = ("Why these rules exist: summer chop nearly broke me in 2026. "
                    "Thin summer sessions move without reliable follow-through — "
                    "the frustration was the danger, not the losses. These rules "
                    "were decided in November, in the cold. Summer Pierre doesn't "
                    "get to renegotiate them.")


def _summer_banner(week_dates):
    """The locked summer rules, shown at the top of the calendar in summer."""
    if any(d.month in SUMMER_MONTHS for d in week_dates):
        return {"lines": SUMMER_RULES_LINES, "why": SUMMER_RULES_WHY}
    return None


class ProbabilityCalendar:
    def __init__(self):
        self.cache = Cache()
        self._mem = None
        self._mem_ts = None

    # ── public API ──────────────────────────────────────────────────────────
    def fetch(self, force=False):
        now = datetime.now(ET)
        if (not force and self._mem and self._mem_ts
                and (now - self._mem_ts).total_seconds() < CACHE_TTL_S):
            return self._mem
        data = self._compute()
        self._mem = data
        self._mem_ts = now
        try:
            atomic_write_json(CACHE_FILE, {"timestamp": now.isoformat(), "data": data})
        except Exception as e:
            pulse_logger.log(f"Probability calendar cache write failed: {e}", level="WARNING")
        return data

    # ── inputs ──────────────────────────────────────────────────────────────
    def _load_events(self):
        """events: date -> [{'title', 'time'}] red-folder events, this week.

        From the EC pipeline's cache. Data prints only: speeches are not
        release days (FOMC-day titles are handled separately)."""
        events = {}

        def add(dstr, title, time_str):
            if not dstr or not title:
                return
            events.setdefault(dstr, []).append({"title": title, "time": time_str})

        # This week — EC pipeline cache (events already classified there).
        try:
            payload = self.cache.load("economic_calendar") or {}
            ec_data = payload.get("data") or payload
            for ev in ec_data.get("events") or []:
                if ev.get("impact") != "High":
                    continue
                title = ev.get("title") or ""
                if title in FOMC_TITLES:
                    add(ev.get("event_date"), title, None)
                elif title in EC_EXCLUSIONS or ev.get("is_speech"):
                    continue
                else:
                    add(ev.get("event_date"), title, None)
        except Exception as e:
            pulse_logger.log(f"Calendar: EC cache read failed: {e}", level="WARNING")

        return events

    def _load_earnings_markers(self):
        try:
            with open(EARNINGS_PATH) as f:
                data = json.load(f)
            return data.get("markers") or []
        except Exception:
            return []

    def _load_history(self):
        """Frozen blocks for past days: date ISO -> the day block exactly
        as it rendered while the day was live."""
        try:
            with open(HISTORY_FILE) as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_history(self, history):
        try:
            atomic_write_json(HISTORY_FILE, history)
        except Exception as e:
            pulse_logger.log(f"Probability calendar history write failed: {e}",
                             level="WARNING")

    def _seed_history(self, history, monday):
        """One-time seed (see SEED_HISTORY): if this is the week of
        2026-11-09, write the governing ratings for its already-past
        Monday/Tuesday when they are missing. Returns True when anything
        was added."""
        if monday != SEED_WEEK_MONDAY:
            return False
        added = False
        for iso, block in SEED_HISTORY.items():
            if iso not in history:
                history[iso] = block
                added = True
        return added

    def _pulse_confidence(self):
        """Latest snapshot confidence — used ONLY against the 55% gate,
        internally. Never rendered (see module guardrail)."""
        try:
            from processors.snapshot_generator import snapshot_generator
            latest = snapshot_generator.get_latest()
            if latest:
                return (latest.get("bias") or {}).get("confidence")
        except Exception:
            pass
        return None

    def _fetch_bars(self, symbol, period, interval):
        """[(timestamp_et, open, high, low, close)] — market_structure pattern."""
        try:
            df = yf.Ticker(symbol).history(period=period, interval=interval)
            if df is None or df.empty:
                return []
            idx = df.index
            if idx.tz is None:
                idx = idx.tz_localize("America/New_York")
            else:
                idx = idx.tz_convert("America/New_York")
            bars = []
            for ts, o, h, l, c in zip(
                idx, df["Open"].values, df["High"].values,
                df["Low"].values, df["Close"].values,
            ):
                vals = (float(o), float(h), float(l), float(c))
                if any(v != v for v in vals):  # NaN guard
                    continue
                bars.append((ts, *vals))
            return bars
        except Exception as e:
            pulse_logger.log(f"Calendar: bars fetch failed {symbol} {interval}: {e}",
                             level="WARNING")
            return []

    def _market_facts(self, today, now):
        """Recorded market facts: prior-session range percentiles, today's
        overnight travel (range + path detail), Monday's weekend gap."""
        facts = {"prev_rank": None, "prev2_rank": None,
                 "prev_close_pos": None, "median10_rank": None,
                 "travel_pct": None, "travel_high_pct": None,
                 "travel_low_pct": None, "travel_net_pct": None,
                 "travel_low_first": None, "weekend_gap_pct": None}

        # NQ daily bars → completed sessions and range percentiles.
        nq_daily = self._fetch_bars("NQ=F", "2y", "1d")
        completed = []
        for ts, o, h, l, c in nq_daily:
            d = ts.date()
            if d < today or (d == today and now.time() >= time(16, 0)):
                completed.append((d, o, h, l, c))
        if completed:
            ranges = [h - l for _, _, h, l, _c in completed]
            facts["prev_rank"] = _pct_rank(ranges, ranges[-1])
            if len(ranges) >= 2:
                facts["prev2_rank"] = _pct_rank(ranges, ranges[-2])
            last10 = ranges[-10:]
            ranks10 = [_pct_rank(ranges, r) for r in last10]
            ranks10 = [r for r in ranks10 if r is not None]
            if ranks10:
                facts["median10_rank"] = sorted(ranks10)[len(ranks10) // 2]
            _d, _o, ph, pl, pc = completed[-1]
            if ph > pl:
                facts["prev_close_pos"] = (pc - pl) / (ph - pl)

        # Overnight travel into today + weekend gap (NQ 5-minute bars).
        nq_5m = self._fetch_bars("NQ=F", "5d", "5m")
        if nq_5m and completed:
            prior_close = completed[-1][4]
            win_start = ET.localize(datetime.combine(
                today - timedelta(days=1), time(18, 0)))
            win_end = min(now, ET.localize(datetime.combine(today, time(8, 30))))
            window = [b for b in nq_5m if win_start <= b[0] <= win_end]
            if len(window) >= 10 and prior_close:
                hi_bar = max(window, key=lambda b: b[2])
                lo_bar = min(window, key=lambda b: b[3])
                hi, lo = hi_bar[2], lo_bar[3]
                facts["travel_pct"] = (hi - lo) / prior_close * 100
                facts["travel_high_pct"] = (hi / prior_close - 1) * 100
                facts["travel_low_pct"] = (lo / prior_close - 1) * 100
                facts["travel_net_pct"] = (window[-1][4] / prior_close - 1) * 100
                facts["travel_low_first"] = lo_bar[0] < hi_bar[0]
                if today.weekday() == 0:
                    facts["weekend_gap_pct"] = (window[0][1] / prior_close - 1) * 100
        return facts

    # ── rating engine ───────────────────────────────────────────────────────
    def _compute(self):
        now = datetime.now(ET)
        today = now.date()
        # This week = the week containing today; on Sat/Sun, the coming week.
        monday = today - timedelta(days=today.weekday())
        if today.weekday() >= 5:
            monday += timedelta(days=7)

        events = self._load_events()
        markers = self._load_earnings_markers()
        facts = self._market_facts(today, now)
        pulse_conf = self._pulse_confidence()

        history = self._load_history()
        history_dirty = self._seed_history(history, monday)

        days = []
        for offset in range(5):
            d = monday + timedelta(days=offset)
            if not _is_trading_day(d):
                continue
            if d < today and d.isoformat() in history:
                # Frozen: the block that governed the day, verbatim. A
                # past day's rating never changes after rollover.
                frozen = dict(history[d.isoformat()])
                frozen["is_today"] = False
                days.append(frozen)
                continue
            block = self._rate_day(
                d, today, events, markers, facts, pulse_conf,
                monday_of=monday, quiet_week_known=True,
                frozen_past=d < today)
            days.append(block)
            if d == today:
                # Save today's live block on every compute (overwrite) —
                # the day's last state before rollover is its record.
                history[d.isoformat()] = block
                history_dirty = True
        if history_dirty:
            self._save_history(history)
        weeks = [{"week_of": monday.isoformat(), "label": "This week",
                  "days": days}]
        week_dates = [monday + timedelta(days=o) for o in range(5)]
        return {"generated_at": now.isoformat(), "today": today.isoformat(),
                "updated_label": now.strftime("%b %-d, %Y · %-I:%M %p ET"),
                "summer_banner": _summer_banner(week_dates),
                "weeks": weeks}

    def _week_red_days(self, monday, events):
        """Distinct red-folder days (data prints or FOMC) in a Mon–Fri week."""
        count = 0
        for offset in range(5):
            d = monday + timedelta(days=offset)
            if not _is_trading_day(d):
                continue
            titles = {e["title"] for e in events.get(d.isoformat(), [])}
            if titles - FOMC_TITLES or d in FOMC_DECISION_DATES or titles & FOMC_TITLES:
                count += 1
        return count

    def _rate_day(self, d, today, events, markers, facts, pulse_conf,
                  monday_of, quiet_week_known, frozen_past=False):
        scored = []   # (points, text) — points drive the tier, text is rendered
        notes = []    # 0-point reason strings

        day_events = events.get(d.isoformat(), [])
        fomc_titles = [e["title"] for e in day_events if e["title"] in FOMC_TITLES]
        releases = [e["title"] for e in day_events if e["title"] not in FOMC_TITLES]
        is_fomc = d in FOMC_DECISION_DATES or bool(fomc_titles)
        is_release_day = bool(releases)

        # ── Summer mode (LOCKED plan): Jun–Aug trades red-folder
        #    windows only — the window opens on the week's first
        #    release and carries through Friday, gated each morning
        #    by the Pulse ──
        summer = d.month in SUMMER_MONTHS
        summer_out = False
        summer_carry = False
        summer_reason = ""
        if summer:
            week_release_days = []
            for offset in range(5):
                wd = monday_of + timedelta(days=offset)
                if not _is_trading_day(wd):
                    continue
                wtitles = [e["title"] for e in events.get(wd.isoformat(), [])
                           if e["title"] not in FOMC_TITLES]
                if wtitles or wd in FOMC_DECISION_DATES:
                    week_release_days.append(wd)
            first_release = week_release_days[0] if week_release_days else None
            if first_release is None:
                summer_out = True
                summer_reason = ("Summer plan — no red-folder release this "
                                 "week; the locked summer rule trades release "
                                 "windows only, so the whole week sits out")
            elif is_fomc:
                summer_out = True
                summer_reason = ("Summer plan — FOMC day; he never trades "
                                 "the FOMC morning, so the window opens the "
                                 "next day")
            elif d < first_release:
                summer_out = True
                summer_reason = ("Summer plan — no catalyst yet; the week's "
                                 "first release is "
                                 f"{first_release.strftime('%A')}, and the "
                                 "locked summer rule stays out until the "
                                 "window opens")
            elif not is_release_day:
                summer_carry = True

        prev_td = _prior_trading_day(d)
        next_td = _next_trading_day(d)

        # ── Event timing (strongest factor family) ──
        if is_fomc:
            scored.append((-2, "FOMC decision day — the morning is dead into a "
                               "2 PM announcement; his pattern is no FOMC morning, "
                               "the next day is the play"))
        elif is_release_day:
            names = ", ".join(sorted(set(releases)))
            scored.append((2, f"Red-folder release day — {names} at 8:30 AM ET; "
                              "the day's range is built around the print"))
        else:
            # Pre-FOMC: 1–2 trading days out, not itself a release day.
            if next_td in FOMC_DECISION_DATES or _next_trading_day(next_td) in FOMC_DECISION_DATES:
                scored.append((-2, "Pre-FOMC — positioning into the decision; "
                                   "volume runs about 24% below normal in the "
                                   "run-up, the weakest mornings of the cycle"))
            if prev_td in FOMC_DECISION_DATES:
                notes.append("Post-FOMC — participation returns, but reliable "
                             "follow-through doesn't; no automatic upgrade")

        # Day after a data release: the print is still being counted.
        prev_releases = [e["title"] for e in events.get(prev_td.isoformat(), [])
                         if e["title"] not in FOMC_TITLES]
        if prev_releases and not is_release_day and not is_fomc:
            names = ", ".join(sorted(set(prev_releases)))
            scored.append((1, f"Day after the {names} release — the print is "
                              "still being counted; the day after stays in play"))
        elif not is_release_day and not is_fomc:
            # Week inheritance: an earlier release this week is still the story.
            for offset in range(d.weekday()):
                earlier = monday_of + timedelta(days=offset)
                earlier_titles = [e["title"] for e
                                  in events.get(earlier.isoformat(), [])
                                  if e["title"] not in FOMC_TITLES]
                if earlier_titles:
                    names = ", ".join(sorted(set(earlier_titles)))
                    notes.append(f"Week context — still counting {names} from "
                                 f"{earlier.strftime('%A')}; the week inherits "
                                 "its state, it doesn't reset each morning")
                    break

        # ── Participation / session shape ──
        if d in BOND_CLOSED_THIN:
            scored.append((-2, f"Thin session — bond market closed "
                                f"({BOND_CLOSED_THIN[d]}); participation runs "
                                "structurally light while futures stay open"))
        if d in HALF_DAYS:
            scored.append((-1, "Half session — shortened day; volume runs "
                                "about half of a normal session"))

        red_days = self._week_red_days(monday_of, events)
        if quiet_week_known and red_days <= 1 and not is_release_day and not is_fomc and not summer:
            scored.append((-1, "Quiet week — one or zero red-folder days all "
                                "week; a weak calendar is positive evidence "
                                "of quiet, not just a missing boost"))

        if summer_carry:
            scored.append((1, "Summer carry — the week's release window is "
                              "open and carries through Friday while the Pulse "
                              "gives the okay; a catalyst week doesn't close "
                              "after two days"))

        # ── Range facts (checkpoint inputs; only for days whose prior ──
        #    session is already a recorded fact) ──
        # frozen_past (a past day with no frozen record): the facts
        # describe TODAY's prior session, never that day's — this block
        # is skipped entirely so a past day can't inherit today's
        # numbers. (The today-only block below is gated on d == today,
        # so it is likewise unreachable for a past day.)
        prior_is_fact = prev_td < today or (
            prev_td == today and datetime.now(ET).time() >= time(16, 0))
        if (not frozen_past and prior_is_fact
                and facts.get("prev_rank") is not None
                and d <= _next_trading_day(today)):
            rank = facts["prev_rank"]
            rank2 = facts.get("prev2_rank")
            if rank >= 97:
                if is_release_day or is_fomc:
                    notes.append(f"The prior session's range was {rank:.0f}th "
                                 "percentile — expect two-sided trade around "
                                 "the catalyst")
                else:
                    scored.append((-2, f"Range exhaustion — the prior session "
                                        f"printed a {rank:.0f}th-percentile range; "
                                        "days after huge ranges run wide but "
                                        "inefficient and reverse about two times "
                                        "in three"))
            elif rank >= 95:
                if is_release_day or is_fomc:
                    notes.append(f"The prior session's range was {rank:.0f}th "
                                 "percentile — wide day behind, catalyst ahead")
                else:
                    scored.append((-1, f"Range exhaustion — the prior session "
                                        f"printed a {rank:.0f}th-percentile range; "
                                        "the morning after a huge day is usually "
                                        "poor"))
            if (rank >= 90 and rank2 is not None and rank2 >= 90
                    and not is_release_day and not is_fomc):
                scored.append((-1, "Two extended-range days in a row — "
                                    "exhaustion stacks"))
            pos = facts.get("prev_close_pos")
            if pos is not None and not any(p <= -1 for p, _t in scored):
                if 0.35 <= pos <= 0.65:
                    notes.append("The prior session closed mid-range — balance "
                                 "was accepted; rotation is the default read")
                elif pos >= 0.85 or pos <= 0.15:
                    notes.append("The prior session closed at its range extreme "
                                 "— initiative; follow-through odds tick up")

        # ── Today-only checkpoint facts ──
        travel_lock = False
        if d == today:
            travel = facts.get("travel_pct")
            if travel is not None and travel >= TRAVEL_LOCK_PCT:
                travel_lock = True
            elif travel is not None:
                story = _travel_story(facts)
                scored.append((0, f"Overnight travel {travel:.1f}% — under "
                                  f"the locked {TRAVEL_LOCK_PCT:.0f}% travel "
                                  f"line, so no travel lock."
                                  + (f" {story}" if story else "")))
            gap = facts.get("weekend_gap_pct")
            if d.weekday() == 0 and gap is not None and abs(gap) >= 0.5:
                scored.append((-1, f"Weekend gap of {gap:+.1f}% — Monday gaps "
                                    "carry a fill tendency; the gap is the "
                                    "morning's first story"))
            if pulse_conf is not None:
                compressed = (facts.get("median10_rank") is not None
                              and facts["median10_rank"] <= 25)
                if pulse_conf >= 55:
                    scored.append((1, "The Pulse has crossed its 55% gate at "
                                      "this checkpoint — gate crossings count "
                                      "as recorded facts for the plan"))
                elif compressed:
                    scored.append((-1, "Low-vol stack — recent ranges are "
                                        "compressed (10-session median at or "
                                        "below the 25th percentile) with the "
                                        "Pulse under its gate; intraday "
                                        "follow-through is weakest here"))
            if summer and not summer_out and pulse_conf is not None \
                    and pulse_conf < 55:
                summer_out = True
                summer_reason = ("Summer plan — the Pulse reads Neutral this "
                                 "morning (confidence under the 55% gate); "
                                 "the locked summer rule ignores the day "
                                 "entirely")

        # ── Markers (noted, never scored) ──
        marker_fired = False
        expiry = _third_friday(d.year, d.month) if d.month in (3, 6, 9, 12) else None
        if expiry is not None and _roll_week_monday(d) is not None:
            if d == expiry:
                notes.append("Expiry Friday — the quarterly futures roll "
                             "completes today; churn here is plumbing, not signal")
            else:
                notes.append("Futures roll week — volume is migrating to the "
                             "next quarter's contract; a modest effect in "
                             "NQ/ES, thinner in the outgoing contract")

        for m in markers:
            try:
                mdate = date.fromisoformat(m.get("date", ""))
            except ValueError:
                continue
            session = m.get("session", "after_close")
            applies = (mdate == d and session == "before_open") or (
                mdate == prev_td and session == "after_close")
            if applies:
                marker_fired = True
                when = "before the open" if session == "before_open" else "last night"
                notes.append(f"Earnings marker — {m.get('symbol', '')} "
                             f"({m.get('name', '')}) reported {when}; a top-weight "
                             "Nasdaq-100 name, marked like an election marker — "
                             "never scored, its beat or miss never predicted")

        # ── Tier ──
        points = sum(p for p, _t in scored)
        if points >= 2:
            tier = TIER_PROBABLE
        elif points >= 0:
            tier = TIER_MID
        elif points >= -2:
            tier = TIER_DEFENSIVE
        else:
            tier = TIER_DONT_LOOK

        hard_factors = travel_lock or is_fomc or summer_out or d in BOND_CLOSED_THIN or any(
            "Pre-FOMC" in t for _p, t in scored)
        if marker_fired and tier == TIER_DONT_LOOK and not hard_factors:
            tier = TIER_DEFENSIVE
            notes.append("Earnings exemption — a marker night can't be rated "
                         "DON'T LOOK on quietness alone; the overnight "
                         "checkpoint (travel, gap) does the real rating work")

        reasons = [t for _p, t in scored] + notes
        if travel_lock:
            tier = TIER_DONT_LOOK
            story = _travel_story(facts)
            reasons.insert(0, f"LOCKED RULE — overnight travel "
                              f"{facts['travel_pct']:.1f}% crossed the locked "
                              f"{TRAVEL_LOCK_PCT:.0f}% travel line (full "
                              f"overnight range, round trips included)."
                              + (f" {story}" if story else "")
                              + " Extended overnight travel makes this a "
                              "no-trade morning, whatever else the week holds")
        if summer_out:
            tier = TIER_DONT_LOOK
            reasons.insert(0, summer_reason)
        if not reasons:
            reasons = ["No catalyst and no positive evidence of quiet — a "
                       "baseline day; the morning itself decides"]
        return {
            "date": d.isoformat(),
            "weekday": d.strftime("%A"),
            "rating": tier,
            "reasons": reasons,
            "is_today": d == today,
        }


probability_calendar = ProbabilityCalendar()
