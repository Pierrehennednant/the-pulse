import json
import os
from datetime import datetime
import pytz
from config import TIMEZONE
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger
from pipelines.economic_calendar import economic_calendar_pipeline

PROP_FIRM_THRESHOLD_FILE = '/data/prop_firm_weekly_threshold.json'

class PropFirmRecommendationEngine:
    """Prop Firm recommendation — the app's one recommendation engine.
    Formerly a subclass alongside a separate "Live mode" engine
    (RecommendationEngine, 55/65 confidence bands); that engine's output
    was never actually displayed (confirmed via repo-wide search before
    removal — its only reader was a UI toggle that has itself been
    removed) and has been deleted rather than left as dead code.

      Bias threshold         ±0.30 quiet week (≤1 red folder day) / ±0.33 standard week (≥2)
      Show-card confidence     55%
      Quarter-entry confidence 55%–59%
      Normal-entry confidence  60%–69%
      Aggressive-entry confidence  ≥70%
      Pillar alignment         ≥45% of total week weight must agree with bias
                               Quiet week: EC 15%, total 85%, threshold ≥38.25%
                               Standard week: EC 30%, total 100%, threshold ≥45%

    Quiet week = 0 or 1 calendar days with at least one red folder event.
    A day with multiple red folder events counts as 1 red folder day.
    Threshold recomputed once per calendar day — on the first genuinely-
    live EC cycle of that day — then held for the rest of the day.
    EC's red-folder-day count changes from calendar edits, not live market
    movement, so same-day freshness is what matters, not same-cycle.
    Persisted to PROP_FIRM_THRESHOLD_FILE, keyed by date (with the ISO
    week also stored, to distinguish a daily refresh from an actual new
    week for the "new week detected" log). On a cycle where EC data isn't
    usable, the last computed day's value is held rather than recomputed
    from empty data. A distinct log line fires whenever the quiet/standard
    classification itself flips relative to the last persisted value.
    """

    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)

    _WEEK_WEIGHTS = {
        'standard': {'economic_calendar': 30, 'geopolitical': 25, 'institutional': 25, 'macro_sentiment': 20},
        'quiet':    {'economic_calendar': 15, 'geopolitical': 25, 'institutional': 25, 'macro_sentiment': 20},
    }

    def _get_weekly_threshold(self, econ_data):
        """Return week mode dict. Recomputes once per calendar day — on the
        first genuinely-live EC cycle of that day — then holds for the rest
        of the day. EC's red-folder-day count changes because of calendar
        edits (new/reclassified events), not live market movement, so
        same-day freshness is what's needed, not same-cycle (5-min) freshness.

        IMPORTANT: "holds for the rest of the day" means the CLASSIFICATION
        is held stable across cycles — it does NOT mean the persisted file is
        trusted blindly. Whenever genuinely-live EC data is available this
        cycle, red_folder_days is ALWAYS freshly recomputed (cheap, in-memory,
        same cost economic_calendar.py's own weak_ec_week check already pays
        every cycle) and compared against whatever is on disk for today. A
        same-day cache hit is only served as-is when it still matches the
        fresh count — otherwise the classification is recomputed and the file
        is corrected immediately, rather than being served stale for the rest
        of the day. This closes a real bug: a value computed and persisted
        earlier today (e.g. under pre-fix counting logic, or before a
        same-day calendar edit) would previously be served untouched until
        midnight even after the underlying code or data changed.

        Returns dict with keys:
          bias_threshold, red_folder_days, is_new_week, is_quiet_week,
          ec_weight, total_weight, alignment_threshold
        """
        now = datetime.now(self.timezone)
        iso = now.isocalendar()
        current_week = (iso[0], iso[1])
        today_str = now.strftime('%Y-%m-%d')

        prior_cached = None     # from a PREVIOUS calendar date — cross-day change-log baseline
        same_day_cached = None  # from TODAY — candidate to hold, but validated against fresh data below
        try:
            if os.path.exists(PROP_FIRM_THRESHOLD_FILE):
                with open(PROP_FIRM_THRESHOLD_FILE, 'r') as f:
                    on_disk = json.load(f)
                if on_disk.get('date') == today_str and 'is_quiet_week' in on_disk:
                    same_day_cached = on_disk
                else:
                    prior_cached = on_disk
        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm threshold cache read failed: {e}", level="WARNING")

        events = econ_data.get('events', []) if econ_data else []
        status = (econ_data or {}).get('status')
        # NOT `or not events` — a genuinely quiet week (0 red folder events)
        # produces status='live' with events=[] from a real, fully-resolved
        # fetch, and that must NOT defer forever (confirmed live bug: EC
        # correctly read 0 events/weak_ec_week=true every cycle, but this
        # check treated 0 events as "still loading" and deferred every single
        # cycle, so the threshold file never got past yesterday's stale
        # Standard/3-red-folder-days value). Traced status='live': it's set
        # in exactly one place, economic_calendar.py's fetch() (terminal line
        # of its success path, after events/red_folder_days/weak_ec_week are
        # all computed) — but a SECOND call site, ui/dashboard.py's
        # _run_partial_refresh(), fabricates the same status='live' string as
        # a cold-cache placeholder (no fetch has run yet) with events=[] too.
        # So the status string alone can't be trusted to mean "fetch
        # resolved" — 'red_folder_days' can: it's only ever stamped by
        # economic_calendar.py's real terminal success path, never by that
        # placeholder or by main.py's exception-fallback {}. Its presence,
        # not the event count, is what actually separates "confirmed empty"
        # from "not yet resolved."
        should_defer = status in ('unavailable', 'stale') or 'red_folder_days' not in (econ_data or {})

        if should_defer:
            pulse_logger.log(
                f"⏳ Prop Firm weekly threshold — EC data not yet live this cycle "
                f"(status={status!r}, {len(events)} events) — deferring today's recompute, will retry next cycle"
            )
            held = same_day_cached or prior_cached
            if held:
                # Hold the last computed value until a live cycle today
                # actually succeeds — don't flicker to a fresh empty-data read.
                # (No live events this cycle means no fresh count to validate
                # against, so the same-day value can't be re-checked right now
                # either — it'll be validated on the next live cycle.)
                return {
                    'bias_threshold': held['threshold'],
                    'red_folder_days': held['red_folder_days'],
                    'is_new_week': False,
                    'is_quiet_week': held['is_quiet_week'],
                    'ec_weight': held['ec_weight'],
                    'total_weight': held['total_weight'],
                    'alignment_threshold': held['alignment_threshold'],
                }
            # No prior value at all — compute honestly from whatever econ_data
            # has (as before) but don't persist it.
            red_folder_days = economic_calendar_pipeline._count_red_folder_days(events)
            is_quiet = red_folder_days <= 1
            threshold = 0.30 if is_quiet else 0.33
            ec_weight = 15 if is_quiet else 30
            total_weight = 85 if is_quiet else 100
            return {
                'bias_threshold': threshold,
                'red_folder_days': red_folder_days,
                'is_new_week': True,
                'is_quiet_week': is_quiet,
                'ec_weight': ec_weight,
                'total_weight': total_weight,
                'alignment_threshold': round(total_weight * 0.45, 2),
            }

        # Canonical count — shared with economic_calendar.py's weak_ec_week
        # determination so the two paths can't silently drift apart again.
        # ALWAYS live-recomputed this cycle (see docstring) — never blindly
        # trusts same_day_cached by date alone.
        red_folder_days = economic_calendar_pipeline._count_red_folder_days(events)
        is_quiet = red_folder_days <= 1

        if same_day_cached and same_day_cached['red_folder_days'] == red_folder_days:
            # Genuinely unchanged since this morning's computation — hold
            # exactly as designed, no rewrite, no new-week/change-log noise.
            return {
                'bias_threshold': same_day_cached['threshold'],
                'red_folder_days': same_day_cached['red_folder_days'],
                'is_new_week': False,
                'is_quiet_week': same_day_cached['is_quiet_week'],
                'ec_weight': same_day_cached['ec_weight'],
                'total_weight': same_day_cached['total_weight'],
                'alignment_threshold': same_day_cached['alignment_threshold'],
            }

        threshold = 0.30 if is_quiet else 0.33
        ec_weight = 15 if is_quiet else 30
        total_weight = 85 if is_quiet else 100
        alignment_threshold = round(total_weight * 0.45, 2)  # 38.25 (quiet) or 45.0 (standard)

        # Change-log baseline: prefer today's own (now-stale) cached value if
        # one exists, so a same-day correction is reported the same way a
        # cross-day change already is — otherwise fall back to the previous
        # day's value (the original cross-day behavior, unchanged).
        baseline = same_day_cached or prior_cached
        if baseline and baseline['is_quiet_week'] != is_quiet:
            old_label = 'Quiet' if baseline['is_quiet_week'] else 'Standard'
            new_label = 'Quiet' if is_quiet else 'Standard'
            direction = 'added' if red_folder_days > baseline['red_folder_days'] else 'removed'
            pulse_logger.log(
                f"⚠️ Week classification changed: {old_label} → {new_label} "
                f"(EC {baseline['ec_weight']}% → {ec_weight}%, bias ±{baseline['threshold']} → ±{threshold}) "
                f"— red folder day {direction} since last check "
                f"({baseline['red_folder_days']} → {red_folder_days})"
            )
        elif same_day_cached and same_day_cached['red_folder_days'] != red_folder_days:
            # Same classification, but the underlying count itself changed
            # mid-day (e.g. a code fix landed, or a calendar edit that didn't
            # cross the quiet/standard boundary) — logged for visibility so a
            # correction like this is never silent.
            pulse_logger.log(
                f"🔄 Red folder day count corrected mid-day: "
                f"{same_day_cached['red_folder_days']} → {red_folder_days} "
                f"(week classification unchanged: {'Quiet' if is_quiet else 'Standard'})"
            )

        is_new_week = prior_cached is None or prior_cached.get('week') != list(current_week)
        if same_day_cached:
            is_new_week = False  # a same-day correction is never a new week

        try:
            atomic_write_json(PROP_FIRM_THRESHOLD_FILE, {
                'week': list(current_week),
                'date': today_str,
                'threshold': threshold,
                'red_folder_days': red_folder_days,
                'is_quiet_week': is_quiet,
                'ec_weight': ec_weight,
                'total_weight': total_weight,
                'alignment_threshold': alignment_threshold,
                'set_at': now.isoformat(),
            })
        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm threshold cache write failed: {e}", level="WARNING")

        return {
            'bias_threshold': threshold,
            'red_folder_days': red_folder_days,
            'is_new_week': is_new_week,
            'is_quiet_week': is_quiet,
            'ec_weight': ec_weight,
            'total_weight': total_weight,
            'alignment_threshold': alignment_threshold,
        }

    def _no_rec(self, week_info):
        """No-recommendation sentinel — carries quiet week metadata for dashboard display."""
        return {
            'label': None,
            'quiet_week': week_info['is_quiet_week'],
            'ec_weight': week_info['ec_weight'],
            'bias_threshold': week_info['bias_threshold'],
        }

    def _rec(self, week_info, **kwargs):
        """Build a recommendation dict with quiet week metadata attached."""
        return {
            'quiet_week': week_info['is_quiet_week'],
            'ec_weight': week_info['ec_weight'],
            'bias_threshold': week_info['bias_threshold'],
            **kwargs,
        }

    def compute_prop_firm(self, bias_data, geo_data, macro_data, econ_data=None):
        try:
            week_info = self._get_weekly_threshold(econ_data)
            is_quiet = week_info['is_quiet_week']
            bias_threshold = week_info['bias_threshold']
            ec_weight = week_info['ec_weight']
            alignment_threshold = week_info['alignment_threshold']
            red_folder_days = week_info['red_folder_days']

            if week_info['is_new_week']:
                mode_label = 'quiet' if is_quiet else 'standard'
                day_s = 'day' if red_folder_days == 1 else 'days'
                pulse_logger.log(
                    f"📊 Prop Firm — new week detected: {mode_label} "
                    f"({red_folder_days} red folder {day_s})"
                )

            day_s = 'day' if red_folder_days == 1 else 'days'
            if is_quiet:
                pulse_logger.log(f"🔇 Quiet week active — {red_folder_days} red folder {day_s} — EC {ec_weight}%, bias ±{bias_threshold}")
            else:
                pulse_logger.log(f"📅 Standard week — {red_folder_days} red folder {day_s} — EC {ec_weight}%, bias ±{bias_threshold}")

            final_score = (bias_data.get('final_score', 0) or 0) if bias_data else 0
            if final_score >= bias_threshold:
                bias = 'Bullish'
            elif final_score <= -bias_threshold:
                bias = 'Bearish'
            else:
                return self._no_rec(week_info)

            pillar_weights = self._WEEK_WEIGHTS['quiet' if is_quiet else 'standard']
            pillar_contributions = (bias_data.get('pillar_contributions', {}) or {}) if bias_data else {}
            aligned_weight = sum(
                pillar_weights.get(p, 0)
                for p, c in pillar_contributions.items()
                if (bias == 'Bullish' and c.get('raw_score', 0) > 0.15)
                or (bias == 'Bearish' and c.get('raw_score', 0) < -0.15)
            )
            if aligned_weight < alignment_threshold:
                return self._no_rec(week_info)

            confidence = bias_data.get('confidence', 0) if bias_data else 0
            if confidence < 55:
                return self._no_rec(week_info)

            total_w = week_info['total_weight']
            if confidence >= 70:
                return self._rec(week_info,
                    mode='normal',
                    label=f'Prop Firm — {bias}, Normal entry',
                    reason=f'{aligned_weight}% of {total_w}% weight aligned · Confidence {confidence}%',
                    strength='strong',
                    bias=bias,
                )
            return self._rec(week_info,
                mode='quarter',
                label=f'Prop Firm — {bias}, Quarter entry',
                reason=f'Confidence {confidence}% — building toward Normal',
                strength='moderate',
                bias=bias,
            )

        except Exception as e:
            pulse_logger.log(f"⚠️ Prop Firm recommendation engine failed: {e}", level="WARNING")
            return None


prop_firm_engine = PropFirmRecommendationEngine()
