"""Shared recompute path for the Pulse.

compute_and_save() turns already-standardized pillar data into a bias
score, prop-firm recommendation, and saved snapshot. It is the single
code path used by both the scheduled run_pulse() (main.py) and the
on-demand refresh after a manual Yahoo pull — so the two can never
drift apart.
"""

from config import QUIET_WEEK_EC_WEIGHT
from utils.logger import pulse_logger
from pipelines.macro_sentiment import macro_sentiment_pipeline
from pipelines.weekly_summary import weekly_summary_pipeline
from pipelines.recommendation import prop_firm_engine
from processors.data_formatter import data_formatter
from processors.bias_calculator import bias_calculator
from processors.snapshot_generator import snapshot_generator


def compute_and_save(formatted_data):
    """Bias + prop-firm + weekly summary + snapshot from standardized
    pillar data. Returns (snapshot_id, bias_score)."""
    # Derive bias_threshold from EC pipeline's weak_ec_week flag — same canonical source
    # as _get_weekly_threshold(), eliminates stale cache read before compute_prop_firm()
    _ec_weak = formatted_data.get('economic', {}).get('weak_ec_week')
    if _ec_weak is True:
        bias_threshold = 0.30
        ec_weight = QUIET_WEEK_EC_WEIGHT
    elif _ec_weak is False:
        bias_threshold = 0.33
        ec_weight = None  # standard week: config EC weight (30) stands
    else:
        bias_threshold = 0.50
        ec_weight = None

    bias_score = bias_calculator.compute(formatted_data, bias_threshold=bias_threshold, ec_weight=ec_weight)

    prop_recommendation = prop_firm_engine.compute_prop_firm(
        bias_score,
        formatted_data.get('geopolitical', {}),
        formatted_data.get('macro', {}),
        formatted_data.get('economic', {}),
    )
    bias_score['recommendation_prop'] = prop_recommendation

    weekly_summary_pipeline.fetch(formatted_data=formatted_data, bias=bias_score)
    snapshot_id = snapshot_generator.save(bias_score, formatted_data)
    pulse_logger.log(
        f"✅ Pulse updated | {bias_score['bias_emoji']} {bias_score['bias']} | "
        f"Confidence: {bias_score['confidence']}% | Snapshot: {snapshot_id}"
    )
    return snapshot_id, bias_score


def refresh_macro_now():
    """On-demand refresh after a manual Yahoo pull: re-reads the macro
    pillar (fresh VIX/VXN signals are computed at read time), recomputes
    bias against the other pillars' latest snapshot data, and saves a new
    snapshot — so the dashboard reflects the pull immediately instead of
    waiting for the next 5-minute run_pulse(). The scheduled slots and the
    rest of run_pulse() are untouched.

    Returns (snapshot_id, bias_score), or (None, None) when there is no
    prior snapshot to build on or the macro fetch fails."""
    latest = snapshot_generator.get_latest()
    if not latest or not latest.get('pillars'):
        pulse_logger.log("⚠️ Manual refresh skipped — no prior snapshot", level="WARNING")
        return None, None
    try:
        macro_data = macro_sentiment_pipeline.fetch()
    except Exception as e:
        pulse_logger.log(f"⚠️ Manual refresh: macro fetch failed: {e}", level="WARNING")
        return None, None
    formatted_data = dict(latest['pillars'])
    formatted_data['macro'] = data_formatter.standardize({'macro': macro_data})['macro']
    return compute_and_save(formatted_data)
