import json
import os
import schedule
import time
import threading
from datetime import datetime
import concurrent.futures
import pytz

from config import TIMEZONE, REFRESH_INTERVAL_MINUTES
from utils.logger import pulse_logger


def wait_for_persistent_volume(max_wait_seconds: int = 120):
    data_dir = "/data"
    test_file = os.path.join(data_dir, ".volume_ready_test")
    print(f"🔄 Waiting up to {max_wait_seconds}s for persistent volume at {data_dir}...")
    for i in range(max_wait_seconds):
        try:
            if os.path.isdir(data_dir) and os.access(data_dir, os.W_OK):
                with open(test_file, "w") as f:
                    f.write("ok")
                with open(test_file, "r") as f:
                    if f.read() == "ok":
                        os.remove(test_file)
                        print(f"✅ Persistent volume /data is ready and writable (took {i+1}s)")
                        return True
        except Exception:
            pass
        if (i + 1) % 15 == 0:
            print(f"   Still waiting for /data volume... ({i+1}s elapsed)")
        time.sleep(1)
    print(f"⚠️ WARNING: /data volume not writable after {max_wait_seconds}s — proceeding anyway")
    return False


wait_for_persistent_volume(max_wait_seconds=120)

from pipelines.macro_sentiment import macro_sentiment_pipeline
from pipelines.economic_calendar import economic_calendar_pipeline
from pipelines.institutional import institutional_pipeline
from pipelines.geopolitical import geopolitical_pipeline
from pipelines.weekly_summary import weekly_summary_pipeline
from pipelines.ai_lens import ai_lens_pipeline
from pipelines.manual_input import manual_input_pipeline
from pipelines.recommendation import recommendation_engine, prop_firm_engine

from processors.data_formatter import data_formatter
from processors.bias_calculator import bias_calculator
from processors.snapshot_generator import snapshot_generator

from utils.error_handler import error_handler
from utils.cache import cache

from ui.dashboard import app as dashboard_app

def run_pulse():
    pulse_logger.log("🔄 Running The Pulse refresh...")
    try:
        macro_data = macro_sentiment_pipeline.fetch()
    except Exception as e:
        pulse_logger.log(f"⚠️ Macro failed: {e}", level="WARNING")
        macro_data = {}

    try:
        economic_calendar_pipeline.maybe_reset_weekly_blocklist()
        econ_data = economic_calendar_pipeline.fetch()
    except Exception as e:
        pulse_logger.log(f"⚠️ Economic failed: {e}", level="WARNING")
        econ_data = {}

    try:
        inst_data = institutional_pipeline.fetch()
    except Exception as e:
        pulse_logger.log(f"⚠️ Institutional failed: {e}", level="WARNING")
        inst_data = {}

    try:
        geopolitical_pipeline.maybe_reset_geo_blocklist()
    except Exception as e:
        pulse_logger.log(f"⚠️ Geo blocklist reset check failed: {e}", level="WARNING")

    # Geopolitical with 45s thread timeout — parallel fetching + Haiku needs time
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(geopolitical_pipeline.fetch)
            geo_data = future.result(timeout=45)
    except concurrent.futures.TimeoutError:
        pulse_logger.log("⚠️ Geopolitical timed out after 45s — using cache", level="WARNING")
        cached = cache.load("geopolitical")
        geo_data = cached['data'] if cached else {}
    except Exception as e:
        pulse_logger.log(f"⚠️ Geopolitical failed: {e}", level="WARNING")
        cached = cache.load("geopolitical")
        geo_data = cached['data'] if cached else {}

    try:
        formatted_data = data_formatter.standardize({
            'macro': macro_data,
            'economic': econ_data,
            'institutional': inst_data,
            'geopolitical': geo_data
        })

        try:
            with open('/data/size_mode.json', 'r') as f:
                size_mode = json.load(f).get('mode', 'quarter')
        except Exception:
            size_mode = 'quarter'

        # Derive bias_threshold from EC pipeline's weak_ec_week flag — same canonical source
        # as _get_weekly_threshold(), eliminates stale cache read before compute_prop_firm()
        _ec_weak = formatted_data.get('economic', {}).get('weak_ec_week')
        if _ec_weak is True:
            bias_threshold = 0.30
        elif _ec_weak is False:
            bias_threshold = 0.33
        else:
            bias_threshold = 0.50

        bias_score = bias_calculator.compute(formatted_data, size_mode=size_mode, bias_threshold=bias_threshold)

        recommendation = recommendation_engine.compute(
            bias_score,
            formatted_data.get('geopolitical', {}),
            formatted_data.get('macro', {}),
        )
        bias_score['recommendation'] = recommendation

        prop_recommendation = prop_firm_engine.compute_prop_firm(
            bias_score,
            formatted_data.get('geopolitical', {}),
            formatted_data.get('macro', {}),
            formatted_data.get('economic', {}),
        )
        bias_score['recommendation_prop'] = prop_recommendation

        # Quiet/standard week log — derived from compute_prop_firm() result, not cache file
        if isinstance(prop_recommendation, dict):
            _is_quiet = prop_recommendation.get('quiet_week', False)
            _ec_weight = prop_recommendation.get('ec_weight', 30)
            _bias_thr = prop_recommendation.get('bias_threshold', bias_threshold)
            _red_days = formatted_data.get('economic', {}).get('red_folder_days', 0)
            _day_s = 'day' if _red_days == 1 else 'days'
            if _is_quiet:
                pulse_logger.log(f"🔇 Quiet week active — {_red_days} red folder {_day_s} — EC {_ec_weight}%, bias ±{_bias_thr}")
            else:
                pulse_logger.log(f"📅 Standard week — {_red_days} red folder {_day_s} — EC {_ec_weight}%, bias ±{_bias_thr}")

        weekly_summary_pipeline.fetch(formatted_data=formatted_data, bias=bias_score)
        snapshot_id = snapshot_generator.save(bias_score, formatted_data)
        pulse_logger.log(f"✅ Pulse updated | {bias_score['bias_emoji']} {bias_score['bias']} | Confidence: {bias_score['confidence']}% | Snapshot: {snapshot_id}")

        now_est = datetime.now(pytz.timezone(TIMEZONE))

        # AI Lens — generate once daily after 8:30 AM EST
        if now_est.hour > 8 or (now_est.hour == 8 and now_est.minute >= 30):
            try:
                ai_lens_pipeline.generate(bias_score, formatted_data)
            except Exception as e:
                pulse_logger.log(f"⚠️ AI Lens failed: {e}", level="WARNING")

        if now_est.hour >= 16 and not snapshot_generator.has_daily_for_today():
            snapshot_generator.save_daily(bias_score, formatted_data)
            pulse_logger.log("📅 Daily closing snapshot saved")
    except Exception as e:
        error_handler.handle(e, "Main Orchestrator")

def run_scheduler():
    schedule.every(REFRESH_INTERVAL_MINUTES).minutes.do(run_pulse)
    schedule.every(24).hours.do(manual_input_pipeline.clear_old_inputs)
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            pulse_logger.log(f"⚠️ Scheduler job failed — continuing: {e}", level="WARNING")
        time.sleep(1)

if __name__ == '__main__':
    pulse_logger.log("🚀 The Pulse is starting...")

    port = int(os.environ.get('PORT', 8080))

    # Start first pulse refresh in background so Flask starts immediately
    first_run_thread = threading.Thread(target=run_pulse, daemon=True)
    first_run_thread.start()

    # Start scheduler in background
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    pulse_logger.log(f"🌐 Dashboard running on http://0.0.0.0:{port}")
    dashboard_app.run(host='0.0.0.0', port=port, debug=False)
