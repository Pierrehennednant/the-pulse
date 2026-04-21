import json
import os
import schedule
import time
import threading
from datetime import datetime
import concurrent.futures

from config import TIMEZONE, REFRESH_INTERVAL_MINUTES

from pipelines.macro_sentiment import macro_sentiment_pipeline
from pipelines.economic_calendar import economic_calendar_pipeline
from pipelines.institutional import institutional_pipeline
from pipelines.geopolitical import geopolitical_pipeline
from pipelines.weekly_summary import weekly_summary_pipeline
from pipelines.manual_input import manual_input_pipeline
from pipelines.recommendation import recommendation_engine

from processors.data_formatter import data_formatter
from processors.bias_calculator import bias_calculator
from processors.snapshot_generator import snapshot_generator

from utils.logger import pulse_logger
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
        econ_data = economic_calendar_pipeline.fetch()
    except Exception as e:
        pulse_logger.log(f"⚠️ Economic failed: {e}", level="WARNING")
        econ_data = {}

    try:
        inst_data = institutional_pipeline.fetch()
    except Exception as e:
        pulse_logger.log(f"⚠️ Institutional failed: {e}", level="WARNING")
        inst_data = {}

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
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to load size_mode.json, defaulting to quarter: {e}", level="WARNING")
            size_mode = 'quarter'

        bias_score = bias_calculator.compute(formatted_data, size_mode=size_mode)

        recommendation = recommendation_engine.compute(
            bias_score,
            formatted_data.get('geopolitical', {}),
            formatted_data.get('macro', {})
        )
        bias_score['recommendation'] = recommendation

        weekly_summary_pipeline.fetch(formatted_data=formatted_data, bias=bias_score)
        snapshot_id = snapshot_generator.save(bias_score, formatted_data)
        pulse_logger.log(f"✅ Pulse updated | {bias_score['bias_emoji']} {bias_score['bias']} | Confidence: {bias_score['confidence']}% | Snapshot: {snapshot_id}")
    except Exception as e:
        error_handler.handle(e, "Main Orchestrator")

def run_scheduler():
    schedule.every(REFRESH_INTERVAL_MINUTES).minutes.do(run_pulse)
    schedule.every(24).hours.do(manual_input_pipeline.clear_old_inputs)
    while True:
        schedule.run_pending()
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
