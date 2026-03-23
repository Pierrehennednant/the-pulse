import schedule
import time
import threading
from datetime import datetime
import pytz

from config import TIMEZONE, REFRESH_INTERVAL_MINUTES

from pipelines.macro_sentiment import macro_sentiment_pipeline
from pipelines.economic_calendar import economic_calendar_pipeline
from pipelines.institutional import institutional_pipeline
from pipelines.geopolitical import geopolitical_pipeline
from pipelines.news_sentiment import news_sentiment_pipeline

from processors.data_formatter import data_formatter
from processors.bias_calculator import bias_calculator
from processors.snapshot_generator import snapshot_generator

from utils.logger import pulse_logger
from utils.error_handler import error_handler

from ui.dashboard import app as dashboard_app

def run_pulse():
    pulse_logger.log("🔄 Running The Pulse refresh...")
    try:
        macro_data = macro_sentiment_pipeline.fetch()
        econ_data = economic_calendar_pipeline.fetch()
        inst_data = institutional_pipeline.fetch()
        geo_data = geopolitical_pipeline.fetch()
        news_data = news_sentiment_pipeline.fetch(geo_data=geo_data)

        formatted_data = data_formatter.standardize({
            'macro': macro_data,
            'economic': econ_data,
            'institutional': inst_data,
            'geopolitical': geo_data,
            'news': news_data
        })

        bias_score = bias_calculator.compute(formatted_data)
        snapshot_id = snapshot_generator.save(bias_score, formatted_data)

        pulse_logger.log(f"✅ Pulse updated | {bias_score['bias_emoji']} {bias_score['bias']} | Confidence: {bias_score['confidence']}% | Snapshot: {snapshot_id}")

    except Exception as e:
        error_handler.handle(e, "Main Orchestrator")

def run_scheduler():
    schedule.every(REFRESH_INTERVAL_MINUTES).minutes.do(run_pulse)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    pulse_logger.log("🚀 The Pulse is starting...")
    run_pulse()
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    pulse_logger.log("🌐 Dashboard running on http://0.0.0.0:8080")
    dashboard_app.run(host='0.0.0.0', port=5000, debug=False)
