import json
from flask import Flask, render_template, jsonify, request
from utils.file_lock import atomic_write_json
from processors.snapshot_generator import snapshot_generator
from pipelines.manual_input import manual_input_pipeline
from utils.logger import pulse_logger

app = Flask(__name__, template_folder='templates')

_MAX_TITLE_LEN = 200
_MAX_VALUE_LEN = 50

def _validate_manual_input(event_title, actual_value):
    """Return (ok, error_message). Checks type, length, and no null bytes."""
    if not isinstance(event_title, str) or not isinstance(actual_value, str):
        return False, 'event_title and actual_value must be strings'
    if not event_title or not actual_value:
        return False, 'Missing event_title or actual_value'
    if len(event_title) > _MAX_TITLE_LEN:
        return False, f'event_title exceeds {_MAX_TITLE_LEN} characters'
    if len(actual_value) > _MAX_VALUE_LEN:
        return False, f'actual_value exceeds {_MAX_VALUE_LEN} characters'
    if '\x00' in event_title or '\x00' in actual_value:
        return False, 'Null bytes not allowed'
    return True, None

def _run_partial_refresh(label):
    """Fresh econ fetch + cached macro/inst/geo → full bias recompute."""
    from pipelines.economic_calendar import economic_calendar_pipeline
    from pipelines.recommendation import recommendation_engine
    from processors.data_formatter import data_formatter
    from processors.bias_calculator import bias_calculator
    from utils.cache import cache

    cache.delete('economic_calendar')
    econ_data = economic_calendar_pipeline.fetch()

    macro_cached = cache.load('macro_sentiment')
    macro_data = macro_cached['data'] if macro_cached else {}

    inst_cached = cache.load('institutional')
    inst_data = inst_cached['data'] if inst_cached else {}

    geo_cached = cache.load('geopolitical')
    geo_data = geo_cached['data'] if geo_cached else {}

    formatted_data = data_formatter.standardize({
        'macro': macro_data,
        'economic': econ_data,
        'institutional': inst_data,
        'geopolitical': geo_data,
    })

    try:
        with open('/data/size_mode.json', 'r') as f:
            size_mode = json.load(f).get('mode', 'quarter')
    except Exception:
        size_mode = 'quarter'

    bias_score = bias_calculator.compute(formatted_data, size_mode=size_mode)

    recommendation = recommendation_engine.compute(
        bias_score,
        formatted_data.get('geopolitical', {}),
        formatted_data.get('macro', {}),
    )
    bias_score['recommendation'] = recommendation

    snapshot_generator.save(bias_score, formatted_data)
    pulse_logger.log(f"✅ {label} partial refresh complete")

@app.route('/')
def home():
    latest = snapshot_generator.get_latest()
    return render_template('dashboard.html', snapshot=latest)

@app.route('/api/latest')
def api_latest():
    latest = snapshot_generator.get_latest()
    return jsonify(latest)

@app.route('/snapshot/<snapshot_id>')
def view_snapshot(snapshot_id):
    snapshot = snapshot_generator.load(snapshot_id)
    if not snapshot:
        return "Snapshot not found", 404
    return render_template('dashboard.html', snapshot=snapshot)

@app.route('/api/snapshot/<snapshot_id>')
def api_snapshot(snapshot_id):
    snapshot = snapshot_generator.load(snapshot_id)
    if not snapshot:
        return jsonify({'error': 'Snapshot not found'}), 404
    return jsonify(snapshot)

@app.route('/api/manual_input', methods=['POST'])
def manual_input():
    try:
        data = request.get_json()
        event_title = data.get('event_title')
        actual_value = data.get('actual_value')
        story_url = data.get('story_url', None)

        ok, err = _validate_manual_input(event_title, actual_value)
        if not ok:
            return jsonify({'error': err}), 400

        success = manual_input_pipeline.save_actual(event_title, actual_value, story_url)

        if success:
            try:
                _run_partial_refresh(f"manual_input | {event_title}")
            except Exception as refresh_err:
                pulse_logger.log(f"⚠️ manual_input partial refresh failed: {refresh_err}", level="WARNING")

            return jsonify({'status': 'saved', 'event': event_title, 'actual': actual_value})

        return jsonify({'error': 'Failed to save'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/manual_inputs')
def get_manual_inputs():
    inputs = manual_input_pipeline.get_inputs()
    return jsonify(inputs)

@app.route('/api/reset_manual_input', methods=['POST'])
def reset_manual_input():
    try:
        data = request.get_json()
        event_title = data.get('event_title')
        if not isinstance(event_title, str) or not event_title:
            return jsonify({'error': 'Missing event_title'}), 400
        if len(event_title) > _MAX_TITLE_LEN or '\x00' in event_title:
            return jsonify({'error': 'Invalid event_title'}), 400

        with open('/data/permanent_manual_inputs.json', 'r') as f:
            inputs = json.load(f)
        if event_title in inputs:
            del inputs[event_title]
            atomic_write_json('/data/permanent_manual_inputs.json', inputs)

        try:
            _run_partial_refresh(f"reset_manual_input | {event_title}")
        except Exception as refresh_err:
            pulse_logger.log(f"⚠️ reset_manual_input partial refresh failed: {refresh_err}", level="WARNING")

        return jsonify({'status': 'reset', 'event': event_title})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/size_mode', methods=['POST'])
def set_size_mode():
    data = request.get_json()
    mode = data.get('mode', 'quarter')
    if mode not in ['quarter', 'normal']:
        return jsonify({'status': 'error', 'message': 'Invalid mode'}), 400
    size_mode_file = '/data/size_mode.json'
    try:
        atomic_write_json(size_mode_file, {'mode': mode})
        return jsonify({'status': 'saved', 'mode': mode})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
