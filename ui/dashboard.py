import json
from flask import Flask, render_template, jsonify, request
from processors.snapshot_generator import snapshot_generator
from pipelines.manual_input import manual_input_pipeline

app = Flask(__name__, template_folder='templates')

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
        if not event_title or not actual_value:
            return jsonify({'error': 'Missing event_title or actual_value'}), 400

        success = manual_input_pipeline.save_actual(event_title, actual_value, story_url)

        if success:
            from pipelines.economic_calendar import economic_calendar_pipeline
            from processors.data_formatter import data_formatter
            from processors.bias_calculator import bias_calculator
            from processors.snapshot_generator import snapshot_generator
            from utils.cache import cache

            econ_data = economic_calendar_pipeline.fetch()

            macro_cached = cache.load('macro_sentiment')
            inst_cached = cache.load('institutional')
            geo_cached = cache.load('geopolitical')
            news_cached = cache.load('news_sentiment')

            formatted_data = data_formatter.standardize({
                'macro': macro_cached['data'] if macro_cached else None,
                'economic': econ_data,
                'institutional': inst_cached['data'] if inst_cached else None,
                'geopolitical': geo_cached['data'] if geo_cached else None,
                'news': news_cached['data'] if news_cached else None
            })

            bias_score = bias_calculator.compute(formatted_data)
            snapshot_generator.save(bias_score, formatted_data)

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
        if not event_title:
            return jsonify({'error': 'Missing event_title'}), 400
        with open('/data/permanent_manual_inputs.json', 'r') as f:
            inputs = json.load(f)
        if event_title in inputs:
            del inputs[event_title]
            with open('/data/permanent_manual_inputs.json', 'w') as f:
                json.dump(inputs, f, indent=2)
        return jsonify({'status': 'reset', 'event': event_title})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
