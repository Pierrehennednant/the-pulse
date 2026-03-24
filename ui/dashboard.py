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
            return jsonify({'status': 'saved', 'event': event_title, 'actual': actual_value})
        return jsonify({'error': 'Failed to save'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/manual_inputs')
def get_manual_inputs():
    inputs = manual_input_pipeline.get_inputs()
    return jsonify(inputs)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
