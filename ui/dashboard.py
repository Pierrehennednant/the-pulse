from flask import Flask, render_template, jsonify
from processors.snapshot_generator import snapshot_generator

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
