import hmac
import json
import os
from datetime import datetime, timedelta
from functools import wraps

import pytz
from flask import (Flask, jsonify, redirect, render_template, request,
                   session, url_for)

from config import DASHBOARD_PASSWORD, SECRET_KEY, TIMEZONE
from pipelines.manual_input import manual_input_pipeline
from processors.snapshot_generator import snapshot_generator
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger

app = Flask(__name__, template_folder='templates')
# SECRET_KEY must be set in Railway env vars. Without it sessions reset on every restart.
app.secret_key = SECRET_KEY or os.urandom(24)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)

_MAX_TITLE_LEN = 200
_MAX_VALUE_LEN = 50


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def _is_api_request():
    return request.path.startswith('/api/')

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        if not session.get('authenticated'):
            if _is_api_request():
                return jsonify({'error': 'Unauthorized'}), 401
            return redirect(url_for('login', next=request.path))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Login / logout
# ---------------------------------------------------------------------------

@app.route('/login', methods=['GET', 'POST'])
def login():
    if not DASHBOARD_PASSWORD:
        return redirect(url_for('home'))
    error = None
    if request.method == 'POST':
        submitted = request.form.get('password', '')
        if hmac.compare_digest(submitted, DASHBOARD_PASSWORD):
            session.permanent = True
            session['authenticated'] = True
            next_url = request.args.get('next', '/')
            if not next_url.startswith('/'):
                next_url = '/'
            return redirect(next_url)
        error = 'Incorrect password.'
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

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
    """Recompute bias from cached pillars and save a fresh snapshot.
    Always reads EC data from cache — callers must update the cache before
    calling if their action changed EC event data. No outbound HTTP calls.
    """
    from pipelines.institutional import institutional_pipeline
    from pipelines.recommendation import recommendation_engine
    from processors.data_formatter import data_formatter
    from processors.bias_calculator import bias_calculator
    from utils.cache import cache

    ec_cached = cache.load('economic_calendar')
    econ_data = ec_cached['data'] if ec_cached else {
        'pillar': 'economic_calendar', 'events': [], 'pillar_score': 0, 'status': 'live'
    }

    macro_cached = cache.load('macro_sentiment')
    macro_data = macro_cached['data'] if macro_cached else {}

    # Read COT from the permanent file directly — never trigger a CFTC fetch
    # during a partial refresh. fetch() can hit CFTC on Fridays and, when
    # that fails, overwrites the file with blank data, making the display
    # permanently blank until the next successful Friday fetch.
    inst_data = None
    try:
        with open(institutional_pipeline.permanent_file, 'r') as _f:
            _cot = json.load(_f)
        if _cot.get('nq_futures'):
            inst_data = _cot
            inst_data.setdefault('status', 'live')
    except Exception as _e:
        pulse_logger.log(f"⚠️ Partial refresh — could not load COT file: {_e}", level="WARNING")

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

    try:
        with open('/data/prop_firm_weekly_threshold.json', 'r') as f:
            pf_week = json.load(f)
        if pf_week.get('is_quiet_week'):
            bias_threshold = 0.30
        else:
            bias_threshold = 0.33
    except Exception:
        bias_threshold = 0.50

    bias_score = bias_calculator.compute(formatted_data, size_mode=size_mode, bias_threshold=bias_threshold)

    recommendation = recommendation_engine.compute(
        bias_score,
        formatted_data.get('geopolitical', {}),
        formatted_data.get('macro', {}),
    )
    bias_score['recommendation'] = recommendation

    from pipelines.recommendation import prop_firm_engine
    prop_recommendation = prop_firm_engine.compute_prop_firm(
        bias_score,
        formatted_data.get('geopolitical', {}),
        formatted_data.get('macro', {}),
        formatted_data.get('economic', {}),
    )
    bias_score['recommendation_prop'] = prop_recommendation

    snapshot_generator.save(bias_score, formatted_data)
    pulse_logger.log(f"✅ {label} partial refresh complete")


# ---------------------------------------------------------------------------
# Dashboard routes
# ---------------------------------------------------------------------------

@app.route('/')
@require_auth
def home():
    latest = snapshot_generator.get_latest()
    return render_template('dashboard.html', snapshot=latest)

@app.route('/snapshot/<snapshot_id>')
@require_auth
def view_snapshot(snapshot_id):
    snapshot = snapshot_generator.load(snapshot_id)
    if not snapshot:
        return "Snapshot not found", 404
    return render_template('dashboard.html', snapshot=snapshot)


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.route('/api/latest')
@require_auth
def api_latest():
    latest = snapshot_generator.get_latest()
    return jsonify(latest)

@app.route('/api/snapshot/<snapshot_id>')
@require_auth
def api_snapshot(snapshot_id):
    snapshot = snapshot_generator.load(snapshot_id)
    if not snapshot:
        return jsonify({'error': 'Snapshot not found'}), 404
    return jsonify(snapshot)

@app.route('/api/manual_input', methods=['POST'])
@require_auth
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
            # Update the EC cache in place — no live Forex Factory fetch needed.
            try:
                from pipelines.economic_calendar import economic_calendar_pipeline
                from utils.cache import cache as _cache
                ec_cached = _cache.load('economic_calendar')
                if ec_cached:
                    ec_data = ec_cached['data']
                    for event in ec_data.get('events', []):
                        if event.get('title') == event_title:
                            result, market_impact, reason = economic_calendar_pipeline.get_market_implication(
                                event_title, actual_value,
                                event.get('forecast', ''), event.get('previous', '')
                            )
                            event['actual'] = actual_value
                            event['result'] = result
                            event['market_impact'] = market_impact
                            event['reason'] = reason
                            if story_url:
                                event['story_url'] = story_url
                            break
                    ec_data['pillar_score'] = economic_calendar_pipeline.calculate_score(ec_data['events'])
                    _cache.save('economic_calendar', ec_data)
            except Exception as cache_err:
                pulse_logger.log(f"⚠️ manual_input cache update failed: {cache_err}", level="WARNING")
            try:
                _run_partial_refresh(f"manual_input | {event_title}")
            except Exception as refresh_err:
                pulse_logger.log(f"⚠️ manual_input partial refresh failed: {refresh_err}", level="WARNING")

            try:
                from pipelines.ai_lens import ai_lens_pipeline
                latest = snapshot_generator.get_latest()
                if latest:
                    ai_lens_pipeline.generate(latest['bias'], latest['pillars'], force=True)
            except Exception as ai_err:
                pulse_logger.log(f"⚠️ AI Lens re-trigger on manual_input failed: {ai_err}", level="WARNING")

            return jsonify({'status': 'saved', 'event': event_title, 'actual': actual_value})

        return jsonify({'error': 'Failed to save'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/manual_inputs')
@require_auth
def get_manual_inputs():
    inputs = manual_input_pipeline.get_inputs()
    return jsonify(inputs)

@app.route('/api/reset_manual_input', methods=['POST'])
@require_auth
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

        # Reset the event in the EC cache in place — no live Forex Factory fetch needed.
        try:
            from pipelines.economic_calendar import economic_calendar_pipeline
            from utils.cache import cache as _cache
            ec_cached = _cache.load('economic_calendar')
            if ec_cached:
                ec_data = ec_cached['data']
                for event in ec_data.get('events', []):
                    if event.get('title') == event_title:
                        event['actual'] = 'Pending'
                        if event.get('is_speech'):
                            event['result'] = 'speech'
                            event['market_impact'] = 'unknown'
                            event['reason'] = f"{event_title} — No data to parse. Market will reprice on tone. No trade 30 minutes before."
                        else:
                            event['result'] = 'pending'
                            event['market_impact'] = 'unknown'
                            event['reason'] = f'{event_title} not yet released'
                        event.pop('story_url', None)
                        event.pop('story_context', None)
                        break
                ec_data['pillar_score'] = economic_calendar_pipeline.calculate_score(ec_data['events'])
                _cache.save('economic_calendar', ec_data)
        except Exception as cache_err:
            pulse_logger.log(f"⚠️ reset_manual_input cache update failed: {cache_err}", level="WARNING")
        try:
            _run_partial_refresh(f"reset_manual_input | {event_title}")
        except Exception as refresh_err:
            pulse_logger.log(f"⚠️ reset_manual_input partial refresh failed: {refresh_err}", level="WARNING")

        return jsonify({'status': 'reset', 'event': event_title})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/delete_ec_event', methods=['POST'])
@require_auth
def delete_ec_event():
    try:
        from pipelines.economic_calendar import economic_calendar_pipeline
        from utils.cache import cache

        data = request.get_json()
        event_title = data.get('event_title')
        if not isinstance(event_title, str) or not event_title:
            return jsonify({'error': 'Missing event_title'}), 400
        if len(event_title) > _MAX_TITLE_LEN or '\x00' in event_title:
            return jsonify({'error': 'Invalid event_title'}), 400

        ec_cached = cache.load('economic_calendar')
        if not ec_cached:
            return jsonify({'error': 'No EC cache to modify'}), 404

        ec_data = ec_cached['data']
        event = next((e for e in ec_data.get('events', []) if e.get('title') == event_title), None)
        if not event:
            return jsonify({'error': 'Event not found'}), 404

        economic_calendar_pipeline.add_to_blocklist(event_title, event.get('time_est', ''))

        # Strip the deleted event from the cache so the bias recompute below doesn't
        # need a live Forex Factory fetch to apply the blocklist.
        ec_data['events'] = [e for e in ec_data['events'] if e.get('title') != event_title]
        ec_data['pillar_score'] = economic_calendar_pipeline.calculate_score(ec_data['events'])
        cache.save('economic_calendar', ec_data)

        try:
            _run_partial_refresh(f"delete_ec_event | {event_title}")
        except Exception as refresh_err:
            pulse_logger.log(f"⚠️ delete_ec_event partial refresh failed: {refresh_err}", level="WARNING")

        return jsonify({'status': 'deleted', 'event': event_title})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/ec-blocklist', methods=['GET'])
@require_auth
def get_ec_blocklist():
    from pipelines.economic_calendar import economic_calendar_pipeline
    blocklist = economic_calendar_pipeline._load_blocklist()
    entries = {k: v for k, v in blocklist.items() if not k.startswith('__')}
    return jsonify(entries)

@app.route('/api/ec-blocklist', methods=['DELETE'])
@require_auth
def remove_ec_blocklist():
    from pipelines.economic_calendar import economic_calendar_pipeline
    data = request.get_json()
    title = data.get('title') if data else None
    if not isinstance(title, str) or not title.strip():
        return jsonify({'error': 'Missing title'}), 400
    title = title.strip()
    blocklist = economic_calendar_pipeline._load_blocklist()
    matched = [k for k in blocklist if not k.startswith('__') and k.startswith(title + '::')]
    if not matched:
        if title in blocklist and not title.startswith('__'):
            matched = [title]
    if not matched:
        return jsonify({'error': 'Title not found in EC blocklist'}), 404
    for key in matched:
        del blocklist[key]
    economic_calendar_pipeline._save_blocklist(blocklist)
    pulse_logger.log(f"🚫 EC blocklist — removed: {matched}")
    return jsonify({'status': 'removed', 'keys_removed': matched, 'blocklist': {k: v for k, v in blocklist.items() if not k.startswith('__')}})

@app.route('/api/size_mode', methods=['POST'])
@require_auth
def set_size_mode():
    data = request.get_json()
    if not data:
        return jsonify({'status': 'error', 'message': 'Missing request body'}), 400
    mode = data.get('mode', 'quarter')
    if mode not in ['quarter', 'normal']:
        return jsonify({'status': 'error', 'message': 'Invalid mode'}), 400
    size_mode_file = '/data/size_mode.json'
    try:
        atomic_write_json(size_mode_file, {'mode': mode})
        try:
            # Recompute immediately so the next /api/latest fetch (triggered by
            # setSizeMode() 1 second after toggle) returns a snapshot with the
            # updated size_mode and directive — preventing the button from
            # reverting to the previous value on auto-refresh.
            _run_partial_refresh('size_mode toggle')
        except Exception as refresh_err:
            pulse_logger.log(f"⚠️ size_mode refresh failed: {refresh_err}", level="WARNING")
        return jsonify({'status': 'saved', 'mode': mode})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

GEO_BLOCKLIST_FILE = '/data/geo_blocklist.json'

def _load_geo_blocklist():
    try:
        if os.path.exists(GEO_BLOCKLIST_FILE):
            with open(GEO_BLOCKLIST_FILE, 'r') as f:
                raw = json.load(f)
            return raw if isinstance(raw, list) else []
    except Exception:
        pass
    return []

@app.route('/api/geo-blocklist', methods=['GET'])
@require_auth
def get_geo_blocklist():
    return jsonify(_load_geo_blocklist())

@app.route('/api/geo-blocklist', methods=['POST'])
@require_auth
def add_geo_blocklist():
    data = request.get_json()
    title = data.get('title') if data else None
    if not isinstance(title, str) or not title.strip():
        return jsonify({'error': 'Missing title'}), 400
    if len(title) > _MAX_TITLE_LEN or '\x00' in title:
        return jsonify({'error': 'Invalid title'}), 400
    blocklist = _load_geo_blocklist()
    title = title.strip()
    if title not in blocklist:
        blocklist.append(title)
        atomic_write_json(GEO_BLOCKLIST_FILE, blocklist)
    pulse_logger.log(f"🚫 Geo blocklist — added: {title[:60]}")
    return jsonify({'status': 'added', 'title': title, 'blocklist': blocklist})

@app.route('/api/geo-blocklist', methods=['DELETE'])
@require_auth
def remove_geo_blocklist():
    data = request.get_json()
    title = data.get('title') if data else None
    if not isinstance(title, str) or not title.strip():
        return jsonify({'error': 'Missing title'}), 400
    blocklist = _load_geo_blocklist()
    title = title.strip()
    if title in blocklist:
        blocklist.remove(title)
        atomic_write_json(GEO_BLOCKLIST_FILE, blocklist)
        pulse_logger.log(f"🚫 Geo blocklist — removed: {title[:60]}")
        return jsonify({'status': 'removed', 'title': title, 'blocklist': blocklist})
    return jsonify({'error': 'Title not found in blocklist'}), 404

@app.route('/api/geo-tier-override', methods=['PATCH'])
@require_auth
def geo_tier_override():
    data = request.get_json()
    title = data.get('title') if data else None
    tier = data.get('tier') if data else None
    if not isinstance(title, str) or not title.strip():
        return jsonify({'error': 'Missing title'}), 400
    if tier not in (1, 2, 3):
        return jsonify({'error': 'tier must be 1, 2, or 3'}), 400
    cache_file = '/data/gemini_classifications.json'
    try:
        with open(cache_file, 'r') as f:
            cache = json.load(f)
    except Exception as e:
        return jsonify({'error': f'Failed to load classification cache: {e}'}), 500
    title = title.strip()
    if title not in cache:
        return jsonify({'error': 'Title not found in classification cache'}), 404
    old_tier = cache[title].get('tier')
    cache[title]['tier'] = tier
    atomic_write_json(cache_file, cache)
    pulse_logger.log(f"🧭 Geo tier override | {title[:60]} | {old_tier} → {tier}")
    return jsonify({'status': 'updated', 'title': title, 'old_tier': old_tier, 'tier': tier, 'entry': cache[title]})

@app.route('/api/ai_lens')
@require_auth
def api_ai_lens():
    from pipelines.ai_lens import ai_lens_pipeline
    cached = ai_lens_pipeline._load_cache()
    if not cached or not cached.get('analysis'):
        return jsonify({'error': 'No AI Lens data available'}), 404
    is_fresh = False
    try:
        tz = pytz.timezone(TIMEZONE)
        ts = datetime.fromisoformat(cached['timestamp'])
        if ts.tzinfo is None:
            ts = pytz.utc.localize(ts)
        is_fresh = ts.astimezone(tz).date() == datetime.now(tz).date()
    except Exception:
        pass
    return jsonify({
        'analysis': cached['analysis'],
        'timestamp': cached['timestamp'],
        'is_fresh': is_fresh,
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
