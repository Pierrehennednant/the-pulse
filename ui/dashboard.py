import hmac
import json
import os
import threading
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

    # Derive bias_threshold from EC pipeline's weak_ec_week flag — same
    # canonical source as _get_weekly_threshold(), same pattern already used
    # in main.py's run_pulse(). Deliberately NOT reading
    # /data/prop_firm_weekly_threshold.json directly here: that file can be
    # briefly stale relative to THIS cycle's own live EC data (it's only
    # corrected once compute_prop_firm() -> _get_weekly_threshold() runs,
    # which happens after this line), whereas weak_ec_week is computed fresh,
    # in-memory, every time economic_calendar.py's fetch() runs — no caching
    # delay, so no staleness window at all.
    _ec_weak = formatted_data.get('economic', {}).get('weak_ec_week')
    if _ec_weak is True:
        bias_threshold = 0.30
    elif _ec_weak is False:
        bias_threshold = 0.33
    else:
        bias_threshold = 0.50

    bias_score = bias_calculator.compute(formatted_data, bias_threshold=bias_threshold)

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
        event_date = data.get('event_date', '')
        confidence = data.get('confidence', 0.75)
        # Rate decision events only (Federal Funds Rate — the sole entry point,
        # see pipelines/economic_calendar.py's RATE_DECISION_EVENTS). None for
        # every other event type.
        priced_action = data.get('priced_action', None)
        dots_signal = data.get('dots_signal', None)

        # FOMC Statement is informational-only — Federal Funds Rate is the
        # sole entry point for Action/Priced Action (see
        # economic_calendar.py's RATE_DECISION_EVENTS). Reject at the API
        # level too, not just the UI, so nothing can create a second,
        # duplicate signal by posting directly.
        if event_title == 'FOMC Statement':
            return jsonify({
                'error': 'FOMC Statement is informational-only — enter '
                         'Action / Priced Action on Federal Funds Rate instead.'
            }), 400

        ok, err = _validate_manual_input(event_title, actual_value)
        if not ok:
            return jsonify({'error': err}), 400

        success = manual_input_pipeline.save_actual(
            event_title, actual_value, story_url, event_date=event_date,
            confidence=confidence, priced_action=priced_action, dots_signal=dots_signal
        )

        if success:
            # Update the EC cache in place — no live Forex Factory fetch needed.
            try:
                from pipelines.economic_calendar import economic_calendar_pipeline
                from utils.cache import cache as _cache
                ec_cached = _cache.load('economic_calendar')
                if ec_cached:
                    ec_data = ec_cached['data']
                    for event in ec_data.get('events', []):
                        if event.get('title') == event_title and (not event_date or event.get('event_date', '') == event_date):
                            result, market_impact, reason = economic_calendar_pipeline.get_market_implication(
                                event_title, actual_value,
                                event.get('forecast', ''), event.get('previous', ''),
                                priced_action=priced_action
                            )
                            event['actual'] = actual_value
                            event['result'] = result
                            event['market_impact'] = market_impact
                            event['reason'] = reason
                            event['confidence'] = confidence
                            if event_title in economic_calendar_pipeline.RATE_DECISION_EVENTS:
                                event['priced_action'] = priced_action
                                event['dots_signal'] = dots_signal
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
        event_date = data.get('event_date', '')
        if not isinstance(event_title, str) or not event_title:
            return jsonify({'error': 'Missing event_title'}), 400
        if len(event_title) > _MAX_TITLE_LEN or '\x00' in event_title:
            return jsonify({'error': 'Invalid event_title'}), 400
        if event_title == 'FOMC Statement':
            return jsonify({'error': 'FOMC Statement has nothing to reset — informational-only.'}), 400

        with open('/data/permanent_manual_inputs.json', 'r') as f:
            inputs = json.load(f)
        mi_key = manual_input_pipeline.make_key(event_title, event_date)
        # Remove compound key; also remove legacy title-only key if present
        changed = False
        if mi_key in inputs:
            del inputs[mi_key]
            changed = True
        if event_title in inputs:
            del inputs[event_title]
            changed = True
        if changed:
            atomic_write_json('/data/permanent_manual_inputs.json', inputs)

        # Reset the event in the EC cache in place — no live Forex Factory fetch needed.
        try:
            from pipelines.economic_calendar import economic_calendar_pipeline
            from utils.cache import cache as _cache
            ec_cached = _cache.load('economic_calendar')
            if ec_cached:
                ec_data = ec_cached['data']
                for event in ec_data.get('events', []):
                    if event.get('title') == event_title and (not event_date or event.get('event_date', '') == event_date):
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
                        event.pop('evt_score', None)
                        event.pop('confidence', None)
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

GEO_MANUAL_BLOCKLIST_FILE = '/data/geo_manual_blocklist.json'

def _load_geo_manual_blocklist():
    try:
        if os.path.exists(GEO_MANUAL_BLOCKLIST_FILE):
            with open(GEO_MANUAL_BLOCKLIST_FILE, 'r') as f:
                raw = json.load(f)
            if isinstance(raw, list):
                return [e for e in raw if isinstance(e, dict) and e.get('title')]
    except Exception:
        pass
    return []

@app.route('/api/geo-blocklist', methods=['GET'])
@require_auth
def get_geo_blocklist():
    return jsonify(_load_geo_manual_blocklist())

def _finish_geo_blocklist_post_actions(label):
    """Runs AFTER the blocklist file write and AFTER the HTTP response has
    already been sent (called from a background thread, never inline in
    the request handler). Live reproduction (2026-09-23) found
    `from pipelines.geopolitical import geopolitical_pipeline` can stall
    ~23s in this environment when its own dependency (the sentiment-
    analysis model) fails to load — running that import, the cache sync,
    the pin-store scrub, and _run_partial_refresh() synchronously in the
    request path meant a slow/failed import could make the whole
    block/unblock request take longer than _blocklistAction()'s 15s
    client-side timeout, so the UI reported a failure even though the
    actual blocklist file write (the part that matters for correctness)
    had already succeeded before this function was ever called.

    Import-scoping note (same as the earlier fix this replaces): import
    once, bind the name (None on failure) — never inside just the first
    try block — since `from pipelines.geopolitical import
    geopolitical_pipeline` anywhere in a function makes that name
    function-local for the WHOLE function per Python's scoping rules,
    and a later block using it after a failed import would otherwise
    raise UnboundLocalError instead of a clear message.
    """
    try:
        from pipelines.geopolitical import geopolitical_pipeline
    except Exception as import_err:
        geopolitical_pipeline = None
        pulse_logger.log(f"⚠️ {label} — could not import geopolitical_pipeline: {import_err}", level="WARNING")
    # Sync the geo pillar cache — _run_partial_refresh() below reads this
    # cache raw, with no blocklist filtering of its own. Without this, the
    # freshly-(un)blocked article (and the score it contributes) stays in
    # the cache until the next scheduled fetch() cycle, up to 5 minutes
    # later, even though the blocklist file itself is already updated.
    if geopolitical_pipeline is not None:
        try:
            from utils.cache import cache as _cache
            geo_cached = _cache.load('geopolitical')
            if geo_cached:
                refreshed = geopolitical_pipeline._refresh_cached_data(geo_cached['data'])
                _cache.save('geopolitical', refreshed)
        except Exception as cache_err:
            pulse_logger.log(f"⚠️ {label} cache sync failed: {cache_err}", level="WARNING")
        # Also scrub the persisted pin store — _refresh_cached_data() above
        # only touches this cycle's cached all_items, it never looks at
        # pinned_stories.json. Without this, a blocked PINNED article stays
        # in the pin file untouched, and only gets purged whenever the next
        # genuine live fetch_news() cycle happens to run
        # load_pinned_stories() — which can be delayed indefinitely during
        # a TheNewsAPI outage stretch, since every cache-fallback cycle in
        # between skips this function entirely.
        try:
            geopolitical_pipeline.load_pinned_stories()
        except Exception as pin_err:
            pulse_logger.log(f"⚠️ {label} pin scrub failed: {pin_err}", level="WARNING")
    try:
        _run_partial_refresh(label)
    except Exception as refresh_err:
        pulse_logger.log(f"⚠️ {label} partial refresh failed: {refresh_err}", level="WARNING")


@app.route('/api/geo-blocklist', methods=['POST'])
@require_auth
def add_geo_blocklist():
    data = request.get_json()
    title = data.get('title') if data else None
    if not isinstance(title, str) or not title.strip():
        return jsonify({'error': 'Missing title'}), 400
    if len(title) > _MAX_TITLE_LEN or '\x00' in title:
        return jsonify({'error': 'Invalid title'}), 400
    blocklist = _load_geo_manual_blocklist()
    title = title.strip()
    if not any(entry['title'] == title for entry in blocklist):
        blocklist.append({
            'title': title,
            'blocked_at': datetime.now(pytz.utc).isoformat()
        })
        atomic_write_json(GEO_MANUAL_BLOCKLIST_FILE, blocklist)
    pulse_logger.log(f"🚫 Geo manual blocklist — added: {title[:60]}")
    # The write above is the only part that matters for correctness, and
    # it's already durable at this point. Cache sync / pin scrub / partial
    # refresh are best-effort freshness, not correctness — self-heal on
    # the next cycle regardless — so they run in the background AFTER this
    # response goes out, not before it (see _finish_geo_blocklist_post_actions
    # for why: they can stall for many seconds on a slow/broken
    # dependency, and the frontend must see 'added' well under its own
    # 15s timeout since the file write it cares about already happened).
    threading.Thread(
        target=_finish_geo_blocklist_post_actions,
        args=(f"geo-blocklist | {title[:40]}",),
        daemon=True
    ).start()
    return jsonify({'status': 'added', 'title': title, 'blocklist': blocklist})

@app.route('/api/geo-blocklist', methods=['DELETE'])
@require_auth
def remove_geo_blocklist():
    data = request.get_json()
    title = data.get('title') if data else None
    if not isinstance(title, str) or not title.strip():
        return jsonify({'error': 'Missing title'}), 400
    blocklist = _load_geo_manual_blocklist()
    title = title.strip()
    new_blocklist = [entry for entry in blocklist if entry['title'] != title]
    if len(new_blocklist) == len(blocklist):
        return jsonify({'error': 'Title not found in blocklist'}), 404
    atomic_write_json(GEO_MANUAL_BLOCKLIST_FILE, new_blocklist)
    pulse_logger.log(f"🚫 Geo manual blocklist — removed: {title[:60]}")
    # Same reasoning as add_geo_blocklist() — keep the two symmetric, as
    # the rest of this codebase already insists they should be. The
    # original bug report that started this whole investigation was about
    # UNBLOCK hanging on exactly this same stall, so this side needs the
    # same fix at least as much as add does.
    threading.Thread(
        target=_finish_geo_blocklist_post_actions,
        args=(f"geo-unblock | {title[:40]}",),
        daemon=True
    ).start()
    return jsonify({'status': 'removed', 'title': title, 'blocklist': new_blocklist})

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

@app.route('/api/geo-force-reclassify', methods=['POST'])
@require_auth
def geo_force_reclassify():
    """Manual trigger for GeopoliticalPipeline.force_reclassify() — re-runs
    the full current classification prompt against a pinned or cached
    item's STORED source text (never a live fetch of story_url) and
    applies the direction-dependent clock/anchor rule. No automatic/
    scheduled version — this route is the only trigger surface."""
    data = request.get_json()
    headline = data.get('headline') if data else None
    if not isinstance(headline, str) or not headline.strip():
        return jsonify({'error': 'Missing headline'}), 400
    from pipelines.geopolitical import geopolitical_pipeline
    result = geopolitical_pipeline.force_reclassify(headline.strip())
    if not result.get('ok'):
        return jsonify({'error': result.get('error', 'Reclassification failed')}), 400
    return jsonify(result)

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
