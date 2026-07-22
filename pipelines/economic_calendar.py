import json
import os
from datetime import datetime, timedelta
import anthropic
import pytz
from config import TIMEZONE, STALE_THRESHOLDS
from utils.retry import fetch_with_retry
from utils.file_lock import atomic_write_json
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler
from pipelines.manual_input import manual_input_pipeline

BLOCKLIST_FILE = '/data/ec_blocklist.json'

class EconomicCalendarPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "economic_calendar"
        self.url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def _blocklist_key(self, title, time_est):
        date_part = time_est.split(',')[0] if ',' in time_est else time_est[:10]
        return f"{title}::{date_part}"

    def _load_blocklist(self):
        try:
            if os.path.exists(BLOCKLIST_FILE):
                with open(BLOCKLIST_FILE, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _save_blocklist(self, blocklist):
        try:
            atomic_write_json(BLOCKLIST_FILE, blocklist)
        except Exception as e:
            pulse_logger.log(f"⚠️ EC blocklist save failed: {e}", level="WARNING")

    def add_to_blocklist(self, title, time_est):
        blocklist = self._load_blocklist()
        key = self._blocklist_key(title, time_est)
        blocklist[key] = {
            'title': title,
            'time_est': time_est,
            'blocked_at': datetime.now(self.timezone).isoformat()
        }
        self._save_blocklist(blocklist)
        pulse_logger.log(f"🚫 EC blocklist: added '{key}'")

    def maybe_reset_weekly_blocklist(self):
        """Clear the blocklist when a new week's FF data arrives (Sunday only).
        Called exclusively from the scheduled run_pulse() so partial refreshes
        triggered by deletions or manual inputs never fire the reset."""
        if datetime.now(self.timezone).weekday() != 6:
            return
        this_week = datetime.now(self.timezone).strftime('%Y-%W')
        blocklist = self._load_blocklist()
        if blocklist.get('__reset_week__') == this_week:
            return
        self._save_blocklist({'__reset_week__': this_week})
        pulse_logger.log("🗑️ EC blocklist cleared — Sunday weekly reset")

    def is_market_moving(self, event):
        if event.get('country', '').upper() != 'USD':
            return False
        impact = event.get('impact', '').lower()
        if impact == 'high':
            return True
        if impact == 'medium':
            # Only keep medium impact if it's a speech event
            title = event.get('title', '').lower()
            speech_keywords = [
                'speaks', 'speech', 'press conference', 'testimony', 'testifies',
                'statement', 'remarks', 'interview', 'appearance'
            ]
            return any(keyword in title for keyword in speech_keywords)
        return False

    def convert_to_est(self, date_str):
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.utc)
            est = dt.astimezone(pytz.timezone(TIMEZONE))
            return est.strftime('%a %b %d, %I:%M %p EST')
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to parse event date '{date_str}': {e}", level="WARNING")
            return date_str

    # Events that are shown in the dashboard as watch events but excluded from EC scoring.
    SCORING_EXCLUSIONS = {'FOMC Meeting Minutes'}

    # Explicit polarity per event — +1 means beating forecast is bullish for equities,
    # -1 means beating forecast is bearish (inflation / unemployment events).
    # Formula: final_score = magnitude × POLARITY[event] × sign(surprise)
    POLARITY = {
        "Non-Farm Employment Change":     +1,
        "ADP Non-Farm Employment Change": +1,
        "Unemployment Rate":              -1,
        "Average Hourly Earnings m/m":    -1,
        "Core CPI m/m":                   -1,
        "CPI m/m":                        -1,
        "CPI y/y":                        -1,
        "Core PPI m/m":                   -1,
        "PPI m/m":                        -1,
        "Core PCE m/m":                   -1,
        "GDP q/q":                        +1,
        "Final GDP q/q":                  +1,
        "ISM Manufacturing PMI":          +1,
        "ISM Services PMI":               +1,
        "Retail Sales m/m":               +1,
        "Core Retail Sales m/m":          +1,
    }

    # Inflation metrics — higher = more inflation = bearish for equities
    # Beat/miss logic inverts: miss = less inflation = bullish, beat = more inflation = bearish
    INFLATION_METRICS = [
        'ppi m/m', 'core ppi m/m',
        'cpi m/m', 'core cpi m/m',
        'cpi y/y', 'core cpi y/y',
        'ppi y/y', 'core ppi y/y',
        'pce price index m/m', 'core pce price index m/m',
    ]

    def is_inflation_metric(self, title):
        return title.lower().strip() in self.INFLATION_METRICS

    def get_market_implication(self, title, actual, forecast, previous):
        if actual in ['hawkish', 'dovish', 'neutral', 'bearish', 'bullish']:
            if actual in ['hawkish', 'bearish']:
                return 'bearish', 'bearish', f"{title} — Bearish tone detected. Rate fears or hawkish stance, bearish for equities."
            elif actual in ['dovish', 'bullish']:
                return 'bullish', 'bullish', f"{title} — Bullish tone detected. Dovish stance or supportive language, bullish for equities."
            else:
                return 'neutral', 'neutral', f"{title} — Neutral tone. No major market repricing expected."
        if not actual or actual == '':
            return 'pending', 'unknown', f'{title} not yet released'
        try:
            actual_val = float(actual.replace('%', '').replace('K', '').replace('M', '').replace('B', '').replace('T', ''))
            forecast_val = float(forecast.replace('%', '').replace('K', '').replace('M', '').replace('B', '').replace('T', '')) if forecast else None
            previous_val = float(previous.replace('%', '').replace('K', '').replace('M', '').replace('B', '').replace('T', '')) if previous else None
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to parse values for '{title}': {e}", level="WARNING")
            return 'pending', 'unknown', f'{title} — cannot parse values'

        inverted = self.is_inflation_metric(title)

        if forecast_val is not None:
            if actual_val > forecast_val:
                if inverted:
                    return 'beat', 'bearish', f'{title} came in hot ({actual} vs {forecast} expected) — higher inflation, bearish for equities'
                return 'beat', 'bullish', f'{title} beat forecast ({actual} vs {forecast})'
            elif actual_val < forecast_val:
                if inverted:
                    return 'miss', 'bullish', f'{title} came in cool ({actual} vs {forecast} expected) — lower inflation, bullish for equities'
                return 'miss', 'bearish', f'{title} missed forecast ({actual} vs {forecast})'
            else:
                return 'inline', 'neutral', f'{title} inline with forecast ({actual})'
        elif previous_val is not None:
            if actual_val > previous_val:
                if inverted:
                    return 'improved', 'bearish', f'{title} rose from previous ({actual} vs {previous}) — rising inflation, bearish for equities'
                return 'improved', 'bullish', f'{title} improved from previous ({actual} vs {previous})'
            elif actual_val < previous_val:
                if inverted:
                    return 'declined', 'bullish', f'{title} fell from previous ({actual} vs {previous}) — cooling inflation, bullish for equities'
                return 'declined', 'bearish', f'{title} declined from previous ({actual} vs {previous})'
            else:
                return 'unchanged', 'neutral', f'{title} unchanged from previous ({actual})'
        return 'pending', 'unknown', f'{title} — no comparison available'

    def is_speech_event(self, title):
        speech_keywords = [
            'speaks', 'speech', 'press conference', 'testimony', 'testifies',
            'statement', 'remarks', 'interview', 'appearance'
        ]
        return any(keyword in title.lower() for keyword in speech_keywords)

    def _classify_speaker(self, title):
        t = title.lower()
        if 'fed chairman' in t or 'fed chair' in t:
            return 'Fed Chair'
        if 'president' in t:
            return 'Presidential'
        return 'Other'

    def auto_detect_speech_sentiment(self, event_title, speaker_type='Other'):
        """Query TheNewsAPI after speech starts, return (label, confidence, tier) tuple.
        tier is only meaningful for Presidential speeches; defaults to 'T2' for others."""
        api_key = os.environ.get('THENEWS_API_KEY', '')
        if not api_key:
            return None, 0.5, 'T2'
        try:
            url = "https://api.thenewsapi.com/v1/news/all"
            params = {
                'api_token': api_key,
                'search': event_title,
                'language': 'en',
                'limit': 10,
                'published_after': (datetime.now(pytz.utc) - timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M:%S')
            }
            response = fetch_with_retry(url, params=params, timeout=10)
            articles = response.json().get('data', [])
            if not articles:
                return 'neutral', 0.5, 'T2'

            bearish_keywords = ['hawkish', 'hike', 'tighten', 'inflation concern', 'higher for longer', 'no cuts', 'tariff', 'restrictive']
            bullish_keywords = ['dovish', 'cut', 'ease', 'pivot', 'supportive', 'pause', 'accommodative', 'rate cut']

            bearish_count = 0
            bullish_count = 0
            for article in articles:
                text = (article.get('title', '') + ' ' + article.get('description', '')).lower()
                if any(kw in text for kw in bearish_keywords):
                    bearish_count += 1
                if any(kw in text for kw in bullish_keywords):
                    bullish_count += 1

            total = bearish_count + bullish_count
            confidence = max(bearish_count, bullish_count) / total if total > 0 else 0.5

            if bearish_count > bullish_count:
                sentiment = 'bearish'
            elif bullish_count > bearish_count:
                sentiment = 'bullish'
            else:
                sentiment = 'neutral'

            tier = 'T2'
            if speaker_type == 'Presidential':
                tier = self._haiku_presidential_tier(event_title, articles)

            return sentiment, confidence, tier
        except Exception as e:
            pulse_logger.log(f"⚠️ Speech auto-detect failed for {event_title}: {e}", level="WARNING")
            return 'neutral', 0.5, 'T2'

    def _haiku_presidential_tier(self, event_title, articles):
        """Classify a Presidential speech by market-impact tier via Haiku.
        Returns 'T1', 'T2', or 'T3'. Defaults to 'T2' on any failure."""
        if not articles:
            return 'T2'
        try:
            article_texts = '\n'.join(
                f"- {a.get('title', '')} {a.get('description', '')}"
                for a in articles[:8]
            ).strip()
            prompt = (
                f"A US Presidential speech titled '{event_title}' just occurred. "
                f"Here are recent news article headlines and descriptions about it:\n\n"
                f"{article_texts}\n\n"
                "Classify the speech's market impact as exactly one of:\n"
                "T1 — Major impact: new tariffs, trade war escalation, significant sanctions, "
                "Fed policy commentary, declarations of economic emergency, or any announcement "
                "that directly and materially moves equity markets.\n"
                "T2 — Notable: policy updates without major new measures, diplomatic developments, "
                "general economic commentary, or statements that may influence but do not shock markets.\n"
                "T3 — Routine: ceremonial appearances, non-economic topics, congratulatory remarks, "
                "or any speech with negligible direct market relevance.\n\n"
                "Respond with ONLY the label: T1, T2, or T3"
            )
            client = anthropic.Anthropic()
            response = client.messages.create(
                model='claude-haiku-4-5-20251001',
                max_tokens=8,
                messages=[{'role': 'user', 'content': prompt}]
            )
            tier = response.content[0].text.strip().upper()
            if tier not in ('T1', 'T2', 'T3'):
                pulse_logger.log(f"⚠️ Haiku returned unexpected presidential tier '{tier}' — defaulting to T2", level="WARNING")
                return 'T2'
            pulse_logger.log(f"🎙️ Presidential speech tier (Haiku): {tier} — {event_title}")
            return tier
        except Exception as e:
            pulse_logger.log(f"⚠️ Presidential tier classification failed: {e} — defaulting to T2", level="WARNING")
            return 'T2'

    def _magnitude_score(self, event, direction):
        """Return magnitude-weighted score for a numerical beat/miss event.

        Event name is read from event['title'] — the key used throughout fetch().
        Relative deviation = abs(actual - forecast) / abs(forecast).
        Bands: ≤20% → ±0.40 | 21–50% → ±0.63 | >50% → ±0.88
        When the event title is in POLARITY, sign = POLARITY[title] × sign(surprise).
        Falls back to the market_impact direction for unrecognised events (with warning).
        Falls back to flat direction if forecast is zero or values can't be parsed.
        """
        if direction == 0.0:
            return 0.0
        forecast_str = event.get('forecast', '')
        actual_str = event.get('actual', '')
        if not forecast_str or forecast_str in ('N/A', ''):
            return direction
        try:
            def _parse(s):
                return float(str(s).replace('%', '').replace('K', '').replace('M', '')
                             .replace('B', '').replace('T', '').strip())
            actual_val = _parse(actual_str)
            forecast_val = _parse(forecast_str)
        except (ValueError, TypeError):
            return direction
        if forecast_val == 0:
            return direction  # avoid division by zero
        rel_dev = abs(actual_val - forecast_val) / abs(forecast_val)
        magnitude = 0.88 if rel_dev > 0.50 else 0.63 if rel_dev > 0.20 else 0.40
        title = event.get('title', '')
        if title in self.POLARITY:
            sign_surprise = 1 if actual_val > forecast_val else (-1 if actual_val < forecast_val else 0)
            return magnitude * self.POLARITY[title] * sign_surprise
        pulse_logger.log(f"⚠️ EC scoring: '{title}' not in POLARITY map — using market_impact direction", level="WARNING")
        return magnitude if direction > 0 else -magnitude

    def _count_red_folder_days(self, events):
        """Count calendar days with at least one high-impact event (excluding SCORING_EXCLUSIONS)."""
        red_days = set()
        for e in events:
            if e.get('title') in self.SCORING_EXCLUSIONS:
                continue
            if e.get('impact', '').lower() == 'high':
                time_est = e.get('time_est', '')
                day = time_est.split(',')[0] if ',' in time_est else time_est[:10]
                if day:
                    red_days.add(day)
        return len(red_days)

    def calculate_score(self, events):
        if not events:
            return 0.0
        score = 0.0
        count = 0
        flat_map = {'bullish': 1.0, 'bearish': -1.0, 'neutral': 0.0}
        for event in events:
            # Watch-only events — show in dashboard but do not score
            if event.get('title') in self.SCORING_EXCLUSIONS:
                continue
            # Skip pending and unknown
            if event.get('result') in ['pending', 'unknown', 'speech']:
                continue
            # Skip neutral speech — no new info, nothing changed
            if event.get('is_speech') and event.get('market_impact') == 'neutral':
                continue
            result = event.get('result', '')
            market_impact = event.get('market_impact', 'neutral')
            if result not in ['beat', 'miss', 'inline', 'improved', 'declined', 'unchanged', 'bearish', 'bullish']:
                continue
            if market_impact not in flat_map:
                continue
            direction = flat_map[market_impact]
            # Magnitude-weighted scoring for numerical events with actual vs forecast.
            # 'improved'/'declined'/'unchanged' compare against previous (no forecast) — flat.
            # Speech events: cap × direction × confidence (Other: flat cap, no confidence scaling).
            if result in ('beat', 'miss', 'inline'):
                evt_score = self._magnitude_score(event, direction)
            elif event.get('is_speech'):
                speaker_type = event.get('speaker_type', 'Other')
                if speaker_type == 'Fed Chair':
                    cap = 0.55
                    confidence = event.get('confidence', 0.75)
                    evt_score = cap * direction * confidence
                elif speaker_type == 'Presidential':
                    _tier = event.get('speech_tier', 'T2')
                    cap = 0.55 if _tier == 'T1' else 0.10 if _tier == 'T3' else 0.25
                    confidence = event.get('confidence', 0.75)
                    evt_score = cap * direction * confidence
                else:
                    evt_score = 0.25 * direction
            else:
                evt_score = direction
            score += evt_score
            count += 1
            event['evt_score'] = round(evt_score, 4)
        return round(score / max(count, 1), 2) if count > 0 else 0.0

    def apply_manual_inputs(self, events):
        manual_inputs = manual_input_pipeline.get_inputs()
        for event in events:
            title = event['title']
            key = manual_input_pipeline.make_key(title, event.get('event_date', ''))
            # Lookup order: 1) exact compound key, 2) bare title legacy fallback.
            # Tier-3 title-prefix wildcard removed — caused cross-date contamination
            # when the same event title appeared on multiple days in the same week.
            manual = manual_inputs.get(key) or manual_inputs.get(title)
            if manual:
                event['actual'] = manual['actual']
                event['story_url'] = manual.get('story_url')
                event['story_context'] = manual.get('story_context')
                event['confidence'] = manual.get('confidence', 0.75)
                result, market_impact, reason = self.get_market_implication(
                    event['title'], manual['actual'], event['forecast'], event['previous']
                )
                event['result'] = result
                event['market_impact'] = market_impact
                event['reason'] = reason
            elif event.get('is_speech') and event.get('actual') not in ('Pending', '', None):
                # No current manual input — reset stale speech event to default state.
                # Handles the case where a manual input was deleted but the EC pipeline
                # cache still holds the old result/market_impact/evt_score.
                event['actual'] = 'Pending'
                event['result'] = 'speech'
                event['market_impact'] = 'unknown'
                event['reason'] = f"{title} — No data to parse. Market will reprice on tone. No trade 30 minutes before."
                event.pop('evt_score', None)
                event.pop('confidence', None)
                event.pop('story_url', None)
                event.pop('story_context', None)
        return events

    def fetch(self):
        try:
            response = fetch_with_retry(self.url, headers=self.headers, timeout=10)

            if response.status_code == 429:
                pulse_logger.log("⚠️ Forex Factory rate limited — using cached data", level="WARNING")
                cached = cache.load(self.cache_key)
                if cached:
                    cached['data']['events'] = self.apply_manual_inputs(cached['data'].get('events', []))
                    cached['data']['status'] = 'stale'
                    return cached['data']
                return {
                    'pillar': 'economic_calendar',
                    'timestamp': datetime.now(self.timezone).isoformat(),
                    'events': [],
                    'pillar_score': 0,
                    'warnings': ['⚠️ Economic calendar temporarily unavailable — rate limited'],
                    'status': 'stale'
                }

            if not response.text.strip():
                raise ValueError("Empty response from Forex Factory")

            blocklist = self._load_blocklist()

            raw_events = response.json()
            events = []
            date_strs = {}
            ff_keys = set()
            for event in raw_events:
                if not self.is_market_moving(event):
                    continue
                title = event.get('title', '')
                forecast = event.get('forecast', '')
                previous = event.get('previous', '')
                date_str = event.get('date', '')
                time_est = self.convert_to_est(date_str)
                bl_key = self._blocklist_key(title, time_est)
                ff_keys.add(bl_key)
                if bl_key in blocklist and not bl_key.startswith('__'):
                    pulse_logger.log(f"🚫 EC blocklist: skipping '{title}'")
                    continue
                event_date = date_str[:10] if date_str else ''
                event_row = {
                    'title': title,
                    'time_est': time_est,
                    'event_date': event_date,
                    'forecast': forecast or 'N/A',
                    'previous': previous or 'N/A',
                    'actual': 'Pending',
                    'impact': event.get('impact', ''),
                    'result': 'pending',
                    'market_impact': 'unknown',
                    'reason': f'{title} not yet released'
                }
                if self.is_speech_event(event_row['title']):
                    event_row['is_speech'] = True
                    event_row['speaker_type'] = self._classify_speaker(title)
                    event_row['result'] = 'speech'
                    event_row['market_impact'] = 'unknown'
                    event_row['reason'] = f"{event_row['title']} — No data to parse. Market will reprice on tone. No trade 30 minutes before."
                    date_strs[title] = date_str
                else:
                    event_row['is_speech'] = False
                events.append(event_row)

            # Self-clean: drop blocklist entries FF no longer serves
            if blocklist:
                stale = [k for k in blocklist if k not in ff_keys and not k.startswith('__')]
                if stale:
                    for k in stale:
                        del blocklist[k]
                    self._save_blocklist(blocklist)
                    pulse_logger.log(f"🧹 EC blocklist self-cleaned: {stale}")

            # Apply saved manual inputs before speech detection so actual reflects prior tags
            events = self.apply_manual_inputs(events)

            today_str = datetime.now(self.timezone).strftime('%Y-%m-%d')
            for event_row in events:
                if not event_row.get('is_speech'):
                    continue
                if event_row.get('event_date', '') != today_str:
                    continue
                date_str = date_strs.get(event_row['title'], '')
                if date_str:
                    try:
                        speech_dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        if speech_dt.tzinfo is None:
                            speech_dt = pytz.utc.localize(speech_dt)
                        else:
                            speech_dt = speech_dt.astimezone(pytz.utc)
                        trigger_time = speech_dt + timedelta(minutes=60)
                        now_utc = datetime.now(pytz.utc)

                        if now_utc >= trigger_time:
                            all_inputs = manual_input_pipeline.get_inputs()
                            t = event_row['title']
                            existing = next((v for k, v in all_inputs.items()
                                             if k == t or k.startswith(t + '::')), None)
                            if not existing:
                                pulse_logger.log(f"🎙️ Auto-detecting speech sentiment for: {event_row['title']}")
                                speaker_type_local = event_row.get('speaker_type', 'Other')
                                sentiment, conf, tier = self.auto_detect_speech_sentiment(
                                    event_row['title'], speaker_type=speaker_type_local
                                )
                                if sentiment:
                                    manual_input_pipeline.save_actual(event_row['title'], sentiment, None, event_date=event_row.get('event_date', ''), confidence=conf)
                                    pulse_logger.log(f"✅ Auto-tagged {event_row['title']} as {sentiment} (confidence: {conf:.2f})")
                                if speaker_type_local == 'Presidential' and tier:
                                    event_row['speech_tier'] = tier
                    except Exception as e:
                        pulse_logger.log(f"⚠️ Speech trigger check failed: {e}", level="WARNING")

            events = self.apply_manual_inputs(events)
            score = self.calculate_score(events)
            warnings = []

            # Weak EC Mode — dampen score when the week has ≤1 meaningful red folder day
            red_folder_days = self._count_red_folder_days(events)
            weak_ec_week = red_folder_days <= 1
            if weak_ec_week:
                score = round(score * 0.5, 2)

            result_data = {
                'pillar': 'economic_calendar',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'events': events,
                'pillar_score': score,
                'weak_ec_week': weak_ec_week,
                'red_folder_days': red_folder_days,
                'warnings': warnings,
                'status': 'live'
            }
            cache.save(self.cache_key, result_data)
            pulse_logger.log(f"✓ Economic Calendar updated | {len(events)} USD high/medium impact events | Score: {score}")
            return result_data

        except Exception as e:
            error_handler.handle(e, "Economic Calendar")
            cached = cache.load(self.cache_key)
            if cached:
                cached['data']['events'] = self.apply_manual_inputs(cached['data'].get('events', []))
                cached['data']['status'] = 'stale'
                return cached['data']
            return {
                'pillar': 'economic_calendar',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'events': [],
                'pillar_score': 0,
                'warnings': [],
                'status': 'unavailable'
            }

economic_calendar_pipeline = EconomicCalendarPipeline()
