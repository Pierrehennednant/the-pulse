import os
from datetime import datetime, timedelta
import pytz
from config import TIMEZONE, STALE_THRESHOLDS
from utils.retry import fetch_with_retry
from utils.cache import cache
from utils.logger import pulse_logger
from utils.error_handler import error_handler

class EconomicCalendarPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.cache_key = "economic_calendar"
        self.url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
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
                'speaks', 'speech', 'press conference', 'testimony',
                'statement', 'remarks', 'interview', 'appearance'
            ]
            return any(keyword in title for keyword in speech_keywords)
        return False

    def convert_to_est(self, date_str):
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            est = dt.astimezone(pytz.timezone(TIMEZONE))
            return est.strftime('%a %b %d, %I:%M %p EST')
        except Exception as e:
            pulse_logger.log(f"⚠️ Failed to parse event date '{date_str}': {e}", level="WARNING")
            return date_str

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
            'speaks', 'speech', 'press conference', 'testimony',
            'statement', 'remarks', 'interview', 'appearance'
        ]
        return any(keyword in title.lower() for keyword in speech_keywords)

    def auto_detect_speech_sentiment(self, speaker_name):
        """Query TheNewsAPI 30min after speech starts, return bearish/bullish/neutral."""
        api_key = os.environ.get('THENEWS_API_KEY', '')
        if not api_key:
            return None
        try:
            search_query = f"{speaker_name} hawkish dovish rate inflation cut hike hold tariff"
            url = "https://api.thenewsapi.com/v1/news/all"
            params = {
                'api_token': api_key,
                'search': search_query,
                'language': 'en',
                'limit': 10,
                'published_after': (datetime.now(pytz.utc) - timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M:%S')
            }
            response = fetch_with_retry(url, params=params, timeout=10)
            articles = response.json().get('data', [])
            if not articles:
                return 'neutral'

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

            if bearish_count > bullish_count:
                return 'bearish'
            elif bullish_count > bearish_count:
                return 'bullish'
            else:
                return 'neutral'
        except Exception as e:
            pulse_logger.log(f"⚠️ Speech auto-detect failed for {speaker_name}: {e}", level="WARNING")
            return 'neutral'

    def calculate_score(self, events):
        if not events:
            return 0.0
        score = 0.0
        count = 0
        impact_map = {'bullish': 1, 'bearish': -1, 'neutral': 0}
        for event in events:
            # Skip pending and unknown
            if event.get('result') in ['pending', 'unknown', 'speech']:
                continue
            # Skip neutral speech — no new info, nothing changed
            if event.get('is_speech') and event.get('market_impact') == 'neutral':
                continue
            # Only count speech if it has a directional tag
            if event.get('result') in ['beat', 'miss', 'inline', 'improved', 'declined', 'unchanged', 'bearish', 'bullish']:
                if event.get('market_impact') in impact_map:
                    score += impact_map[event['market_impact']]
                    count += 1
        return round(score / max(count, 1), 2) if count > 0 else 0.0
    
    def apply_manual_inputs(self, events):
        from pipelines.manual_input import manual_input_pipeline
        manual_inputs = manual_input_pipeline.get_inputs()
        for event in events:
            if event['title'] in manual_inputs:
                manual = manual_inputs[event['title']]
                event['actual'] = manual['actual']
                event['story_url'] = manual.get('story_url')
                event['story_context'] = manual.get('story_context')
                result, market_impact, reason = self.get_market_implication(
                    event['title'], manual['actual'], event['forecast'], event['previous']
                )
                event['result'] = result
                event['market_impact'] = market_impact
                event['reason'] = reason
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

            raw_events = response.json()
            events = []
            for event in raw_events:
                if not self.is_market_moving(event):
                    continue
                title = event.get('title', '')
                forecast = event.get('forecast', '')
                previous = event.get('previous', '')
                date_str = event.get('date', '')
                event_row = {
                    'title': title,
                    'time_est': self.convert_to_est(date_str),
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
                    event_row['result'] = 'speech'
                    event_row['market_impact'] = 'unknown'
                    event_row['reason'] = f"{event_row['title']} — No data to parse. Market will reprice on tone. No trade 30 minutes before."

                    from pipelines.manual_input import manual_input_pipeline as mip
                    TIER1_SPEAKERS = ['powell', 'fed chair', 'waller', 'williams', 'jefferson', 'kugler', 'cook', 'musalem', 'bessent', 'treasury']
                    TIER2_SPEAKERS = ['trump', 'white house', 'president', 'lagarde', 'ecb']
                    all_speakers = TIER1_SPEAKERS + TIER2_SPEAKERS

                    title_lower = event_row['title'].lower()
                    matched_speaker = next((s for s in all_speakers if s in title_lower), None)

                    if matched_speaker and date_str:
                        try:
                            speech_dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            if speech_dt.tzinfo is None:
                                speech_dt = pytz.utc.localize(speech_dt)
                            else:
                                speech_dt = speech_dt.astimezone(pytz.utc)
                            trigger_time = speech_dt + timedelta(minutes=60)
                            now_utc = datetime.now(pytz.utc)

                            if now_utc >= trigger_time and event_row['actual'] == 'Pending':
                                existing = mip.get_inputs().get(event_row['title'])
                                if not existing:
                                    pulse_logger.log(f"🎙️ Auto-detecting speech sentiment for: {event_row['title']}")
                                    sentiment = self.auto_detect_speech_sentiment(matched_speaker)
                                    if sentiment:
                                        mip.save_actual(event_row['title'], sentiment, None)
                                        pulse_logger.log(f"✅ Auto-tagged {event_row['title']} as {sentiment}")
                        except Exception as e:
                            pulse_logger.log(f"⚠️ Speech trigger check failed: {e}", level="WARNING")
                else:
                    event_row['is_speech'] = False
                events.append(event_row)

            events = self.apply_manual_inputs(events)
            score = self.calculate_score(events)
            warnings = []

            result_data = {
                'pillar': 'economic_calendar',
                'timestamp': datetime.now(self.timezone).isoformat(),
                'events': events,
                'pillar_score': score,
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
