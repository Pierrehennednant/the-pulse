import json
import os
import re
import requests
from datetime import datetime
import pytz
from config import TIMEZONE, GROK_API_KEY
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger

AI_LENS_CACHE_FILE = '/data/ai_lens_cache.json'
_GROK_BASE_URL = 'https://api.x.ai/v1'
_GROK_MODEL = 'grok-4.20-0309-reasoning'

_SYSTEM_PROMPT = (
    "You are a market regime analyst for a systematic futures trader who trades NQ and ES. "
    "Write a concise AI Lens analysis following these four priorities in order: "
    "1) Why we're here — explain clearly why the market is in its current regime based on the data provided. "
    "2) Psychological insight — describe the emotional and nervous system state of the market right now, "
    "what the 95% of traders are likely doing, and what the disciplined 5% should be doing instead. "
    "3) Regime context — state how many days we have been in this regime and reference how long similar "
    "regimes have historically lasted and what typically ends them. "
    "4) What to watch — give exactly two or three specific concrete triggers that would break the current regime. "
    "Hard rules: maximum 180 words, tone must be calm sharp and psychologically insightful, "
    "never give directional bias or trade recommendations, always reinforce thinking like the 5% not the 95%. "
    "Never reference raw pillar score numbers in your analysis — translate all scores into qualitative language "
    "such as 'strongly bullish', 'mildly bearish', or 'neutral'. "
    "A score of zero means neutral and must always be described as neutral, never as zero or any number. "
    "Never use the word 'cap' anywhere in your analysis."
)


class AILensPipeline:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)

    def _load_cache(self):
        try:
            if os.path.exists(AI_LENS_CACHE_FILE):
                with open(AI_LENS_CACHE_FILE) as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def _save_cache(self, analysis):
        try:
            atomic_write_json(AI_LENS_CACHE_FILE, {
                'timestamp': datetime.now(self.timezone).isoformat(),
                'analysis': analysis,
            })
        except Exception as e:
            pulse_logger.log(f"⚠️ AI Lens cache write failed: {e}", level="WARNING")

    def _has_generated_today(self):
        cached = self._load_cache()
        if not cached:
            return False
        try:
            ts = datetime.fromisoformat(cached['timestamp'])
            if ts.tzinfo is None:
                ts = pytz.utc.localize(ts)
            return ts.astimezone(self.timezone).date() == datetime.now(self.timezone).date()
        except Exception:
            return False

    def _load_daily_history(self):
        """Load up to 10 daily snapshots sorted newest-first. Returns list of dicts."""
        daily_dir = '/data/snapshots/daily'
        if not os.path.exists(daily_dir):
            return []
        try:
            files = sorted(
                [f for f in os.listdir(daily_dir) if f.endswith('.json')],
                key=lambda f: os.path.getmtime(os.path.join(daily_dir, f)),
                reverse=True
            )[:10]
            history = []
            for fname in files:
                try:
                    with open(os.path.join(daily_dir, fname)) as fp:
                        snap = json.load(fp)
                    bias_block = snap.get('bias', {})
                    ts_raw = snap.get('timestamp', '')
                    date_str = ''
                    if ts_raw:
                        ts = datetime.fromisoformat(ts_raw)
                        if ts.tzinfo is None:
                            ts = pytz.utc.localize(ts)
                        date_str = ts.astimezone(self.timezone).strftime('%Y-%m-%d')
                    contributions = bias_block.get('pillar_contributions', {})
                    history.append({
                        'date': date_str,
                        'bias': bias_block.get('bias', 'Unknown'),
                        'confidence': bias_block.get('confidence', 0),
                        'pillar_scores': {
                            k: round(v.get('raw_score', 0), 3)
                            for k, v in contributions.items()
                        },
                    })
                except Exception:
                    continue
            return history
        except Exception:
            return []

    def _build_messages(self, bias_score, formatted_data):
        bias = bias_score.get('bias', 'Neutral')
        confidence = bias_score.get('confidence', 0)
        contributions = bias_score.get('pillar_contributions', {})

        macro = formatted_data.get('macro', {})
        econ = formatted_data.get('economic', {})
        geo = formatted_data.get('geopolitical', {})

        vix = macro.get('vix', {})
        vxn = macro.get('vxn', {})
        fg = macro.get('fear_greed', {})

        # Compute regime age from daily history
        history = self._load_daily_history()
        regime_age = 0
        for entry in history:
            if entry['bias'] == bias:
                regime_age += 1
            else:
                break
        regime_age = max(regime_age, 1)

        geo_items = geo.get('news_items', [])
        geo_headlines = [
            item.get('headline', item.get('title', ''))
            for item in geo_items[:5]
            if item.get('headline') or item.get('title')
        ]

        econ_events = econ.get('events', [])
        upcoming = [e for e in econ_events if e.get('result') in ('pending', 'speech')][:5]

        lines = [
            f"CURRENT STATE:",
            f"  Bias: {bias} | Confidence: {confidence}% | Regime age: {regime_age} day(s)",
            "",
            "PILLAR SCORES (current):",
        ]
        for key, data in contributions.items():
            lines.append(
                f"  {key}: score={data.get('raw_score', 0)}, "
                f"weight={data.get('base_weight', 0)}%, "
                f"status={data.get('status', 'unknown')}"
            )

        lines += [
            "",
            "MACRO SENTIMENT:",
            f"  VIX: {vix.get('value', '--')} (signal: {vix.get('signal', '--')})",
            f"  VXN: {vxn.get('value', '--')} (signal: {vxn.get('signal', '--')})",
            f"  Fear & Greed: {fg.get('score', '--')} — {fg.get('rating', 'N/A')} (signal: {fg.get('signal', '--')})",
            "",
        ]

        if history:
            lines.append(f"DAILY SNAPSHOT HISTORY (last {len(history)} closing days, newest first):")
            for i, entry in enumerate(history, 1):
                scores = ', '.join(f"{k}={v}" for k, v in entry['pillar_scores'].items())
                lines.append(
                    f"  Day {i} ({entry['date']}): "
                    f"Bias={entry['bias']}, Confidence={entry['confidence']}%, "
                    f"pillars=[{scores}]"
                )
            lines.append("")

        if geo_headlines:
            lines.append("ACTIVE GEOPOLITICAL HEADLINES:")
            for h in geo_headlines:
                lines.append(f"  - {h}")
            lines.append("")

        if upcoming:
            lines.append("UPCOMING ECONOMIC EVENTS (pending, within 48h):")
            for e in upcoming:
                lines.append(
                    f"  - {e.get('title', '')} | "
                    f"impact: {e.get('impact', '')} | "
                    f"forecast: {e.get('forecast', '--')}"
                )
            lines.append("")

        context = "\n".join(lines)

        return [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": f"Current market data:\n\n{context}"},
        ]

    def generate(self, bias_score, formatted_data, force=False):
        """Generate analysis and cache it. Returns analysis string or None."""
        if not force and self._has_generated_today():
            pulse_logger.log("🔭 AI Lens — already generated today, skipping")
            cached = self._load_cache()
            return cached.get('analysis') if cached else None

        if not GROK_API_KEY:
            pulse_logger.log("⚠️ AI Lens — GROK_API_KEY not set, serving cache", level="WARNING")
            cached = self._load_cache()
            return cached.get('analysis') if cached else None

        try:
            messages = self._build_messages(bias_score, formatted_data)
            resp = requests.post(
                f"{_GROK_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {GROK_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={"model": _GROK_MODEL, "messages": messages, "max_tokens": 1500},
                timeout=60,
            )
            resp.raise_for_status()
            raw = resp.json()['choices'][0]['message']['content']
            analysis = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()
            self._save_cache(analysis)
            pulse_logger.log(f"✓ AI Lens generated ({len(analysis.split())} words)")
            return analysis
        except Exception as e:
            pulse_logger.log(f"⚠️ AI Lens — Grok API failed: {e}, serving cache", level="WARNING")
            cached = self._load_cache()
            return cached.get('analysis') if cached else None


ai_lens_pipeline = AILensPipeline()
