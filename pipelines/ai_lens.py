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

    def _compute_regime_age(self, bias):
        """Count consecutive daily closing snapshots with the same bias direction."""
        daily_dir = '/data/snapshots/daily'
        if not os.path.exists(daily_dir):
            return 1
        try:
            files = sorted(
                [f for f in os.listdir(daily_dir) if f.endswith('.json')],
                key=lambda f: os.path.getmtime(os.path.join(daily_dir, f)),
                reverse=True
            )
            age = 0
            for fname in files:
                with open(os.path.join(daily_dir, fname)) as fp:
                    snap = json.load(fp)
                if snap.get('bias', {}).get('bias', '') == bias:
                    age += 1
                else:
                    break
            return max(age, 1)
        except Exception:
            return 1

    def _build_messages(self, bias_score, formatted_data):
        bias = bias_score.get('bias', 'Neutral')
        confidence = bias_score.get('confidence', 0)
        regime_age = self._compute_regime_age(bias)
        contributions = bias_score.get('pillar_contributions', {})

        macro = formatted_data.get('macro', {})
        econ = formatted_data.get('economic', {})
        geo = formatted_data.get('geopolitical', {})

        vix = macro.get('vix', {})
        vxn = macro.get('vxn', {})
        fg = macro.get('fear_greed', {})

        geo_items = geo.get('news_items', [])
        geo_headlines = [item.get('headline', item.get('title', '')) for item in geo_items[:5] if item.get('headline') or item.get('title')]

        econ_events = econ.get('events', [])
        upcoming = [e for e in econ_events if e.get('result') in ('pending', 'speech')][:5]

        lines = [
            f"CURRENT BIAS: {bias} | Confidence: {confidence}% | Regime age: {regime_age} day(s)",
            "",
            "PILLAR SCORES:",
        ]
        for key, data in contributions.items():
            lines.append(f"  {key}: score={data.get('raw_score', 0)}, weight={data.get('base_weight', 0)}%, status={data.get('status', 'unknown')}")

        lines += [
            "",
            "MACRO SENTIMENT:",
            f"  VIX: {vix.get('value', '--')} (signal: {vix.get('signal', '--')})",
            f"  VXN: {vxn.get('value', '--')} (signal: {vxn.get('signal', '--')})",
            f"  Fear & Greed: {fg.get('score', '--')} — {fg.get('rating', 'N/A')} (signal: {fg.get('signal', '--')})",
            "",
        ]

        if geo_headlines:
            lines.append("ACTIVE GEOPOLITICAL HEADLINES:")
            for h in geo_headlines:
                lines.append(f"  - {h}")
            lines.append("")

        if upcoming:
            lines.append("UPCOMING ECONOMIC EVENTS (within 48h, pending):")
            for e in upcoming:
                lines.append(f"  - {e.get('title', '')} | impact: {e.get('impact', '')} | forecast: {e.get('forecast', '--')}")
            lines.append("")

        context = "\n".join(lines)

        return [
            {
                "role": "system",
                "content": (
                    "You are a calm, precise market analyst writing for professional traders. "
                    "You never give directional trade recommendations. "
                    "Think and write like the disciplined 5%, not the emotional 95%. "
                    "Be sharp and direct. Maximum 180-200 words."
                )
            },
            {
                "role": "user",
                "content": (
                    f"Current market data:\n\n{context}\n"
                    "Write a market analysis covering these four points in order:\n"
                    "1. Why we're here — explain why the market is in its current regime\n"
                    "2. Psychological insight — emotional state of the market; what the 95% are doing; what the disciplined 5% should do instead\n"
                    "3. Regime context — days in this regime and historical precedent for how long similar regimes last\n"
                    "4. What to watch — two or three specific, concrete triggers that would break the current regime\n\n"
                    "Hard requirements: 180-200 words maximum, calm and sharp tone, no directional bias or trade recommendations, always reinforce thinking like the 5% not the 95%."
                )
            }
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
                headers={"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"},
                json={"model": _GROK_MODEL, "messages": messages, "max_tokens": 1500},
                timeout=60,
            )
            resp.raise_for_status()
            raw = resp.json()['choices'][0]['message']['content']
            # Strip any <think> reasoning blocks if present
            analysis = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()
            self._save_cache(analysis)
            pulse_logger.log(f"✓ AI Lens generated ({len(analysis.split())} words)")
            return analysis
        except Exception as e:
            pulse_logger.log(f"⚠️ AI Lens — Grok API failed: {e}, serving cache", level="WARNING")
            cached = self._load_cache()
            return cached.get('analysis') if cached else None


ai_lens_pipeline = AILensPipeline()
