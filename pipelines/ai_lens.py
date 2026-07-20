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
    "You are writing the AI Lens for a systematic NQ and ES futures trader. "
    "Write like a sharp trader. No academic language, no passive constructions, no filler. "
    "Total output must be under 160 words across all three sections. No word count in output. "
    "Produce exactly three sections in this order, using these exact headers:\n\n"
    "Why We're Here\n"
    "Maximum 3 sentences. Diagnostic and direct — name the specific pillars driving or canceling the current signal "
    "and explain precisely why they are aligning or conflicting. Reference the actual pillar values passed in. No filler sentences.\n\n"
    "Psychological Insight\n"
    "This is the most important section. Reason from the actual data passed in: put/call ratio, "
    "institutional COT positioning, confidence level, pillar agreement percentage, days since last signal. "
    "Identify the gap between what different market participants are doing and what the data actually says. "
    "Use the 95% vs 5% framing but make it specific to today's conditions — not a generic template. "
    "The core message must always distinguish between urgency as a feeling and edge as a condition. "
    "Tone: calm, direct, slightly uncomfortable — like a trading psychologist naming something real. "
    "Never vague, never encouraging, never generic. Must feel fresh and specific regardless of market regime.\n\n"
    "What To Watch\n"
    "Exactly 2-3 bullet points. Each names one specific measurable event and the exact threshold that "
    "would change the current signal if crossed. No general themes. "
    "Start each bullet directly with the trigger — no introduction line, no label, no dash before the text. "
    "Never combine two triggers into one bullet.\n\n"
    "Hard rules: "
    "maximum 160 words total across all three sections. "
    "No regime context. No historical duration comparisons. No word count in output. "
    "Never give directional bias or trade recommendations. "
    "Translate all pillar scores into qualitative language such as 'strongly bullish', 'mildly bearish', or 'neutral'. "
    "A score of zero is neutral — never say zero or any number. "
    "Never include raw COT contract counts or put/call scores — translate to plain language only."
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
        inst = formatted_data.get('institutional', {})

        vix = macro.get('vix', {})
        vxn = macro.get('vxn', {})
        fg = macro.get('fear_greed', {})

        # Derive trading context metrics from daily history
        history = self._load_daily_history()

        # Consecutive days confidence below 40% (today first)
        days_below_40 = 1 if confidence < 40 else 0
        if days_below_40:
            for entry in history:
                if entry['confidence'] < 40:
                    days_below_40 += 1
                else:
                    break

        # Days since last tradeable signal (bias != Neutral, confidence >= 20%)
        if bias != 'Neutral' and confidence >= 20:
            days_since_signal = 0
        else:
            days_since_signal = 1
            for entry in history:
                if entry['bias'] != 'Neutral' and entry['confidence'] >= 20:
                    break
                days_since_signal += 1

        # Pillar agreement percentage (pillar signals aligned with current bias)
        pillar_signals = bias_score.get('pillar_signals', [])
        if bias != 'Neutral' and pillar_signals:
            agreeing = pillar_signals.count(bias.lower())
            agreement_pct = round(agreeing / len(pillar_signals) * 100)
        else:
            agreement_pct = 0

        # Quiet week mode from prop firm recommendation metadata
        prop_rec = bias_score.get('recommendation_prop')
        quiet_week = prop_rec.get('quiet_week', False) if isinstance(prop_rec, dict) else False

        geo_items = geo.get('news_items', [])
        geo_headlines = [
            item.get('headline', item.get('title', ''))
            for item in geo_items[:5]
            if item.get('headline') or item.get('title')
        ]

        econ_events = econ.get('events', [])
        upcoming = [e for e in econ_events if e.get('result') in ('pending', 'speech')][:5]

        lines = [
            "CURRENT STATE:",
            f"  Bias: {bias} | Confidence: {confidence}%",
            "",
            "TRADING CONTEXT:",
            f"  Days confidence below 40% (consecutive): {days_below_40}",
            f"  Days since last tradeable signal: {days_since_signal}",
            f"  Pillar agreement: {agreement_pct}% of active pillars aligned with bias",
            f"  Quiet week mode (Prop Firm): {'ACTIVE — EC weighted at 15%' if quiet_week else 'inactive — standard weights'}",
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

        # Institutional COT
        nq_fut = inst.get('nq_futures', {})
        es_fut = inst.get('es_futures', {})
        if inst.get('status') != 'unavailable' and (nq_fut or es_fut):
            nq_pct = nq_fut.get('net_pct', 0)
            nq_dir = nq_fut.get('direction', 'unknown')
            es_pct = es_fut.get('net_pct', 0)
            es_dir = es_fut.get('direction', 'unknown')
            lines += [
                "INSTITUTIONAL POSITIONING (COT):",
                f"  NQ: institutions net {nq_dir} ({nq_pct:.1f}% net)",
                f"  ES: institutions net {es_dir} ({es_pct:.1f}% net)",
                "",
            ]
        else:
            lines += ["INSTITUTIONAL POSITIONING (COT): unavailable", ""]

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
