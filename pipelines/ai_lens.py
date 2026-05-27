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
    "Write like a sharp trader, not an analyst. No academic language, no passive constructions, no filler. "
    "Total output must be under 180 words across all four sections. "
    "Produce exactly four sections in this order, using these exact headers:\n\n"
    "Why We're Here\n"
    "2-3 sentences maximum. State directly why the market is in this regime. Punchy and specific.\n\n"
    "Psychological Insight\n"
    "Lead with what the put/call options sentiment and institutional COT positioning are actually showing. "
    "Then contrast what the 95% are doing versus what the disciplined 5% are doing. "
    "If put/call data is unavailable, say so plainly — do not fill with generic assumptions.\n\n"
    "Regime Context\n"
    "One short paragraph. State the day count, the historical range for this type of regime, "
    "and where we are in that window.\n\n"
    "What to Watch\n"
    "Exactly 2-3 bullet points. Each bullet is one specific trigger that would break this regime. "
    "Start each bullet directly with the trigger — no introduction line, no label, no dash before the text. "
    "Never combine two triggers into one bullet.\n\n"
    "Hard rules: "
    "never give directional bias or trade recommendations, always frame the contrast as 5% vs 95%. "
    "Translate all pillar scores into qualitative language such as 'strongly bullish', 'mildly bearish', or 'neutral'. "
    "A score of zero is neutral — never say zero or any number. "
    "Never include raw put/call scores or COT contract counts — translate to plain language only. "
    "Never use the word 'cap'. "
    "Never include a word count in your output."
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

    def _cot_trend(self, history, field):
        """Return 'building', 'unwinding', or 'stable' from last 3 weeks of COT history."""
        values = [h.get(field) for h in history[:3] if h.get(field) is not None]
        if len(values) < 2:
            return 'unknown'
        delta = values[0] - values[-1]
        if delta > 2.0:
            return 'building'
        elif delta < -2.0:
            return 'unwinding'
        return 'stable'

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

        # Put/call options sentiment — sourced from CNN F&G component (5-day avg put/call ratio)
        pc_score = fg.get('put_call_score')
        pc_rating = fg.get('put_call_rating')
        if pc_score is not None:
            lines += [
                "MARKET PSYCHOLOGY INDICATORS:",
                f"  Put/Call Options (5-day avg): {pc_rating} (score: {pc_score}/100)",
                "  Context: low score = elevated put buying / active hedging; high score = call-heavy / complacency",
                "",
            ]
        else:
            lines += ["MARKET PSYCHOLOGY INDICATORS: put/call sentiment unavailable", ""]

        # Institutional COT block — explicit behavioral anchor for psychology section
        nq_fut = inst.get('nq_futures', {})
        es_fut = inst.get('es_futures', {})
        cot_history = inst.get('cot_history', [])
        if inst.get('status') != 'unavailable' and (nq_fut or es_fut):
            nq_net = nq_fut.get('combined_net', 0)
            nq_pct = nq_fut.get('net_pct', 0)
            nq_dir = nq_fut.get('direction', 'unknown')
            nq_trend = self._cot_trend(cot_history, 'nq_net_pct')
            es_net = es_fut.get('combined_net', 0)
            es_pct = es_fut.get('net_pct', 0)
            es_dir = es_fut.get('direction', 'unknown')
            es_trend = self._cot_trend(cot_history, 'es_net_pct')
            lines += [
                "INSTITUTIONAL POSITIONING (COT):",
                f"  NQ: institutions net {nq_dir} at {abs(nq_net):,} contracts ({nq_pct:.1f}% net) — {nq_trend} 3-week trend",
                f"  ES: institutions net {es_dir} at {abs(es_net):,} contracts ({es_pct:.1f}% net) — {es_trend} 3-week trend",
                "",
            ]
        else:
            lines += ["INSTITUTIONAL POSITIONING (COT): unavailable", ""]

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
