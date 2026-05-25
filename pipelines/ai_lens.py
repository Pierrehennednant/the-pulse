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
PCR_CACHE_FILE = '/data/pcr_cache.json'
_GROK_BASE_URL = 'https://api.x.ai/v1'
_GROK_MODEL = 'grok-4.20-0309-reasoning'
_CBOE_PCR_URL = 'https://www.cboe.com/data/putcallratio/'

_SYSTEM_PROMPT = (
    "You are a market regime analyst for a systematic futures trader who trades NQ and ES. "
    "Write a concise AI Lens analysis following these four priorities in order: "
    "1) Why we're here — explain clearly why the market is in its current regime based on the data provided. "
    "2) Psychological insight — use the put/call ratio and institutional COT positioning as the primary basis "
    "for describing the emotional and nervous system state of the market right now, "
    "what the 95% of traders are likely doing, and what the disciplined 5% should be doing instead. "
    "If put/call ratio data is unavailable, acknowledge uncertainty rather than defaulting to generic assumptions. "
    "3) Regime context — state how many days we have been in this regime and reference how long similar "
    "regimes have historically lasted and what typically ends them. "
    "4) What to watch — give exactly two or three specific concrete triggers that would break the current regime. "
    "Hard rules: maximum 180 words, tone must be calm sharp and psychologically insightful, "
    "never give directional bias or trade recommendations, always reinforce thinking like the 5% not the 95%. "
    "Never reference raw pillar score numbers in your analysis — translate all scores into qualitative language "
    "such as 'strongly bullish', 'mildly bearish', or 'neutral'. "
    "A score of zero means neutral and must always be described as neutral, never as zero or any number. "
    "Never include raw put/call ratio numbers or COT contract counts in your output — "
    "translate everything into plain language only. "
    "Never use the word 'cap' anywhere in your analysis. "
    "Never include a word count or any reference to word count in your output."
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

    def _load_pcr_cache(self):
        try:
            if os.path.exists(PCR_CACHE_FILE):
                with open(PCR_CACHE_FILE) as f:
                    return json.load(f)
        except Exception:
            pass
        return None

    def _save_pcr_cache(self, pcr):
        try:
            atomic_write_json(PCR_CACHE_FILE, {
                'timestamp': datetime.now(self.timezone).isoformat(),
                'pcr': pcr,
            })
        except Exception as e:
            pulse_logger.log(f"⚠️ PCR cache write failed: {e}", level="WARNING")

    def _parse_pcr_from_json(self, data, depth=0):
        """Recursively search a JSON structure for a put/call ratio under a 'total'-named key."""
        if depth > 8:
            return None
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(k, str) and 'total' in k.lower():
                    try:
                        pcr = float(v)
                        if 0.3 <= pcr <= 4.0:
                            return pcr
                    except (TypeError, ValueError):
                        pass
                result = self._parse_pcr_from_json(v, depth + 1)
                if result is not None:
                    return result
        elif isinstance(data, list):
            for item in data[:50]:
                result = self._parse_pcr_from_json(item, depth + 1)
                if result is not None:
                    return result
        return None

    def _fetch_pcr(self):
        """Fetch CBOE total put/call ratio. Caches result. Returns float or None."""
        try:
            resp = requests.get(
                _CBOE_PCR_URL,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; ThePulse/1.0)'},
                timeout=15,
            )
            resp.raise_for_status()
            html = resp.text

            # Strategy 1: Next.js __NEXT_DATA__ JSON blob
            nd_match = re.search(
                r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL
            )
            if nd_match:
                try:
                    pcr = self._parse_pcr_from_json(json.loads(nd_match.group(1)))
                    if pcr is not None:
                        self._save_pcr_cache(pcr)
                        pulse_logger.log(f"✓ PCR — CBOE (JSON): {pcr}")
                        return pcr
                except Exception:
                    pass

            # Strategy 2: HTML table — Date | Total | Index | Equity
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
            for row in reversed(rows):
                cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL)
                cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
                if len(cells) >= 2:
                    try:
                        pcr = float(cells[1])
                        if 0.3 <= pcr <= 4.0:
                            self._save_pcr_cache(pcr)
                            pulse_logger.log(f"✓ PCR — CBOE (table): {pcr}")
                            return pcr
                    except (ValueError, TypeError):
                        pass

            pulse_logger.log("⚠️ PCR — could not parse CBOE response", level="WARNING")
        except Exception as e:
            pulse_logger.log(f"⚠️ PCR — CBOE fetch failed: {e}", level="WARNING")

        # Fall back to cache
        cached = self._load_pcr_cache()
        if cached and cached.get('pcr') is not None:
            pulse_logger.log(f"⚠️ PCR — using cache: {cached['pcr']}", level="WARNING")
            return cached['pcr']

        pulse_logger.log("⚠️ PCR — no data available", level="WARNING")
        return None

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

    def _build_messages(self, bias_score, formatted_data, pcr_value):
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

        # Put/call ratio block
        if pcr_value is not None:
            lines += [
                "MARKET PSYCHOLOGY INDICATORS:",
                f"  CBOE Total Put/Call Ratio: {pcr_value:.2f} "
                f"(context: above 1.0 = elevated hedging/fear, below 0.7 = complacency/low hedging)",
                "",
            ]
        else:
            lines += ["MARKET PSYCHOLOGY INDICATORS: put/call ratio unavailable", ""]

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

        # Fetch PCR once daily alongside generation
        pcr_value = self._fetch_pcr()

        try:
            messages = self._build_messages(bias_score, formatted_data, pcr_value)
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
