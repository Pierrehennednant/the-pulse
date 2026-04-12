import json
import os
from datetime import datetime, timezone
import pytz
from config import TIMEZONE
from utils.logger import pulse_logger

class ExecutionDifficultyIndex:
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self.memory_file = "/data/narrative_memory.json"
        self._ensure_exists()

    def _ensure_exists(self):
        if not os.path.exists('/data'):
            os.makedirs('/data')
        if not os.path.exists(self.memory_file):
            with open(self.memory_file, 'w') as f:
                json.dump({"narratives": {}}, f)

    def _load_memory(self):
        try:
            with open(self.memory_file, 'r') as f:
                return json.load(f)
        except:
            return {"narratives": {}}

    def _save_memory(self, data):
        try:
            with open(self.memory_file, 'w') as f:
                json.dump(data, f, indent=2)
        except:
            pass

    def update_narrative_memory(self, flags):
        """Track which narratives are intensifying, stable, or fading."""
        memory = self._load_memory()
        narratives = memory.get("narratives", {})
        now = datetime.now(timezone.utc).isoformat()
        today = datetime.now(self.timezone).strftime('%Y-%m-%d')

        # Mark all existing narratives as potentially fading
        for key in narratives:
            narratives[key]['seen_today'] = False

        # Update from current flags
        for flag in flags:
            key = flag['flag_type'] or flag['title'][:40]
            priority = flag.get('priority', 0)
            direction = flag.get('predicted_impact', 'neutral')

            if key in narratives:
                existing = narratives[key]
                existing['last_seen'] = now
                existing['seen_today'] = True
                existing['peak_priority'] = max(existing.get('peak_priority', 0), priority)
                existing['direction'] = direction

                # Check if intensifying or stable
                last_days = existing.get('consecutive_days', 1)
                if priority > existing.get('last_priority', priority):
                    existing['status'] = 'intensifying'
                elif last_days >= 3:
                    existing['status'] = 'stable'
                else:
                    existing['status'] = 'emerging'

                existing['consecutive_days'] = last_days + 1 if existing.get('last_date') != today else last_days
                existing['last_priority'] = priority
                existing['last_date'] = today
            else:
                narratives[key] = {
                    'first_seen': now,
                    'last_seen': now,
                    'last_date': today,
                    'seen_today': True,
                    'consecutive_days': 1,
                    'peak_priority': priority,
                    'last_priority': priority,
                    'status': 'emerging',
                    'direction': direction
                }

        # Fade narratives not seen today
        for key in narratives:
            if not narratives[key].get('seen_today', False):
                narratives[key]['status'] = 'fading'
                narratives[key]['consecutive_days'] = max(0, narratives[key].get('consecutive_days', 1) - 1)

        memory['narratives'] = narratives
        self._save_memory(memory)
        return narratives

    def compute_vix_score(self, vix_value, vxn_value):
        """Convert VIX/VXN into EDI component score 0-3."""
        vix_score = 0
        vxn_score = 0

        if vix_value:
            if vix_value >= 30:
                vix_score = 3
            elif vix_value >= 25:
                vix_score = 2
            elif vix_value >= 20:
                vix_score = 1
            else:
                vix_score = 0

        if vxn_value:
            if vxn_value >= 35:
                vxn_score = 3
            elif vxn_value >= 28:
                vxn_score = 2
            elif vxn_value >= 22:
                vxn_score = 1
            else:
                vxn_score = 0

        # VIX weighted 55%, VXN weighted 45% (VXN is NQ-specific)
        return round((vix_score * 0.55) + (vxn_score * 0.45), 2)

    def compute_flag_score(self, flags, narratives):
        """Convert flag priorities + narrative momentum into EDI component score 0-3."""
        if not flags:
            return 0

        top_flags = flags[:3]
        flag_score = 0

        for flag in top_flags:
            priority = flag.get('priority', 0)
            key = flag.get('flag_type') or flag['title'][:40]
            narrative = narratives.get(key, {})
            status = narrative.get('status', 'emerging')
            days = narrative.get('consecutive_days', 1)

            # Base score from priority
            base = priority / 100 * 3

            # Momentum multiplier
            if status == 'intensifying':
                multiplier = 1.3
            elif status == 'stable' and days >= 3:
                multiplier = 1.1
            elif status == 'fading':
                multiplier = 0.6
            else:
                multiplier = 1.0

            flag_score = max(flag_score, min(3.0, base * multiplier))

        return round(flag_score, 2)

    def compute(self, macro_data, geo_data):
        """Compute EDI regime: Normal / Elevated / Extreme."""
        try:
            # Get VIX/VXN values
            vix = macro_data.get('vix', {}) if macro_data else {}
            vxn = macro_data.get('vxn', {}) if macro_data else {}
            vix_value = vix.get('value', 0) or 0
            vxn_value = vxn.get('value', 0) or 0

            # Get flags
            flags = geo_data.get('active_flags', []) if geo_data else []

            # Update narrative memory
            narratives = self.update_narrative_memory(flags)

            # Compute component scores
            volatility_score = self.compute_vix_score(vix_value, vxn_value)
            flag_score = self.compute_flag_score(flags, narratives)

            # Combined EDI score — volatility 60%, flags 40%
            edi_score = round((volatility_score * 0.6) + (flag_score * 0.4), 2)

            # Determine regime
            if edi_score >= 1.8:
                regime = 'Extreme'
                regime_emoji = '🔴'
                regime_color = '#e74c3c'
            elif edi_score >= 0.8:
                regime = 'Elevated'
                regime_emoji = '⚠️'
                regime_color = '#ff8c00'
            else:
                regime = 'Normal'
                regime_emoji = '✅'
                regime_color = '#2ecc71'

            # Build reason string
            reasons = []
            if vix_value >= 20:
                reasons.append(f"VIX at {vix_value}")
            if vxn_value >= 22:
                reasons.append(f"VXN at {vxn_value}")

            active_narratives = {k: v for k, v in narratives.items() if v.get('seen_today')}
            for key, narrative in list(active_narratives.items())[:2]:
                status = narrative.get('status', '')
                days = narrative.get('consecutive_days', 1)
                if status == 'intensifying':
                    reasons.append(f"{key} narrative intensifying (day {days})")
                elif status == 'emerging':
                    reasons.append(f"{key} narrative emerging")
                elif status == 'stable':
                    reasons.append(f"{key} ongoing (day {days})")

            reason = ', '.join(reasons) if reasons else 'Normal market conditions'

            # Narrative summary for display
            narrative_summary = []
            for key, n in narratives.items():
                if n.get('seen_today'):
                    narrative_summary.append({
                        'name': key,
                        'status': n['status'],
                        'days': n['consecutive_days'],
                        'direction': n['direction']
                    })
                elif n.get('status') == 'fading':
                    narrative_summary.append({
                        'name': key,
                        'status': 'fading',
                        'days': 0,
                        'direction': n['direction']
                    })

            result = {
                'regime': regime,
                'regime_emoji': regime_emoji,
                'regime_color': regime_color,
                'edi_score': edi_score,
                'volatility_score': volatility_score,
                'flag_score': flag_score,
                'reason': reason,
                'vix_value': vix_value,
                'vxn_value': vxn_value,
                'narratives': narrative_summary
            }

            pulse_logger.log(f"📊 EDI: {regime_emoji} {regime} | Score: {edi_score} | {reason}")
            return result

        except Exception as e:
            pulse_logger.log(f"⚠️ EDI compute failed: {e}", level="WARNING")
            return {
                'regime': 'Normal',
                'regime_emoji': '✅',
                'regime_color': '#2ecc71',
                'edi_score': 0,
                'reason': 'EDI unavailable',
                'narratives': []
            }

execution_difficulty_index = ExecutionDifficultyIndex()
