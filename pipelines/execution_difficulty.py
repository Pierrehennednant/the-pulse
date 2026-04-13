import json
import os
from datetime import datetime, timezone, timedelta
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
                json.dump({"narratives": {}, "headline_log": []}, f)

    def _load_memory(self):
        try:
            with open(self.memory_file, 'r') as f:
                return json.load(f)
        except:
            return {"narratives": {}, "headline_log": []}

    def _save_memory(self, data):
        try:
            with open(self.memory_file, 'w') as f:
                json.dump(data, f, indent=2)
        except:
            pass

    # ── Input 1: VIX/VXN Hard Numbers ──────────────────────────────────────
    def compute_vix_score(self, vix_value, vxn_value):
        """Measures current volatility level. Higher = worse conditions."""
        vix_score = 0
        vxn_score = 0

        if vix_value:
            if vix_value >= 35:
                vix_score = 3
            elif vix_value >= 28:
                vix_score = 2.5
            elif vix_value >= 22:
                vix_score = 1.5
            elif vix_value >= 18:
                vix_score = 0.5
            else:
                vix_score = 0

        if vxn_value:
            if vxn_value >= 40:
                vxn_score = 3
            elif vxn_value >= 32:
                vxn_score = 2.5
            elif vxn_value >= 25:
                vxn_score = 1.5
            elif vxn_value >= 20:
                vxn_score = 0.5
            else:
                vxn_score = 0

        # VXN weighted slightly higher — it's NQ-specific
        return round((vix_score * 0.45) + (vxn_score * 0.55), 2)

    # ── Input 2: VIX Trajectory ─────────────────────────────────────────────
    def compute_vix_trajectory_score(self, vix_change, vxn_change):
        """Measures how fast volatility is moving. Rising fast = worse conditions."""
        score = 0

        # VIX change
        if vix_change is not None:
            if vix_change >= 5:
                score += 2.0
            elif vix_change >= 3:
                score += 1.5
            elif vix_change >= 1.5:
                score += 1.0
            elif vix_change >= 0.5:
                score += 0.5
            elif vix_change <= -2:
                score -= 0.5  # Falling VIX = improving conditions

        # VXN change
        if vxn_change is not None:
            if vxn_change >= 5:
                score += 2.0
            elif vxn_change >= 3:
                score += 1.5
            elif vxn_change >= 1.5:
                score += 1.0
            elif vxn_change >= 0.5:
                score += 0.5
            elif vxn_change <= -2:
                score -= 0.5

        return round(max(0, min(3.0, score / 2)), 2)

    # ── Input 3: Confluence Detection ───────────────────────────────────────
    def compute_confluence_score(self, flags):
        """Multiple high-priority flags firing simultaneously = fragmented conditions."""
        if not flags:
            return 0

        high_priority = [f for f in flags if f.get('priority', 0) >= 80]
        medium_priority = [f for f in flags if 65 <= f.get('priority', 0) < 80]

        if len(high_priority) >= 3:
            return 3.0  # 3+ critical flags = extreme conditions
        elif len(high_priority) == 2:
            return 2.0  # 2 critical flags = elevated
        elif len(high_priority) == 1 and len(medium_priority) >= 2:
            return 1.5  # 1 critical + 2 medium = elevated
        elif len(high_priority) == 1:
            return 1.0
        elif len(medium_priority) >= 3:
            return 1.0
        elif len(medium_priority) >= 2:
            return 0.5
        else:
            return 0

    # ── Input 4: News Velocity ───────────────────────────────────────────────
    def compute_news_velocity_score(self, geo_data):
        """Headlines firing too fast = market in reaction mode, conditions fragmented."""
        memory = self._load_memory()
        headline_log = memory.get("headline_log", [])
        now = datetime.now(timezone.utc)

        # Add current headlines to log
        items = geo_data.get('news_items', []) if geo_data else []
        for item in items:
            published = item.get('published_at', '')
            if published:
                try:
                    dt = datetime.fromisoformat(published.replace('Z', '+00:00'))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    headline_log.append({
                        'headline': item.get('headline', '')[:60],
                        'timestamp': dt.isoformat()
                    })
                except:
                    pass

        # Keep only last 2 hours of headlines
        cutoff = now - timedelta(hours=2)
        headline_log = [
            h for h in headline_log
            if datetime.fromisoformat(h['timestamp']).replace(tzinfo=timezone.utc) > cutoff
        ]

        # Count headlines in last 30 minutes
        cutoff_30 = now - timedelta(minutes=30)
        recent_count = sum(
            1 for h in headline_log
            if datetime.fromisoformat(h['timestamp']).replace(tzinfo=timezone.utc) > cutoff_30
        )

        # Save updated log
        memory['headline_log'] = headline_log[-200:]  # cap at 200 entries
        self._save_memory(memory)

        # Score based on velocity
        if recent_count >= 15:
            return 3.0  # Extreme velocity
        elif recent_count >= 10:
            return 2.0  # High velocity
        elif recent_count >= 6:
            return 1.0  # Moderate velocity
        elif recent_count >= 3:
            return 0.5  # Low velocity
        else:
            return 0.0

    # ── Input 5: Economic Event Proximity ───────────────────────────────────
    def compute_event_proximity_score(self, econ_data):
        """Market freezes/spikes in 15 min window before major releases."""
        if not econ_data:
            return 0

        now_est = datetime.now(self.timezone)
        events = econ_data.get('events', [])
        score = 0

        for event in events:
            if event.get('result') not in ['pending']:
                continue
            if event.get('is_speech'):
                continue

            time_str = event.get('time_est', '')
            try:
                # Parse event time
                event_dt = datetime.strptime(time_str, '%a %b %d, %I:%M %p EST')
                event_dt = event_dt.replace(
                    year=now_est.year,
                    tzinfo=self.timezone
                )
                minutes_until = (event_dt - now_est).total_seconds() / 60

                if 0 <= minutes_until <= 15:
                    impact = event.get('impact', '').lower()
                    if impact == 'high':
                        score = max(score, 3.0)  # Red folder within 15 min = extreme
                    elif impact == 'medium':
                        score = max(score, 1.5)
            except:
                pass

        return score

    # ── Input 6: Time of Day ─────────────────────────────────────────────────
    def compute_time_of_day_score(self):
        """Certain windows are structurally more chaotic regardless of news."""
        now_est = datetime.now(self.timezone)
        hour = now_est.hour
        minute = now_est.minute
        time_decimal = hour + minute / 60

        # Market open 9:25-9:45 AM — most chaotic window
        if 9.41 <= time_decimal <= 9.75:
            return 2.0

        # Fed announcement window 2:00-2:15 PM
        if 14.0 <= time_decimal <= 14.25:
            return 2.5

        # Power hour 3:45-4:00 PM
        if 15.75 <= time_decimal <= 16.0:
            return 1.5

        # Pre-market 4:00-9:25 AM — lower liquidity
        if 4.0 <= time_decimal < 9.41:
            return 0.5

        # Lunch chop 11:30 AM-1:00 PM — low volume, choppy
        if 11.5 <= time_decimal <= 13.0:
            return 0.5

        # Normal session
        return 0.0

    # ── Input 7: Day of Week ─────────────────────────────────────────────────
    def compute_day_of_week_score(self):
        """Monday opens and OpEx Fridays are structurally unpredictable."""
        now_est = datetime.now(self.timezone)
        weekday = now_est.weekday()  # 0=Monday, 4=Friday
        day_of_month = now_est.day

        # Monday — gap risk, overnight news digest, unpredictable open
        if weekday == 0:
            return 1.0

        # Third Friday of month = OpEx — extreme pin risk and volatility
        if weekday == 4:
            # Check if third Friday
            first_day = now_est.replace(day=1)
            first_friday = (4 - first_day.weekday()) % 7 + 1
            third_friday = first_friday + 14
            if day_of_month == third_friday:
                return 2.5  # OpEx Friday = elevated conditions
            return 0.5  # Regular Friday = slightly elevated

        return 0.0

    # ── Narrative Memory ─────────────────────────────────────────────────────
    def update_narrative_memory(self, flags):
        """Track which narratives are intensifying, stable, or fading."""
        memory = self._load_memory()
        narratives = memory.get("narratives", {})
        now = datetime.now(timezone.utc).isoformat()
        today = datetime.now(self.timezone).strftime('%Y-%m-%d')

        for key in narratives:
            narratives[key]['seen_today'] = False

        for flag in flags:
            key = flag.get('flag_type') or flag['title'][:40]
            priority = flag.get('priority', 0)

            if key in narratives:
                existing = narratives[key]
                existing['last_seen'] = now
                existing['seen_today'] = True
                existing['peak_priority'] = max(existing.get('peak_priority', 0), priority)
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
                    'direction': flag.get('predicted_impact', 'unknown')
                }

        stale_keys = []
        for key in narratives:
            if not narratives[key].get('seen_today', False):
                narratives[key]['status'] = 'fading'
                narratives[key]['consecutive_days'] = max(0, narratives[key].get('consecutive_days', 1) - 1)
                if narratives[key]['consecutive_days'] == 0:
                    stale_keys.append(key)
        for key in stale_keys:
            del narratives[key]

        memory['narratives'] = narratives
        self._save_memory(memory)
        return narratives

    def compute_flag_score(self, flags, narratives):
        """Flag priority + narrative momentum = conditions score."""
        if not flags:
            return 0

        flag_score = 0
        for flag in flags[:3]:
            priority = flag.get('priority', 0)
            key = flag.get('flag_type') or flag['title'][:40]
            narrative = narratives.get(key, {})
            status = narrative.get('status', 'emerging')
            days = narrative.get('consecutive_days', 1)

            base = priority / 100 * 3

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

    # ── Master Compute ───────────────────────────────────────────────────────
    def compute(self, macro_data, geo_data, econ_data=None):
        """Compute EDI regime using all 7 condition inputs."""
        try:
            vix = macro_data.get('vix', {}) if macro_data else {}
            vxn = macro_data.get('vxn', {}) if macro_data else {}
            vix_value = vix.get('value', 0) or 0
            vxn_value = vxn.get('value', 0) or 0
            vix_change = vix.get('change', 0) or 0
            vxn_change = vxn.get('change', 0) or 0
            flags = geo_data.get('active_flags', []) if geo_data else []

            # Update narrative memory
            narratives = self.update_narrative_memory(flags)

            # Compute all 7 inputs
            s1_vix = self.compute_vix_score(vix_value, vxn_value)
            s2_trajectory = self.compute_vix_trajectory_score(vix_change, vxn_change)
            s3_confluence = self.compute_confluence_score(flags)
            s4_velocity = self.compute_news_velocity_score(geo_data)
            s5_proximity = self.compute_event_proximity_score(econ_data)
            s6_time = self.compute_time_of_day_score()
            s7_day = self.compute_day_of_week_score()
            s_flags = self.compute_flag_score(flags, narratives)

            # Weighted combination
            # VIX/trajectory = 35%, flags/confluence = 30%, velocity = 15%, time/day = 10%, proximity = 10%
            edi_score = round(
                (s1_vix * 0.20) +
                (s2_trajectory * 0.15) +
                (s_flags * 0.20) +
                (s3_confluence * 0.10) +
                (s4_velocity * 0.15) +
                (s5_proximity * 0.10) +
                (s6_time * 0.05) +
                (s7_day * 0.05),
                2
            )

            # Hard overrides — certain conditions always force a regime regardless of score
            if s5_proximity >= 3.0:
                edi_score = max(edi_score, 2.0)  # Red folder in 15 min always = at least Extreme
            if s3_confluence >= 3.0:
                edi_score = max(edi_score, 1.8)  # 3+ critical flags always = at least Extreme
            if s6_time >= 2.5:
                edi_score = max(edi_score, 1.8)  # Fed announcement window always = at least Extreme

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

            # Build reason string — only include what's actually contributing
            reasons = []
            if s1_vix >= 1.5:
                reasons.append(f"VIX {vix_value} / VXN {vxn_value} elevated")
            if s2_trajectory >= 1.0:
                reasons.append(f"VIX rising fast (+{vix_change})")
            if s3_confluence >= 2.0:
                reasons.append(f"{len([f for f in flags if f.get('priority',0) >= 80])} critical flags active simultaneously")
            if s4_velocity >= 1.0:
                reasons.append("High headline velocity — market in reaction mode")
            if s5_proximity >= 1.5:
                reasons.append("Major economic release within 15 minutes")
            if s6_time >= 2.0:
                reasons.append("Structurally volatile time window")
            if s7_day >= 2.0:
                reasons.append("OpEx Friday — pin risk elevated")
            elif s7_day >= 1.0:
                reasons.append("Monday open — gap risk present")

            # Add active narrative reasons
            active = {k: v for k, v in narratives.items() if v.get('seen_today')}
            for key, n in list(active.items())[:2]:
                status = n.get('status', '')
                days = n.get('consecutive_days', 1)
                if status == 'intensifying':
                    reasons.append(f"{key} intensifying (day {days})")
                elif status == 'emerging':
                    reasons.append(f"{key} emerging")

            reason = ', '.join(reasons) if reasons else 'Normal market conditions'

            # Narrative summary for dashboard display
            narrative_summary = []
            for key, n in narratives.items():
                if n.get('seen_today') or n.get('status') == 'fading':
                    narrative_summary.append({
                        'name': key,
                        'status': n['status'],
                        'days': n.get('consecutive_days', 0),
                        'direction': n.get('direction', 'unknown')
                    })

            result = {
                'regime': regime,
                'regime_emoji': regime_emoji,
                'regime_color': regime_color,
                'edi_score': edi_score,
                'reason': reason,
                'vix_value': vix_value,
                'vxn_value': vxn_value,
                'inputs': {
                    'vix_score': s1_vix,
                    'trajectory_score': s2_trajectory,
                    'confluence_score': s3_confluence,
                    'velocity_score': s4_velocity,
                    'proximity_score': s5_proximity,
                    'time_score': s6_time,
                    'day_score': s7_day,
                    'flag_score': s_flags
                },
                'narratives': narrative_summary
            }

            pulse_logger.log(f"📊 EDI: {regime_emoji} {regime} | Score: {edi_score} | {reason[:80]}")
            return result

        except Exception as e:
            pulse_logger.log(f"⚠️ EDI compute failed: {e}", level="WARNING")
            return {
                'regime': 'Normal',
                'regime_emoji': '✅',
                'regime_color': '#2ecc71',
                'edi_score': 0,
                'reason': 'EDI unavailable',
                'narratives': [],
                'inputs': {}
            }

execution_difficulty_index = ExecutionDifficultyIndex()
