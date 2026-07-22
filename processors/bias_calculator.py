from datetime import datetime
import pytz
from config import PILLAR_WEIGHTS, TIMEZONE
from utils.logger import pulse_logger

class BiasCalculator:
    def compute(self, formatted_data, bias_threshold=0.5):
        total_score = 0.0
        pillar_contributions = {}
        active_pillars = 0
        pillar_signals = []
        pillar_signal_map = {}    # config_key → signal, for weight-aware agreement
        active_pillar_weights = {}  # config_key → effective weight, for active pillars only
        weights = PILLAR_WEIGHTS

        weight_map = {
            'economic': 'economic_calendar',
            'geopolitical': 'geopolitical',
            'institutional': 'institutional',
            'macro': 'macro_sentiment'
        }

        for data_key, config_key in weight_map.items():
            pillar_data = formatted_data.get(data_key, {})
            score = pillar_data.get('pillar_score', 0)
            base_weight = weights.get(config_key, 0)
            weight = base_weight
            status = pillar_data.get('status', 'unavailable')

            # COT decay — stale weekly data weighs less as the week progresses
            if config_key == 'institutional':
                now = datetime.now(pytz.timezone(TIMEZONE))
                today = now.weekday()
                if today in [5, 6]:  # Weekend — COT data maximally stale, contribute nothing
                    weight = 0
                    pulse_logger.log("📉 COT decay — weekend, institutional weight zeroed")
                else:
                    if today == 4:  # Friday — 55% floor until 3:30 PM EST; full weight once COT posts
                        cutoff = now.replace(hour=15, minute=30, second=0, microsecond=0)
                        decay_factor = 1.0 if now >= cutoff else 0.55
                    else:
                        # Mon=0: 100%, Tue=1: 85%, Wed=2: 70%, Thu=3: 55%
                        decay_factor = {0: 1.0, 1: 0.85, 2: 0.70, 3: 0.55}[today]

                    # Monday freshness guard — full weight only if this cycle's data has landed.
                    # If the Monday re-fetch hasn't succeeded (status=stale or timestamp still
                    # from last Friday), hold the 55% floor rather than granting 100% to data
                    # the decay schedule already discounted last week.
                    if today == 0 and decay_factor == 1.0:
                        cot_status = pillar_data.get('status', '')
                        cot_ts = pillar_data.get('timestamp', '')
                        is_stale_on_monday = cot_status == 'stale'
                        if not is_stale_on_monday and cot_ts:
                            try:
                                cached_dt = datetime.fromisoformat(cot_ts)
                                if cached_dt.tzinfo is None:
                                    cached_dt = cached_dt.replace(tzinfo=pytz.timezone(TIMEZONE))
                                is_stale_on_monday = cached_dt.weekday() == 4  # Friday-stamped = last week
                            except Exception:
                                pass
                        if is_stale_on_monday:
                            decay_factor = 0.55
                            pulse_logger.log(f"📉 COT freshness guard — Monday but data is last week's ({cot_status or 'Friday timestamp'}), holding 55% floor")

                    weight = weight * decay_factor
                    if decay_factor < 1.0:
                        pulse_logger.log(f"📉 COT decay applied — {int(decay_factor * 100)}% of {base_weight}% base = {weight:.1f}% effective")
                    elif today == 0:
                        pulse_logger.log(f"✅ COT weight — Monday, full {base_weight}% effective")

            contribution = score * (weight / 100)
            total_score += contribution

            if status not in ['unavailable'] and score != 0:
                active_pillars += 1
                active_pillar_weights[config_key] = weight
                if score > 0.15:
                    sig = 'bullish'
                elif score < -0.15:
                    sig = 'bearish'
                else:
                    sig = 'neutral'
                pillar_signals.append(sig)
                pillar_signal_map[config_key] = sig

            pillar_contributions[config_key] = {
                'raw_score': score,
                'base_weight': base_weight,
                'weight': weight,
                'contribution': round(contribution, 3),
                'status': status
            }

        final_score = round(total_score, 3)

        if final_score >= bias_threshold:
            bias = 'Bullish'
            bias_emoji = '🟢'
        elif final_score <= -bias_threshold:
            bias = 'Bearish'
            bias_emoji = '🔴'
        else:
            bias = 'Neutral'
            bias_emoji = '🟡'

        if active_pillars == 0:
            confidence = 0
            confidence_label = 'No Data'
            confidence_color = 'gray'
        else:
            total_active_weight = sum(active_pillar_weights.values())
            if total_active_weight > 0:
                agreeing_weight = sum(
                    w for key, w in active_pillar_weights.items()
                    if (bias == 'Bullish' and pillar_signal_map[key] == 'bullish')
                    or (bias == 'Bearish' and pillar_signal_map[key] == 'bearish')
                    or (bias == 'Neutral' and pillar_signal_map[key] == 'neutral')
                )
                agreement_pct = agreeing_weight / total_active_weight
            else:
                agreement_pct = 0.0

            excess = max(0.0, abs(final_score) - bias_threshold)
            score_strength = min(excess / max(2.0 - bias_threshold, 0.01), 1.0)
            raw_conf = agreement_pct * 0.6 + score_strength * 0.4

            # Ceiling: max raw_conf achievable when agreement_pct=1.0 and final_score=1.0
            # max_score_strength = (1.0 - threshold) / (2.0 - threshold)
            max_ss = (1.0 - bias_threshold) / max(2.0 - bias_threshold, 0.01)
            ceiling = 0.6 + 0.4 * max_ss
            raw_confidence_pct = int(raw_conf * 100)
            confidence = min(int(raw_conf / ceiling * 100), 100)

            if confidence >= 80:
                confidence_label = 'High Confidence'
                confidence_color = 'green'
            elif confidence >= 65:
                confidence_label = 'Moderate Confidence'
                confidence_color = 'yellow'
            else:
                confidence_label = 'Low Conviction'
                confidence_color = 'orange'

        # Hard Neutral override — below 60% confidence forces Neutral regardless of score
        low_conviction_override = confidence < 60 and bias != 'Neutral'
        if low_conviction_override:
            bias = 'Neutral'
            bias_emoji = '🟡'

        # Trading Directive — confidence-based only
        if low_conviction_override:
            directive = "⚫ No Trade – Low Conviction."
            directive_color = "#7a8fa8"
        elif bias == 'Neutral':
            directive = "🟡 Neutral — Sit out."
            directive_color = "#f39c12"
        elif confidence >= 80:
            if bias == 'Bearish':
                directive = "🔴 Bearish — Half size — scale to Full on confirmation."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — Half size — scale to Full on confirmation."
                directive_color = "#2ecc71"
        elif confidence >= 65:
            if bias == 'Bearish':
                directive = "🔴 Bearish — Half size — look for confirmation before scaling to Full."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — Half size — look for confirmation before scaling to Full."
                directive_color = "#2ecc71"
        else:
            if bias == 'Bearish':
                directive = "🔴 Bearish — Quarter size."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — Quarter size."
                directive_color = "#2ecc71"

        result = {
            'final_score': final_score,
            'bias': bias,
            'bias_emoji': bias_emoji,
            'confidence': confidence,
            'confidence_label': confidence_label,
            'confidence_color': confidence_color,
            'threshold_warning': None,
            'active_pillars': active_pillars,
            'pillar_signals': pillar_signals,
            'pillar_contributions': pillar_contributions,
            'gauge_value': int((final_score + 2) / 4 * 100),
            'directive': directive,
            'directive_color': directive_color,
        }

        _raw_log = f" (raw {raw_confidence_pct}%)" if active_pillars > 0 else ""
        pulse_logger.log(f"📊 Bias: {bias_emoji} {bias} | Confidence: {confidence}%{_raw_log} ({confidence_label}) | Active Pillars: {active_pillars}/4")
        return result

bias_calculator = BiasCalculator()
