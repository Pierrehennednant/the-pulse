import json
from datetime import datetime
import pytz
from config import PILLAR_WEIGHTS, COT_SCORE_CAP, TIMEZONE
from pipelines.institutional import institutional_pipeline
from utils.logger import pulse_logger

NEUTRAL_MODE_FILE = "/data/neutral_mode.json"


# Neutral-day sizing is locked to Quarter (Pierre, Oct 15 2026: per-day rule,
# no rolling window, no automatic promotion). neutral_mode.json is no longer
# read here, so it cannot force Half. Half on a Neutral day would need an
# explicit per-day override for that session; none exists yet.
# NEUTRAL_MODE_FILE stays defined only because ui/dashboard.py imports it.

class BiasCalculator:
    def compute(self, formatted_data, bias_threshold=0.5, ec_weight=None):
        total_score = 0.0
        total_weight_used = 0.0   # sum of base_weight for genuinely-reporting pillars only
        pillar_contributions = {}
        active_pillars = 0
        pillar_signals = []
        pillar_signal_map = {}    # config_key → signal, for weight-aware agreement
        active_pillar_weights = {}  # config_key → effective weight, for active pillars only
        # Weights come from config (single source of truth). ec_weight, when
        # supplied by pulse_refresh, overrides EC for this cycle: 15 in a quiet
        # week, 30 in a standard week. It applies to BOTH the score base and
        # the agreement denominator, because both read this same dict.
        weights = dict(PILLAR_WEIGHTS)
        if ec_weight is not None:
            weights['economic_calendar'] = ec_weight

        weight_map = {
            'economic': 'economic_calendar',
            'geopolitical': 'geopolitical',
            'institutional': 'institutional',
            'macro': 'macro_sentiment'
        }

        for data_key, config_key in weight_map.items():
            pillar_data = formatted_data.get(data_key, {})
            score = pillar_data.get('pillar_score', 0)
            # COT pillar score clamp — cap from config before weighting.
            if config_key == 'institutional' and score is not None:
                score = max(-COT_SCORE_CAP, min(COT_SCORE_CAP, score))
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

                    # Freshness guard (Monday AND Friday after 3:30 PM): full
                    # weight only when the data is the confirmed-newest CFTC
                    # release (report_date == expected Tuesday as-of date AND
                    # fetched after the release). Otherwise hold the 55% floor.
                    if decay_factor == 1.0:
                        confirmed = (pillar_data.get('status') != 'stale'
                                     and institutional_pipeline._cache_confirmed_newest(pillar_data, now))
                        pulse_logger.log(
                            f"COT freshness | report_date={pillar_data.get('report_date')} | "
                            f"confirmed_newest={confirmed} | factor={'1.0' if confirmed else '0.55'}")
                        if not confirmed:
                            decay_factor = 0.55

                    weight = weight * decay_factor
                    if decay_factor < 1.0:
                        pulse_logger.log(f"📉 COT decay applied — {int(decay_factor * 100)}% of {base_weight}% base = {weight:.1f}% effective")
                    elif today == 0:
                        pulse_logger.log(f"✅ COT weight — Monday, full {base_weight}% effective")

            # True silence — no fetch succeeded, no cached fallback available this
            # cycle (data_formatter.py forces status='unavailable' for a None
            # pipeline result; a main.py exception-fallback {} lands here too via
            # this method's own .get default below). A silent pillar casts no
            # vote in either direction: its contribution is 0 AND its weight is
            # excluded from the renormalization base below — dragging final_score
            # toward Neutral would itself be an unearned directional statement.
            # A genuinely-computed 0.0 (status='live'/'cached'/'stale'/'pinned_only')
            # is real signal and keeps contributing/counting exactly as before.
            is_silent = status == 'unavailable'

            if not is_silent:
                contribution = score * (weight / 100)
                total_score += contribution
                # Full base_weight, NOT the decay-adjusted `weight` — so COT decay
                # still permanently dampens its own contribution ceiling instead of
                # being invisibly refunded by renormalization inflating everyone
                # else (including itself) back up on a decay day.
                total_weight_used += base_weight
            else:
                contribution = 0.0

            if not is_silent:
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
                'status': status,
                'silent': is_silent,
            }

        # Renormalize against only genuinely-reporting pillars' base weight. If
        # every pillar is silent this cycle, final_score is 0.0 (Neutral) rather
        # than a division by zero.
        #
        # Agreement override — when two or more non-neutral pillars agree
        # strongly enough to clear the bias bar on their own weight, a lone
        # dissenting institutional (COT) vote is excluded from the score and
        # the weight base. Breadth beats a single loud dissenter: the lean is
        # real even without it. Fires only when the agreeing pillars alone
        # clear the bar — "strongly" is measured, not felt. This replaces the
        # earlier staleness-based veto: the trigger is agreement strength,
        # not COT decay, so it works on Mondays with fresh COT too.
        _agreement_override_fired = False
        _inst = pillar_contributions.get('institutional', {})
        if not _inst.get('silent', True) and _inst.get('weight', 0) > 0:
            _inst_sig = pillar_signal_map.get('institutional')
            if _inst_sig in ('bullish', 'bearish'):
                for _bias, _sig in (('Bullish', 'bullish'), ('Bearish', 'bearish')):
                    if _inst_sig == _sig:
                        continue
                    _agreeing = [k for k, s in pillar_signal_map.items()
                                 if k != 'institutional' and s == _sig]
                    if len(_agreeing) >= 2:
                        _agree_contrib = sum(
                            pillar_contributions[k]['raw_score']
                            * (pillar_contributions[k]['weight'] / 100)
                            for k in _agreeing)
                        _agree_base = sum(
                            pillar_contributions[k]['base_weight'] for k in _agreeing)
                        _standalone = _agree_contrib / (_agree_base / 100) if _agree_base else 0.0
                        _clears = (_standalone >= bias_threshold) if _bias == 'Bullish' \
                            else (_standalone <= -bias_threshold)
                        if _clears:
                            # Exclude the dissenter: exact unrounded contribution
                            # out of the score, base weight out of the base.
                            total_score -= _inst['raw_score'] * (_inst['weight'] / 100)
                            total_weight_used -= _inst['base_weight']
                            active_pillars -= 1
                            active_pillar_weights.pop('institutional', None)
                            pillar_signal_map.pop('institutional', None)
                            pillar_signals = [pillar_signal_map[k] for k in
                                              ('economic_calendar', 'geopolitical',
                                               'institutional', 'macro_sentiment')
                                              if k in pillar_signal_map]
                            _inst['contribution'] = 0.0
                            _inst['override_excluded'] = True
                            _agreement_override_fired = True
                            pulse_logger.log(
                                f"🗳️ Agreement override — {_bias} {_sig} stands on "
                                f"{len(_agreeing)} pillars (standalone {_standalone:+.3f}); "
                                f"institutional dissent excluded")
                        break
        if total_weight_used > 0:
            final_score = round(total_score / (total_weight_used / 100), 3)
        else:
            final_score = 0.0

        if final_score >= bias_threshold:
            bias = 'Bullish'
            bias_emoji = '🟢'
        elif final_score <= -bias_threshold:
            bias = 'Bearish'
            bias_emoji = '🔴'
        else:
            bias = 'Neutral'
            bias_emoji = '🟡'

        agreement_pct = 0.0
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

            if confidence >= 75:
                confidence_label = 'High Confidence'
                confidence_color = 'green'
            elif confidence >= 60:
                confidence_label = 'Moderate Confidence'
                confidence_color = 'yellow'
            else:
                confidence_label = 'Low Conviction'
                confidence_color = 'orange'

        # Hard Neutral override — below 55% confidence forces Neutral regardless of score
        low_conviction_override = confidence < 55 and bias != 'Neutral'
        if low_conviction_override:
            bias = 'Neutral'
            bias_emoji = '🟡'

        # Trading Directive — confidence-based only
        if low_conviction_override:
            directive = "🟡 Very low edge — don't force anything."
            directive_color = "#f39c12"
        elif active_pillars == 0:
            directive = "⚫ No Trade – No Data"
            directive_color = "#7a8fa8"
        elif bias == 'Neutral':
            directive = "🟡 Very low edge — don't force anything."
            directive_color = "#f39c12"
        elif confidence >= 70:
            if bias == 'Bearish':
                directive = "🔴 Bearish — strong edge."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — strong edge."
                directive_color = "#2ecc71"
        elif confidence >= 60:
            if bias == 'Bearish':
                directive = "🔴 Bearish — moderate edge."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — moderate edge."
                directive_color = "#2ecc71"
        else:
            if bias == 'Bearish':
                directive = "🔴 Bearish — weak edge."
                directive_color = "#e74c3c"
            else:
                directive = "🟢 Bullish — weak edge."
                directive_color = "#2ecc71"

        # Gauge needle — signed confidence (direction x magnitude), not final_score.
        # Neutral has no direction (sign 0), so the needle centers rather than leaning
        # either way. Range -100..+100 maps to gauge_value 0..100 (50 = center/no signal).
        direction_sign = 1 if bias == 'Bullish' else (-1 if bias == 'Bearish' else 0)
        signed_confidence = confidence * direction_sign

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
            # Weight-based agreement (decay-adjusted weights, override
            # applied). The ONLY agreement number other modules may show.
            'agreement_pct': round(agreement_pct, 4),
            'gauge_value': int((signed_confidence + 100) / 2),
            'directive': directive,
            'directive_color': directive_color,
        }

        _raw_log = f" (raw {raw_confidence_pct}%)" if active_pillars > 0 else ""
        pulse_logger.log(f"📊 Bias: {bias_emoji} {bias} | Confidence: {confidence}%{_raw_log} ({confidence_label}) | Active Pillars: {active_pillars}/4")
        return result

bias_calculator = BiasCalculator()
