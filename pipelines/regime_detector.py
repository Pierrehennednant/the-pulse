import json
import os
from utils.file_lock import atomic_write_json
from utils.logger import pulse_logger

REGIME_FILE = '/data/regime.json'

class RegimeDetector:
    def _load(self):
        try:
            if os.path.exists(REGIME_FILE):
                with open(REGIME_FILE, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return {
            'regime': 'escalation',
            'calm_days_count': 0,
            'vix_elevated_count': 0,
            'escalation_streak_count': 0
        }

    def _save(self, state):
        atomic_write_json(REGIME_FILE, state)

    def compute_stability(self, vix, avg_uncertainty, calm_days_count):
        """Compute a runtime-only regime stability score (0-100). Not persisted."""
        stability = (
            (1 - min(vix / 30, 1)) * 40 +
            (1 - min(avg_uncertainty / 100, 1)) * 40 +
            min(calm_days_count * 5, 20)
        )
        return int(stability)

    def detect(self, geo_data, macro_data):
        state = self._load()
        current_regime = state.get('regime', 'escalation')
        calm_days_count = state.get('calm_days_count', 0)
        vix_elevated_count = state.get('vix_elevated_count', 0)
        escalation_streak_count = state.get('escalation_streak_count', 0)

        # --- Read signals ---
        vix_value = (macro_data or {}).get('vix', {}).get('value', 0) or 0

        items = (geo_data or {}).get('news_items', [])
        uncertainty_scores = [
            item.get('uncertainty_score', 0)
            for item in items
            if item.get('uncertainty_score') is not None
        ]
        last_5_scores = uncertainty_scores[:5]
        avg_uncertainty = (sum(last_5_scores) / len(last_5_scores)) if last_5_scores else 0
        high_uncertainty_count = sum(1 for s in uncertainty_scores if s >= 70)

        # --- VIX consecutive tracking ---
        if vix_value > 22:
            vix_elevated_count += 1
        else:
            vix_elevated_count = 0

        # --- Live condition signals ---
        expansion_conditions_met = (
            vix_value < 18 and
            avg_uncertainty < 40 and
            high_uncertainty_count == 0
        )
        escalation_conditions_met = vix_value > 22 or high_uncertainty_count >= 2

        # --- State validation: live override when stored regime conflicts ---
        if current_regime == 'expansion' and escalation_conditions_met:
            pulse_logger.log("⚠️ Regime override: expansion state invalidated — escalation conditions active", level="WARNING")
            current_regime = 'escalation'

        # --- Persistence logic ---
        new_regime = current_regime

        if expansion_conditions_met:
            calm_days_count += 1
            escalation_streak_count = 0
        elif escalation_conditions_met:
            escalation_streak_count += 1
            if escalation_streak_count >= 2:
                new_regime = 'escalation'
                calm_days_count = 0
                escalation_streak_count = 0

        # --- Expansion promotion ---
        if calm_days_count >= 3 and new_regime == 'escalation':
            new_regime = 'expansion'

        # --- Log regime changes ---
        if new_regime != current_regime:
            if new_regime == 'expansion':
                pulse_logger.log("🟢 Regime shift: EXPANSION — calm conditions confirmed 3 days")
            else:
                pulse_logger.log("🔴 Regime shift: ESCALATION — elevated risk detected")

        state['regime'] = new_regime
        state['calm_days_count'] = calm_days_count
        state['vix_elevated_count'] = vix_elevated_count
        state['escalation_streak_count'] = escalation_streak_count
        self._save(state)

        stability_score = self.compute_stability(vix_value, avg_uncertainty, calm_days_count)

        return {
            'regime': new_regime,
            'calm_days_count': calm_days_count,
            'high_uncertainty_count': high_uncertainty_count,
            'stability_score': stability_score
        }

regime_detector = RegimeDetector()
