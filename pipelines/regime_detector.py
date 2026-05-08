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
            'vix_elevated_count': 0
        }

    def _save(self, state):
        atomic_write_json(REGIME_FILE, state)

    def detect(self, geo_data, macro_data):
        state = self._load()
        current_regime = state.get('regime', 'escalation')
        calm_days_count = state.get('calm_days_count', 0)
        vix_elevated_count = state.get('vix_elevated_count', 0)

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

        # --- Escalation signals ---
        vix_escalation = vix_elevated_count >= 2
        geo_escalation = high_uncertainty_count >= 2
        any_escalation = vix_escalation or geo_escalation

        # --- Expansion conditions ---
        expansion_conditions_met = (
            vix_value < 18 and
            avg_uncertainty < 40 and
            high_uncertainty_count == 0
        )

        # --- Hysteresis logic ---
        if any_escalation:
            calm_days_count = 0
            new_regime = 'escalation'
        elif expansion_conditions_met:
            calm_days_count += 1
            if calm_days_count >= 3:
                new_regime = 'expansion'
            else:
                new_regime = current_regime
        else:
            new_regime = current_regime

        # --- Log regime changes ---
        if new_regime != current_regime:
            if new_regime == 'expansion':
                pulse_logger.log("🟢 Regime shift: EXPANSION — calm conditions confirmed 3 days")
            else:
                pulse_logger.log("🔴 Regime shift: ESCALATION — elevated risk detected")

        state['regime'] = new_regime
        state['calm_days_count'] = calm_days_count
        state['vix_elevated_count'] = vix_elevated_count
        self._save(state)

        return new_regime

regime_detector = RegimeDetector()
