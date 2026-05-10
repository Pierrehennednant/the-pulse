# The Pulse — Claude Code Project Context

## What This Is

Pre-market macro regime trading dashboard for NQ and ES futures. Synthesizes macro signals into a directional bias (bullish/bearish/neutral) and session size recommendation. Edited via Claude Code in GitHub Codespaces.

- **GitHub:** github.com/Pierrehennednant/the-pulse
- **Hosting:** Railway — auto-deploys on every push to `main`
- **Stack:** Python / Flask, no frontend framework

## Terminal Rules

- **Never use `&&` to chain commands — always use `;`**
- Run `python -m py_compile <file>` on every changed Python file before pushing
- Railway auto-deploys from `main`; a broken push affects production immediately
- Check Railway logs after every push

## Four Pillars

Weights are regime-dependent:

| Pillar | Escalation | Expansion | Pipeline |
|---|---|---|---|
| Economic Calendar | 40% | 45% | `pipelines/economic_calendar.py` |
| Geopolitical | 30% | 20% | `pipelines/geopolitical.py` — Claude Haiku |
| Institutional (COT) | 20% | 25% | `pipelines/institutional.py` |
| Macro Sentiment | 10% | 10% | `pipelines/macro_sentiment.py` |

**COT decay** — institutional weight is reduced progressively Mon–Thu based on staleness since Friday's release:

| Day | Effective weight (escalation) |
|---|---|
| Friday | 20% (full) |
| Monday | 16% (80%) |
| Tuesday | 12% (60%) |
| Wednesday | 8% (40%) |
| Thursday | 4% (20%) |
| Weekend | 0% |

Final bias assembled in `processors/bias_calculator.py`. Dashboard rendered in `ui/dashboard.py` + `ui/templates/`.

## Regime Detector (`pipelines/regime_detector.py`)

Determines whether the current environment is `escalation` (default) or `expansion`. State persisted in `/data/regime.json`.

**Expansion** — requires ALL THREE conditions for 3 consecutive cycles:
- VIX < 18
- Average geo uncertainty (last 5 articles) < 40
- Zero articles scoring 70+ uncertainty

**Escalation flip** — requires 2 consecutive cycles of EITHER:
- VIX > 22
- 2+ articles scoring 70+ uncertainty

A single spike does NOT flip regime. Tracked via `escalation_streak_count`. Expansion state is live-validated each cycle — if escalation conditions are detected while in expansion, regime is immediately overridden.

Persisted state keys: `regime`, `calm_days_count`, `vix_elevated_count`, `escalation_streak_count`.

## Stability Score (`compute_stability` in `regime_detector.py`)

Runtime-only score 0–100. Never persisted.

```
(1 - min(vix / 30, 1)) * 40
+ (1 - min(avg_uncertainty / 100, 1)) * 40
+ min(calm_days_count * 5, 20)
```

Affects:
- Confidence ±5 points (micro-adjustment)
- Normal size threshold: 55% standard, 62% when stability < 30

## Confidence Formula (`processors/bias_calculator.py`)

1. **Base:** `(agreement_pct * 0.6 + score_strength * 0.4) * 100`
2. **Persistence bonus:** escalation +1pt per calm day (max 8), expansion +2pt per calm day (max 15)
3. **Uncertainty dampening:** if escalation and 2+ high-uncertainty articles, × 0.85
4. **Stability micro-adjustment:** `int((stability_score - 50) / 10)`, capped ±5
5. **Hard cap:** 95%

## Recommendation Engine (`pipelines/recommendation.py`)

- Normal size requires confidence ≥ 55% (or ≥ 62% when stability score < 30)
- Below 20% confidence: no recommendation shown
- Regime consistency check reads `/data/snapshots/daily/` only
  - Escalation regime: 3 consecutive days at 55%+ avg confidence
  - Expansion regime: 2 consecutive days at 55%+ avg confidence
  - Neutral days **pause** the streak (do not break it)
  - Opposite direction **breaks** the streak

## Snapshot System

- **Live snapshots:** every 5 minutes → `/data/snapshots/` — keep last 50
- **Daily closing snapshots:** 4:00–4:05 PM EST → `/data/snapshots/daily/` — keep last 10
- Consistency check reads daily snapshots only
- `os.path.isfile()` filter applied everywhere to exclude the `daily/` subdirectory from live snapshot listing/pruning

## COT History

Each Friday after a successful COT fetch, `_append_history()` appends to `/data/cot_history.json`:
```json
{ "timestamp": "...", "nq_net_pct": 0.0, "nq_direction": "bullish", "es_net_pct": 0.0, "es_direction": "bearish" }
```
Last 6 weekly entries kept. Dashboard displays a 3-week trend indicator (↑ Building / ↓ Unwinding / → Stable) for NQ and ES.

## Pinned Stories — Three-Layer Cleanup

1. **Layer 1:** Live feed supersedes a pin on the same story (Haiku SAME/DIFFERENT classification)
2. **Layer 2:** Pin vs pin dedup — newest wins
3. **Layer 3:** 48-hour expiry — last resort

## Key Data Files

| File | Purpose | Retention |
|---|---|---|
| `/data/permanent_manual_inputs.json` | Manual actual values for economic events | 7 days |
| `/data/permanent_cot.json` | Current COT reading (NQ + ES positions) | Until next Friday |
| `/data/cot_history.json` | Weekly COT snapshots for trend indicator | Last 6 weeks |
| `/data/regime.json` | Current regime + streak counters | Persisted |
| `/data/gemini_classifications.json` | Haiku story classification cache | 48-hour expiry |
| `/data/pinned_stories.json` | Pinned geopolitical articles | 48-hour expiry |
| `/data/size_mode.json` | Quarter / Normal size mode toggle | Persisted |
| `/data/snapshots/` | Live 5-minute bias snapshots | Last 50 |
| `/data/snapshots/daily/` | Daily 4 PM closing snapshots | Last 10 |

**Current size mode:** Quarter

## AI Usage

Geopolitical pipeline uses **Claude Haiku** (`claude-haiku-4-5-20251001`) for story classification and pin comparison. Do not swap models without verifying prompt/cost fit.

## Project Layout

```
main.py                        Orchestrator — run_pulse(), scheduler, Flask startup
config.py                      Env vars, pillar weight constants (both regimes)
Procfile                       Railway process definition
requirements.txt               Pinned dependencies (pin Anthropic version)
pipelines/
  economic_calendar.py         Economic calendar pillar + manual input integration
  geopolitical.py              Geopolitical pillar — Haiku classification
  institutional.py             COT fetcher + decay + history tracking
  macro_sentiment.py           VIX, VXN, Fear & Greed
  regime_detector.py           Escalation/expansion regime with hysteresis
  recommendation.py            Size recommendation engine
  manual_input.py              Manual actual value persistence (7-day)
  weekly_summary.py            Weekly narrative summary
processors/
  bias_calculator.py           Weighted bias + confidence + directives
  data_formatter.py            Standardizes pillar outputs, injects cot_history
  snapshot_generator.py        Save/load/prune live and daily snapshots
ui/
  dashboard.py                 Flask routes (API + HTML)
  templates/dashboard.html     Single-page dashboard
utils/
  cache.py                     JSON cache with TTL + delete()
  file_lock.py                 atomic_write_json
  logger.py                    pulse_logger
  error_handler.py             Structured error handling
  retry.py                     fetch_with_retry
data/                          Runtime JSON state (Railway persistent volume)
```

## Deployment

Railway reads `Procfile` and `requirements.txt`. Environment variables (`FRED_API_KEY`, `THENEWS_API_KEY`, Anthropic key) are set in the Railway dashboard. Do not commit secrets. After any dependency change, verify `requirements.txt` is updated and pinned.
