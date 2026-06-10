# The Pulse — Claude Code Project Context

## What This Is

Pre-market macro trading dashboard for NQ and ES futures. Synthesizes macro signals into a directional bias (bullish/bearish/neutral) and session size recommendation. Edited via Claude Code in GitHub Codespaces.

- **GitHub:** github.com/Pierrehennednant/the-pulse
- **Hosting:** Railway — auto-deploys on every push to `main`
- **Stack:** Python / Flask, no frontend framework

## Terminal Rules

- **Never use `&&` to chain commands — always use `;`**
- Run `python -m py_compile <file>` on every changed Python file before pushing
- Railway auto-deploys from `main`; a broken push affects production immediately
- Check Railway logs after every push

## Four Pillars

Weights are **fixed** (no regime switching):

| Pillar | Weight | Pipeline |
|---|---|---|
| Economic Calendar | 30% | `pipelines/economic_calendar.py` |
| Geopolitical | 25% | `pipelines/geopolitical.py` — Claude Haiku |
| Institutional (COT) | 25% | `pipelines/institutional.py` |
| Macro Sentiment | 20% | `pipelines/macro_sentiment.py` |

Final bias assembled in `processors/bias_calculator.py`. Dashboard rendered in `ui/dashboard.py` + `ui/templates/`.

**Active pillar threshold:** ±0.15 — pillars scoring below this are treated as neutral for contribution purposes.

## COT Decay

Institutional weight is reduced progressively Mon–Thu based on staleness since Friday's release. 55% is the floor until new data arrives.

| Day / Time | Decay factor |
|---|---|
| Friday before 3:30 PM EST | 55% (floor — awaiting new release) |
| Friday after 3:30 PM EST | 100% (new data posted) |
| Monday | 100% |
| Tuesday | 85% |
| Wednesday | 70% |
| Thursday | 55% |
| Weekend | 0% |

Each Friday after a successful COT fetch, `_append_history()` appends to `/data/cot_history.json`:
```json
{ "timestamp": "...", "nq_net_pct": 0.0, "nq_direction": "bullish", "es_net_pct": 0.0, "es_direction": "bearish" }
```
Last 6 weekly entries kept. Dashboard displays a 3-week trend indicator (↑ Building / ↓ Unwinding / → Stable) for NQ and ES.

## Confidence Formula (`processors/bias_calculator.py`)

```
confidence = int((agreement_pct * 0.6 + score_strength * 0.4) * 100)
```

No persistence bonuses, no uncertainty dampening, no stability micro-adjustments. Simple formula only.

## EC Magnitude-Weighted Scoring

Economic calendar events are scored by relative deviation from forecast:

| Relative deviation | Impact magnitude |
|---|---|
| ≤ 20% | Mild ±0.40 |
| 21–50% | Moderate ±0.63 |
| > 50% | Strong ±0.88 |

Non-numerical events (speeches) are manually tagged via the dashboard. Blocked events are excluded via the EC blocklist.

## Macro Sentiment Signal Thresholds

Five-level granular classification for each indicator:

**VIX**

| Signal | Threshold |
|---|---|
| Strongly Bullish | < 15.0 |
| Mildly Bullish | 15.0–16.9 |
| Neutral | 17.0–19.9 |
| Mildly Bearish | 20.0–24.9 |
| Strongly Bearish | ≥ 25.0 |

**VXN**

| Signal | Threshold |
|---|---|
| Strongly Bullish | < 18.0 |
| Mildly Bullish | 18.0–19.9 |
| Neutral | 20.0–24.9 |
| Mildly Bearish | 25.0–27.9 |
| Strongly Bearish | ≥ 28.0 |

**Fear & Greed (CNN, 0–100)**

| Signal | Threshold |
|---|---|
| Strongly Bullish | ≥ 75 |
| Mildly Bullish | 55–74 |
| Neutral | 45–54 |
| Mildly Bearish | 35–44 |
| Strongly Bearish | < 35 |

Score is rounded (not truncated) to match CNN's own display rounding.

## Live Mode Thresholds (`pipelines/recommendation.py`)

| Setting | Value |
|---|---|
| Bias threshold | ± 0.50 |
| Confidence to show card | 20% |
| Confidence for quarter entry | 55% |
| Consistency streak | 2 consecutive days |
| Neutral days | Pause the streak (do not break it) |
| Opposite direction | Breaks the streak |

## Prop Firm Mode Thresholds (`pipelines/recommendation.py`)

| Setting | Value |
|---|---|
| Bias threshold | ± 0.40 standard / ± 0.38 quiet week |
| Confidence to show card | 42% |
| Confidence for quarter entry | 42% |
| Consistency streak | 1 day |
| VIX hard limit | ≤ 22 (unchanged from Live) |
| High uncertainty block | Active (unchanged from Live) |

**Dynamic bias threshold (Prop Firm only):** Evaluated once at the start of each ISO week using the Forex Factory calendar. Persisted to `/data/prop_firm_weekly_threshold.json` for the entire week — does not change mid-week.

- 0 or 1 red folder events this week → bias threshold ± 0.38
- 2+ red folder events this week → bias threshold ± 0.40

Logged once per week in Railway logs:
```
📊 Prop Firm — new week: bias threshold ±0.38 (1 red folder event scheduled this week)
```

## Snapshot System

- **Live snapshots:** every 5 minutes → `/data/snapshots/` — keep last 50
- **Daily closing snapshots:** 4:00–4:05 PM EST → `/data/snapshots/daily/` — keep last 10
- Consistency check reads daily snapshots only
- `os.path.isfile()` filter applied everywhere to exclude the `daily/` subdirectory from live snapshot listing/pruning

## Pinned Stories — Three-Layer Cleanup

1. **Layer 1:** Live feed supersedes a pin on the same story (Haiku SAME/DIFFERENT classification)
2. **Layer 2:** Pin vs pin dedup — newest wins
3. **Layer 3:** 48-hour expiry — last resort

## EC Blocklist

- Persistent per-session blocklist at `/data/ec_blocklist.json`
- Clears on Sunday weekly reset
- Events blocked by title + scheduled date identifier

## AI Lens (`pipelines/ai_lens.py`)

- Powered by **Grok** (`grok-4.20-0309-reasoning`) via `GROK_API_KEY`
- Generates once daily after 8:30 AM EST; re-fires on manual EC input submission
- Uses last 10 daily snapshots as historical context
- Cached to `/data/ai_lens_cache.json`

## AI Usage

- **Geopolitical pipeline:** Claude Haiku (`claude-haiku-4-5-20251001`) for story classification and pin comparison. Do not swap models without verifying prompt/cost fit.
- **AI Lens:** Grok (`grok-4.20-0309-reasoning`). Do not swap without verifying cost/output fit.

## Data Sources

| Source | Data | Cache fallback |
|---|---|---|
| FRED API (`FRED_API_KEY`) | VIX, VXN (end-of-day closing) | `/data/vix_cache.json`, `/data/vxn_cache.json` (default 20.0) |
| CNN via `fear_greed` library | Fear & Greed index | `/data/fear_greed_cache.json` |
| Forex Factory JSON (`thisweek.json`) | Economic calendar — red folder events only | In-memory cache |
| TheNewsAPI + Claude Haiku | Geopolitical news classification | `/data/gemini_classifications.json` (48h expiry) |
| CFTC weekly | COT positioning (NQ + ES) | `/data/permanent_cot.json` (until next Friday) |

## Security

- Password protection via `DASHBOARD_PASSWORD` env var
- 7-day session cookies via `SECRET_KEY` env var
- Do not commit secrets — all env vars set in Railway dashboard

## Key Env Vars

`FRED_API_KEY`, `GROK_API_KEY`, `ANTHROPIC_API_KEY`, `THENEWS_API_KEY`, `DASHBOARD_PASSWORD`, `SECRET_KEY`

## Key Data Files

| File | Purpose | Retention |
|---|---|---|
| `/data/permanent_manual_inputs.json` | Manual actual values for economic events | 7 days |
| `/data/permanent_cot.json` | Current COT reading (NQ + ES positions) | Until next Friday |
| `/data/cot_history.json` | Weekly COT snapshots for trend indicator | Last 6 weeks |
| `/data/gemini_classifications.json` | Haiku story classification cache | 48-hour expiry |
| `/data/pinned_stories.json` | Pinned geopolitical articles | 48-hour expiry |
| `/data/ec_blocklist.json` | EC event blocklist | Clears Sunday |
| `/data/size_mode.json` | Quarter / Normal size mode toggle | Persisted |
| `/data/prop_firm_weekly_threshold.json` | Prop Firm bias threshold for current ISO week | Weekly |
| `/data/ai_lens_cache.json` | AI Lens daily narrative cache | Daily |
| `/data/snapshots/` | Live 5-minute bias snapshots | Last 50 |
| `/data/snapshots/daily/` | Daily 4 PM closing snapshots | Last 10 |
| `/data/vix_cache.json` | VIX fallback cache | Until next fetch |
| `/data/vxn_cache.json` | VXN fallback cache | Until next fetch |
| `/data/fear_greed_cache.json` | Fear & Greed fallback cache | Until next fetch |

## Project Layout

```
main.py                        Orchestrator — run_pulse(), scheduler, Flask startup
config.py                      Env vars, pillar weight constants
Procfile                       Railway process definition
requirements.txt               Pinned dependencies (pin Anthropic version)
pipelines/
  economic_calendar.py         Economic calendar pillar + manual input integration
  geopolitical.py              Geopolitical pillar — Haiku classification
  institutional.py             COT fetcher + decay + history tracking
  macro_sentiment.py           VIX, VXN, Fear & Greed
  recommendation.py            Size recommendation engine (Live + Prop Firm)
  manual_input.py              Manual actual value persistence (7-day)
  ai_lens.py                   AI Lens daily narrative (Grok)
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

Railway reads `Procfile` and `requirements.txt`. Environment variables are set in the Railway dashboard. Do not commit secrets. After any dependency change, verify `requirements.txt` is updated and pinned.
