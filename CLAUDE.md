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

| Day / Time | Decay factor | Log |
|---|---|---|
| Friday before 3:30 PM EST | 55% (floor — awaiting new release) | `📉 COT decay applied` |
| Friday after 3:30 PM EST | 100% (new data posted) | — |
| Monday (fresh — fetch succeeded) | 100% | `✅ COT weight — Monday, full 25% effective` |
| Monday (stale — fetch failed) | 55% (freshness guard) | `📉 COT freshness guard — Monday but data is last week's` |
| Tuesday | 85% | `📉 COT decay applied` |
| Wednesday | 70% | `📉 COT decay applied` |
| Thursday | 55% | `📉 COT decay applied` |
| Weekend | 0% | `📉 COT decay — weekend` |

**Monday freshness guard:** Full 100% weight on Monday is conditional on the Monday re-fetch having succeeded. If `institutional.py` returns `status='stale'` or the cached timestamp is from Friday (last week's data), `bias_calculator.py` demotes to the 55% floor — the same level as Friday pre-3:30 PM. A successful Monday fetch (`status='live'`, Monday timestamp) restores full weight immediately.

**Cache hit fix:** The Mon–Thu cache path now recomputes `pillar_score` from `nq_futures.score` and `es_futures.score` if the field is missing or zero, preventing institutional from being silently excluded from bias calculation. Score is logged on every cache hit.

## Confidence Formula (`processors/bias_calculator.py`)

```
raw_conf = agreement_pct * 0.6 + score_strength * 0.4
ceiling  = 0.6 + 0.4 * (1.0 - bias_threshold) / (2.0 - bias_threshold)
confidence = min(int(raw_conf / ceiling * 100), 100)
```

**Ceiling normalization:** `raw_conf` has a mathematical cap below 100% because `score_strength` maxes at `(1.0 - threshold) / (2.0 - threshold)` (assuming pillar scores sum to 1.0 at full agreement). The ceiling for live mode (0.50) is ~73%; for prop firm (0.33) it is ~76%. Dividing by the ceiling maps the achievable range to 0–100 so all existing bands operate on a full scale.

**Ceiling derivation:** `max_score_strength = (1.0 - bias_threshold) / (2.0 - bias_threshold)` → `ceiling = 0.6 + 0.4 × max_score_strength`.

Both raw and normalized values are logged on every refresh for sanity-checking: `Confidence: 83% (raw 61%)`.

No persistence bonuses, no uncertainty dampening, no stability micro-adjustments. Simple formula only.

## EC Magnitude-Weighted Scoring

Economic calendar events are scored by relative deviation from forecast with polarity-aware sign correction.

**Magnitude bands** (relative deviation = abs(actual − forecast) / abs(forecast)):

| Relative deviation | Impact magnitude |
|---|---|
| ≤ 20% | Mild ±0.40 |
| 21–50% | Moderate ±0.63 |
| > 50% | Strong ±0.88 |

**Polarity map** — applied as `final_score = magnitude × POLARITY[event] × sign(surprise)`:

| Event | Polarity | Meaning |
|---|---|---|
| Non-Farm Employment Change | +1 | Beat = bullish |
| ADP Non-Farm Employment Change | +1 | Beat = bullish |
| Unemployment Rate | −1 | Beat (higher) = bearish |
| Average Hourly Earnings m/m | −1 | Beat (higher) = bearish (inflation) |
| Core CPI m/m, CPI m/m, CPI y/y | −1 | Beat = bearish (inflation) |
| Core PPI m/m, PPI m/m | −1 | Beat = bearish (inflation) |
| Core PCE m/m | −1 | Beat = bearish (inflation) |
| GDP q/q | +1 | Beat = bullish |
| ISM Manufacturing PMI, ISM Services PMI | +1 | Beat = bullish |
| Retail Sales m/m, Core Retail Sales m/m | +1 | Beat = bullish |

Events not in the POLARITY map log a warning and fall back to the `market_impact` direction. Speeches are manually tagged via the dashboard. Unknown events use flat base score ±0.40. Blocked events are excluded via the EC blocklist.

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
| Confidence to show card | 60% |
| Confidence for quarter entry | 60%–64% |
| Confidence for half entry (cautious) | 65%–79% — "look for confirmation before scaling to Full" |
| Confidence for half entry (aggressive) | ≥ 80% — "scale to Full on confirmation" |
| Below 60% | Neutral forced — "No Trade – Low Conviction" directive |

## Prop Firm Mode Thresholds (`pipelines/recommendation.py`)

| Setting | Value |
|---|---|
| Bias threshold | ± 0.33 standard / ± 0.30 quiet week |
| Confidence to show card | 60% |
| Confidence for quarter entry | 60%–64% |
| Confidence for half entry (cautious) | 65%–79% |
| Confidence for half entry (aggressive) | ≥ 80% |
| Pillar alignment | ≥ 45% of total week weight must agree with bias |

**Quiet week mode (Prop Firm only):** Evaluated once at the start of each ISO week. Counts red folder **days** (not individual events — a day with multiple red folder events counts as 1 red folder day). Persisted to `/data/prop_firm_weekly_threshold.json` for the entire week — does not change mid-week.

- 0 or 1 red folder days → quiet week: bias threshold ± 0.30, EC weight drops from 30% to 15%, total weight 85%, pillar alignment threshold 45% of 85% = 38.25%
- 2+ red folder days → standard week: bias threshold ± 0.33, EC weight 30%, total weight 100%, pillar alignment threshold 45%

Logged once per week in Railway logs:
```
🔇 Quiet week active — 1 red folder day — EC 15%, bias ±0.30
📅 Standard week — 3 red folder days — EC 30%, bias ±0.33
```

## Snapshot System

- **Live snapshots:** every 5 minutes → `/data/snapshots/` — keep last 50
- **Daily closing snapshots:** 4:00–4:05 PM EST → `/data/snapshots/daily/` — keep last 10
- `os.path.isfile()` filter applied everywhere to exclude the `daily/` subdirectory from live snapshot listing/pruning

## Pinned Stories — Three-Layer Cleanup

1. **Layer 1:** Live feed supersedes a pin on the same story (Haiku SAME/DIFFERENT classification)
2. **Layer 2:** Pin vs pin dedup — newest wins
3. **Layer 3:** 48-hour expiry — last resort

## Geopolitical — Haiku Contextual Tiering

Haiku assigns tier, direction, confidence, and reasoning for every geo article as part of the batch classification call. Tier determines base score and weight in the weighted average.

| Tier | Base score | Weight | Use case |
|---|---|---|---|
| Tier 1 | ±1.7 | 4× | Active war/escalation between major powers, nuclear threats, major confirmed peace deals/ceasefires, credible major supply disruptions (e.g. Hormuz closure) |
| Tier 2 | ±0.75 | 2× | Significant troop buildups, major diplomatic breakdowns, new meaningful sanctions, credible energy market threats |
| Tier 3 | ±0.35 | 1× | Minor diplomatic noise, corporate geopolitical news, speculative/secondary headlines |

**Scoring formula:**
- Haiku path: `article_score = tier_base × haiku_confidence`
- Keyword fallback path: `article_score = tier_base × confidence × flag_multiplier` (flag_multiplier = `1 + 0.2 × priority/100` when priority ≥ 65)
- Final score: `weighted_sum / total_weight`, clipped to [-2.0, +2.0]

**Key rules:**
- Prioritize context over keywords — "ceasefire" or "deal" doesn't auto-assign Tier 1
- Default to lower tier when uncertain
- Oil/Energy: falling oil from peace deal/de-escalation → Bullish; from demand destruction/recession → Bearish
- Fallback to keyword-based tier if Haiku returns malformed JSON or API call fails
- Per-article tier source logged (`Geo tier (Haiku)` vs `Geo tier (keyword fallback)`)
- Aggregate ratio logged per scoring run: `📊 Geo tier source ratio — Haiku: X/Y (Z%) | Keyword fallback: N/Y`

**Tier backfill:** On each pipeline run, active articles with cached classifications missing a `tier` field are backfilled via Haiku one article at a time (not batched). Only articles in the current active set are backfilled — historical cache entries are left as-is.

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

- **Geopolitical pipeline:** Claude Haiku (`claude-haiku-4-5-20251001`) for story classification (relevance, direction, tier, confidence, reasoning, summary), pin comparison, and tier backfill. Do not swap models without verifying prompt/cost fit.
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
| `/data/gemini_classifications.json` | Haiku story classification cache | 48-hour expiry |
| `/data/pinned_stories.json` | Pinned geopolitical articles | 48-hour expiry |
| `/data/ec_blocklist.json` | EC event blocklist | Clears Sunday |
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
  data_formatter.py            Standardizes pillar outputs
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
