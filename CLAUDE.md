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

## VIX/VXN Intraday Pulls (`pipelines/macro_sentiment.py`)

Live VIX/VXN values come from **yfinance** (`^VIX`, `^VXN`) via 4 scheduled slots per
day, not FRED — FRED is now a fallback-only source. Scheduled at **9:40, 9:45, 9:55,
and 10:30 AM ET** (`INTRADAY_SLOTS`), registered in `main.py`'s `run_scheduler()` via
`schedule.every().day.at(slot, TIMEZONE)`.

**Not all 4 slots call yfinance — two are conditional retries:**
- **9:40 (always live):** primary pull, always attempts yfinance.
- **9:45 (conditional):** only calls yfinance if no same-day slot has yet captured
  a real live value (i.e. 9:40 failed). If 9:40 succeeded, 9:45 skips the API call
  entirely and carries the 9:40 value forward via the session-cache fallback path
  — logged as `↻ skipped (live value already captured earlier today)`, and does
  **not** count toward `consecutive_failures`.
- **9:55 (conditional):** same rule — only calls yfinance if both 9:40 and 9:45
  failed to capture a live value.
- **10:30 (always live):** planned refresh, always attempts a fresh pull
  regardless of earlier outcomes, then freezes.

`ALWAYS_LIVE_SLOTS = {"09:40", "10:30"}` in `pipelines/macro_sentiment.py` marks
which slots skip the conditional check. `run_scheduled_pull()` determines
`already_live_today` by scanning the session's slots for any entry with
`source == 'yfinance'`.

**Per-pull retry:** for a slot that does attempt yfinance, 3 attempts total (1
initial + 2 retries), backoff 3s then 5s, before that slot is marked a real failure.

**Validation ("sane" value):** not None, not NaN, and within **5.0–100.0**. Anything
outside this range is treated as a failed pull, not a bad reading.

**Fallback hierarchy, per pull:**
1. yfinance succeeds + validates → used, written to today's session cache with slot,
   timestamp, `source: 'yfinance'`.
2. yfinance fails after retries → most recent successful **same-day** slot is reused
   (`source: 'session-cache (<slot>)'`) — logged with which slot and its original
   timestamp.
3. No successful same-day slot exists yet → FRED's prior-day close is used as
   today's baseline (`source: 'fred-prior-day'`) — logged explicitly as not live data.

**Session cache:** `/data/vix_intraday_session.json`, `/data/vxn_intraday_session.json`
— new files, separate from the existing `/data/vix_cache.json`/`vxn_cache.json` FRED
cache. Resets automatically when the stored date rolls over. Tracks all 4 slots,
`consecutive_failures`, and freeze state.

**UI warning:** `consecutive_failures` only increments on a **real** yfinance attempt
that fails — a conditionally-skipped slot never increments it. After the **2nd
consecutive real failure** (9:40 and 9:45 both actually attempted and failed —
which also means 9:55 will attempt next, since no live value exists yet),
`intraday_warning: true` is set and the dashboard shows "⚠️ Live VIX/VXN data
unavailable — using [fallback source]" in the Macro Sentiment pillar. Not shown
after only the first failure. Clears immediately on the next successful pull.

**Freeze:** immediately after the 10:30 slot resolves (success or fallback), that
value is frozen for the rest of the day (`frozen: true`, `frozen_value`, `frozen_at`).
The regular 5-minute refresh cycle (`fetch_vix()`/`fetch_vxn()`) becomes a passive
reader after this — no further yfinance or FRED calls until the session resets at
the next 9:40 AM ET slot.

**Daily FRED refresh:** a 5th scheduled job, once daily at **4:35 PM ET**
(`daily_fred_refresh()`), independent of the intraday pulls — keeps `/data/vix_cache.json`
/`vxn_cache.json` fresh so the Level-3 fallback never silently goes stale. The existing
3-day staleness exclusion from the macro score (`stale: true` → excluded in
`calculate_score()`) now only applies when the resolved value's source is
`fred-prior-day` and that FRED cache itself is ≥3 days old.

## Live Mode Thresholds (`pipelines/recommendation.py`)

| Setting | Value |
|---|---|
| Bias threshold | ± 0.50 |
| Confidence to show card | 55% |
| Confidence for quarter entry | 55%–59% |
| Confidence for half entry (cautious) | 60%–74% — "look for confirmation before scaling to Full" |
| Confidence for half entry (aggressive) | ≥ 75% — "scale to Full on confirmation" |
| Below 55% | Neutral forced — "No Trade – Low Conviction" directive |

## Prop Firm Mode Thresholds (`pipelines/recommendation.py`)

| Setting | Value |
|---|---|
| Bias threshold | ± 0.33 standard / ± 0.30 quiet week |
| Confidence to show card | 55% |
| Confidence for quarter entry | 55%–59% |
| Confidence for half entry (cautious) | 60%–74% |
| Confidence for half entry (aggressive) | ≥ 75% |
| Pillar alignment | ≥ 45% of total week weight must agree with bias |

**Quiet week mode (Prop Firm only):** Recomputed once per calendar day — on the first genuinely-live Economic Calendar cycle of that day (`status` not `unavailable`/`stale`, `events` non-empty) — then held for the rest of the day. Same-day freshness, not same-cycle (5-minute) freshness: the red-folder-day count changes because of calendar edits (new event added, reclassified impact), not live market movement, so recomputing every 5 minutes would just add noise and risk a false "classification changed" line firing on a transient bad cycle. Counts red folder **days** (not individual events — a day with multiple red folder events counts as 1 red folder day), via the shared `economic_calendar_pipeline._count_red_folder_days()` (excludes `SCORING_EXCLUSIONS`). Persisted to `/data/prop_firm_weekly_threshold.json`, keyed by calendar date. On a cycle where EC data isn't usable, the last computed day's value is held rather than recomputed from empty data.

- 0 or 1 red folder days → quiet week: bias threshold ± 0.30, EC weight drops from 30% to 15%, total weight 85%, pillar alignment threshold 45% of 85% = 38.25%
- 2+ red folder days → standard week: bias threshold ± 0.33, EC weight 30%, total weight 100%, pillar alignment threshold 45%

Logged on the first live cycle of each day:
```
🔇 Quiet week active — 1 red folder day — EC 15%, bias ±0.30
📅 Standard week — 3 red folder days — EC 30%, bias ±0.33
```

If the classification flips relative to the last persisted value (a red folder day appears or disappears and crosses the 0-1 / 2+ boundary between one day's check and the next), an additional loud line fires:
```
⚠️ Week classification changed: Standard → Quiet (EC 30% → 15%, bias ±0.33 → ±0.30) — red folder day removed since last check (2 → 1)
```

## Snapshot System

- **Live snapshots:** every 5 minutes → `/data/snapshots/` — keep last 50
- **Daily closing snapshots:** 4:00–4:05 PM EST → `/data/snapshots/daily/` — keep last 10
- `os.path.isfile()` filter applied everywhere to exclude the `daily/` subdirectory from live snapshot listing/pruning

## Pinned Stories — TTL-Only Eviction

Every article with a valid Haiku (or keyword-fallback) classification contributes to
the Geo pillar score independently — there is no same-story dedup or eviction gate.
The persisted pin store (`/data/pinned_stories.json`) exists purely to bridge scoring
continuity for articles that fall out of the live TheNewsAPI feed; it is uncapped and
uses exact-headline matching only (never a Haiku same-story judgment) to avoid
re-injecting a literal duplicate. The sole eviction mechanism is the 48-hour TTL
(`_pin_is_expired` in `load_pinned_stories()`), measured from the article's
own timestamp (`published_at`, then `timestamp`, then `date`) — never from
`pinned_at`. Pins persist `published_at` when saved. A missing or unparseable
article timestamp fails closed (the pin is dropped) so a card cannot sit past
48 hours of the story itself.

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
| yfinance (`^VIX`, `^VXN`) | VIX, VXN — 4 scheduled intraday pulls/day | `/data/vix_intraday_session.json`, `/data/vxn_intraday_session.json` |
| FRED API (`FRED_API_KEY`) | VIX, VXN prior-day close — Level-3 fallback only, refreshed daily 4:35 PM ET | `/data/vix_cache.json`, `/data/vxn_cache.json` (default 20.0) |
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
| `/data/vix_cache.json` | VIX Level-3 fallback (FRED prior-day close) | Refreshed daily 4:35 PM ET |
| `/data/vxn_cache.json` | VXN Level-3 fallback (FRED prior-day close) | Refreshed daily 4:35 PM ET |
| `/data/vix_intraday_session.json` | VIX today's 4 scheduled yfinance pulls + freeze state | Resets at 9:40 AM ET |
| `/data/vxn_intraday_session.json` | VXN today's 4 scheduled yfinance pulls + freeze state | Resets at 9:40 AM ET |
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
