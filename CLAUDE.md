# The Pulse — Claude Code Project Context

## What This Is

Pre-market macro regime trading dashboard for NQ and ES futures. Runs every morning before market open to synthesize macro signals into a directional bias (bullish/bearish/neutral) and session difficulty rating.

- **GitHub:** github.com/Pierrehennednant/the-pulse
- **Hosting:** Railway — auto-deploys on every push to `main`
- **Stack:** Python / Flask, no frontend framework

## Four Pillars

| Pillar | Weight | Pipeline |
|---|---|---|
| Economic Calendar | 40% | `pipelines/economic_calendar.py` |
| Geopolitical | 30% | `pipelines/geopolitical.py` — Claude Haiku for story classification |
| Institutional | 20% | `pipelines/institutional.py` |
| Macro Sentiment | 10% | `pipelines/macro_sentiment.py` |

Final bias is assembled in `processors/bias_calculator.py`. Dashboard rendered in `ui/dashboard.py` + `ui/templates/`.

## Key Data Files

- `/data/pinned_stories.json` — pinned geopolitical stories; persist until replaced or 48h expires, never dropped by API rotation
- `/data/size_mode.json` — quarter/normal toggle for position size mode display on dashboard

## AI Usage

Geopolitical pipeline uses **Claude Haiku** (`claude-haiku-4-5-20251001`) for story classification. Comparison of incoming stories vs pinned stories also uses Haiku. Do not swap to a different model without verifying prompt/cost fit.

## Terminal Rules

- **Never use `&&` to chain commands — always use `;`**
- Run `python -m py_compile <file>` on every changed Python file before pushing
- Railway auto-deploys from `main`; a broken push affects production immediately

## Project Layout

```
main.py                  Flask entry point
config.py                Env vars and constants
Procfile                 Railway process definition
requirements.txt         Pinned dependencies (pin Anthropic version)
pipelines/               One file per pillar + supporting pipelines
processors/              Bias calculation, data formatting, snapshot
ui/                      Flask routes (dashboard.py) + Jinja templates
utils/                   Cache, logging, error handling
data/                    Runtime JSON state files
```

## Deployment

Railway reads `Procfile` and `requirements.txt`. Environment variables are set in the Railway dashboard. Do not commit secrets. After any dependency change, verify `requirements.txt` is updated and pinned.
