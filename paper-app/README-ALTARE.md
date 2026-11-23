# Penny Watcher — LIVE PAPER on Altare (trial deploy)

The forward paper-trading engine (`paper_daily.py`, untouched) wrapped for
Altare's Python app hosting by `app.py` (status page + daily scheduler).

## What's in the package

- `app.py` — web wrapper: serves a status page on `0.0.0.0:$PORT`, runs the
  engine once on startup, then Tue–Sat 07:14 HKT (the old cron's schedule).
- `paper_daily.py`, `pipeline.py` — the engine, exactly as run locally.
- `requirements.txt` — numpy + pandas only (the Sina fallback degrades
  gracefully without akshare; Nasdaq is the primary source).
- `output/leadlag_pairs.csv` — the qualified lead-lag pairs (engine input).
- `output/paper_state.json` — the live ledger, frozen at 2026-11-12
  (equity 94.135, cash 0.49, 10 open). The first run on Altare catches it up
  day by day; a day is never reprocessed.

Not included, not needed: the 946 MB price cache, the backtest, the web UI
(the GitHub Pages site stays as-is; this trial does not publish to it).

## Preferred route: the existing penny-watcher repo → Altare Apps

This package lives in `ken196502/penny-watcher` under **`paper-app/`** (the
repo root stays the GitHub Pages site). Altare App Hosting supports a
**Root directory** setting for exactly this: in the Altare console,
**Apps → New app → GitHub repository** — the repo is public, so it can be
pasted directly — then set **Root directory = `paper-app`**, runtime Python,
start command `python3 app.py` (it listens on Altare's assigned `PORT`),
health check path `/health`, allocate ~2 GiB memory. Every push to `main`
redeploys (note: pushes that only change the Pages site also trigger a
redeploy of this app — harmless, see the state caveat below).

One caveat: if an Altare redeploy resets the app's files to the repo state,
the ledger restarts from the committed `paper_state.json` (2026-11-12) and
the startup run re-books every day since — deterministic, ~2 minutes, same
result. If files persist across deploys, it just continues. Either way no
day is ever double-booked (`state.last_date` guards it).

## Alternative route: server template + zip upload (about 5 minutes)

1. Create a new server → **Python** template.
2. Allocate **2 GiB memory / 50% CPU / 2 GiB disk** (from the 4 GiB / 125% /
   40 GiB pool — plenty; the engine's daily fetch peaks briefly).
3. Open **Network** and note the allocated address + port.
4. Upload these files with the file manager (keep the `output/` folder
   structure). Don't upload any venv.
5. In **Startup** settings set the main file to `app.py`. If the console
   lets you set environment variables, set `PORT` to the allocated port;
   otherwise set the startup command to `python3 app.py <allocated port>`.
   Optionally set `RUN_TOKEN` to any secret string — that enables
   `GET /run?token=<secret>` to trigger an engine run on demand.
6. Start the server and watch its console: on startup the wrapper runs the
   engine once (~2 minutes, fetching ~173 symbols), then serves the status
   page at `http://<server-address>:<port>/`.

## Verify

- `/` shows "Ledger through <latest US session>", equity, positions.
- `/state.json` returns the ledger; `/runlog` shows the engine's output.
- The GitHub Pages paper page is NOT updated by this trial — the Altare
  ledger is a separate copy until we decide to sync it back / make Altare
  the source of truth.

## Local verification already done (2026-11-23)

The identical package was run on a copy of the ledger locally: fetched
173/173 symbols, booked 2026-11-13 → 2026-11-20 in order, no sells
(nothing due yet), equity 94.135 → 80.745, 10 open, cash 0.49.
The repo's live ledger was not touched.
