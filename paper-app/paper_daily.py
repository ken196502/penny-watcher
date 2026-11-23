"""FORWARD paper trading for the lead-lag finder — runs daily, for real.

This is NOT the backtest (paper_trading.py replays history). This engine
starts from a cash account on 2026-11-04 and only ever moves forward:

  - every run fetches the latest daily bars for the 166 finder stocks and
    their 24 sector ETFs (Nasdaq API primary — the same source the cached
    history was built from — AKShare/Sina fallback),
  - processes each NEW trading day once, in order:
      1. pending entries execute at the day's open (10-slot, 1-per-ticker),
      2. positions due are sold at the day's close (entry + 20 trading days),
      3. after the close, signals are evaluated: an ETF fire ARMS the pair
         for 30 trading days; a stock fire while armed queues an entry for
         the next trading day's open and consumes the arm,
      4. the account is marked to market and the day is appended to the
         equity log. A day is never reprocessed (state.last_date).
  - the ledger lives in output/paper_state.json; export_leadlag.py reads
    it into data.json for the site's LIVE PAPER section.

Seed: on first run the ledger is initialized as of SEED_DATE from the
cached history — arms whose ETF fired within the last 30 trading days and
whose stock has not fired since are live, and a stock fire on SEED_DATE
itself queues a real pending entry. No positions, cash 100.
"""
import json
import pickle
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

import pipeline as p

SEED_DATE = "2026-11-02"          # last day of the cached backtest data
ACCOUNT_START = "2026-11-04"      # the day the paper account opened
FROMDATE = "2024-09-29"
ARM_WINDOW = 30
HOLD_DAYS = 20
COST = p.COST_PER_SIDE
MAX_POS = p.MAX_POSITIONS
STATE_PATH = p.OUT / "paper_state.json"


def triggers(values) -> np.ndarray:
    v = np.asarray(values, dtype=bool)
    prev = np.concatenate(([False], v[:-1]))
    return np.where(v & ~prev)[0]


def fetch_nasdaq(t: str):
    rows = None
    for asset in ("stocks", "etf"):
        url = (f"https://api.nasdaq.com/api/quote/{urllib.parse.quote(t)}/historical"
               f"?assetclass={asset}&fromdate={FROMDATE}&limit=9999")
        try:
            data = json.loads(urllib.request.urlopen(
                urllib.request.Request(url, headers=p.UA), timeout=25).read())
            rows = (data.get("data") or {}).get("tradesTable", {}).get("rows")
            if rows:
                break
        except Exception:
            rows = None
    if not rows:
        return None
    try:
        d = pd.DataFrame(
            [{"Open": p._num(r["open"]), "High": p._num(r["high"]),
              "Low": p._num(r["low"]), "Close": p._num(r["close"]),
              "Volume": p._num(r["volume"])} for r in rows],
            index=[pd.to_datetime(r["date"]) for r in rows])
        return p.split_adjust(d.sort_index().dropna(subset=["Close"]))
    except Exception:
        return None


def fetch_sina(t: str):
    try:
        import akshare as ak
        raw = ak.stock_us_daily(symbol=t, adjust="qfq")
        d = pd.DataFrame(
            {"Open": raw["open"].values, "High": raw["high"].values,
             "Low": raw["low"].values, "Close": raw["close"].values,
             "Volume": raw["volume"].values},
            index=pd.to_datetime(raw["date"].values))
        return d.sort_index().dropna(subset=["Close"])
    except Exception:
        return None


def fetch_one(t: str):
    d = fetch_nasdaq(t)
    src = "nasdaq"
    if d is None or len(d) < 200:
        d = fetch_sina(t)
        src = "sina"
    if d is None or len(d) < 200:
        return t, None, src
    d = d[d.index >= FROMDATE]
    return t, p.add_signals(p.add_indicators(d)), src


def refresh_marks(state, frames):
    """Update per-position marks and arm ages against the latest frames."""
    last = state["last_date"]
    for pos in state["positions"]:
        df = frames.get(pos["ticker"])
        if df is None:
            continue
        px = close_at(df, last)
        if px:
            pos["last_px"] = round(px, 4)
            pos["unreal_pct"] = round((px / pos["entry_px"] - 1) * 100, 2)
    for a in state["arms"]:
        df = frames.get(a["ticker"])
        if df is None:
            continue
        ap = pos_of(df, a["etf_fire"])
        dp = pos_of(df, last)
        if ap is not None and dp is not None:
            a["age_days"] = int(dp - ap)


def load_frames(tickers):
    frames, failed = {}, []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for t, d, src in ex.map(fetch_one, tickers):
            if d is None:
                failed.append(t)
            else:
                frames[t] = d
    return frames, failed


def pos_of(frame, date_str):
    """position of date_str in frame index, or None"""
    i = int(np.searchsorted(frame.index.values, np.datetime64(date_str)))
    if i < len(frame) and str(frame.index[i].date()) == date_str:
        return i
    return None


def close_at(frame, date_str):
    i = int(np.searchsorted(frame.index.values, np.datetime64(date_str)))
    if i >= len(frame):
        i = len(frame) - 1
    elif str(frame.index[i].date()) != date_str:
        i = i - 1
    if i < 0:
        return None
    return float(frame["Close"].values[i])


def seed_state(qual, frames):
    """Initialize the ledger as of SEED_DATE from (freshly fetched) frames."""
    arms, pending = [], []
    for _, row in qual.iterrows():
        t, e, sig = row["ticker"], row["etf"], row["signal"]
        df, edf = frames.get(t), frames.get(e)
        if df is None or edf is None:
            continue
        col = f"sig_{sig}"
        if col not in df.columns or col not in edf.columns:
            continue
        last = pos_of(df, SEED_DATE)
        if last is None:
            continue
        S = triggers(df[col].values)
        e_trig = triggers(edf[col].values)
        E = np.searchsorted(df.index.values, edf.index.values[e_trig], side="left")
        E = np.unique(E[E < len(df)])
        # latest ETF fire at or before SEED_DATE
        cand = E[E <= last]
        if len(cand) == 0:
            continue
        arm = int(cand[-1])
        if last - arm > ARM_WINDOW:
            continue
        stock_fires = S[(S > arm) & (S <= last)]
        if len(stock_fires) == 0:
            arms.append({"ticker": t, "etf": e, "signal": sig,
                         "etf_fire": str(df.index[arm].date())})
        elif int(stock_fires[0]) == last:
            # the FIRST stock fire after the arm landed on the seed day
            # itself -> a real pending entry for the next trading day
            pending.append({"ticker": t, "etf": e, "signal": sig,
                            "etf_fire": str(df.index[arm].date()),
                            "stock_fire": SEED_DATE})
    return {
        "account": {"cash": 100.0, "started": ACCOUNT_START},
        "last_date": SEED_DATE,
        "arms": arms,
        "pending_entries": pending,
        "positions": [],
        "closed": [],
        "equity_log": [{"date": SEED_DATE, "equity": 100.0}],
        "log": [f"{ACCOUNT_START} paper account opened (cash 100); "
                f"seeded {len(arms)} live arms, {len(pending)} pending entries "
                f"from data through {SEED_DATE}"],
    }


def main():
    qual = pd.read_csv(p.OUT / "leadlag_pairs.csv")
    qual = qual[qual["qualified"] == True]  # noqa: E712
    tickers = set(qual["ticker"]) | set(qual["etf"]) | {"SPY"}
    # the ledger's live state must always be fetchable, even for symbols a
    # finder re-run has since disqualified — otherwise held positions can't
    # be marked or sold, and queued entries/arms would strand forever
    # instead of resolving (MISS/EXPIRE) in order
    if STATE_PATH.exists():
        _st = json.loads(STATE_PATH.read_text())
        for _k in ("positions", "pending_entries", "arms"):
            for _r in _st.get(_k, []):
                tickers.add(_r["ticker"])
                tickers.add(_r["etf"])
    tickers = sorted(tickers)
    print(f"fetching {len(tickers)} symbols…", flush=True)
    frames, failed = load_frames(tickers)
    print(f"fetched {len(frames)} ok, failed: {failed}", flush=True)
    if "SPY" not in frames:
        print("SPY fetch failed — cannot advance the calendar; aborting")
        sys.exit(1)

    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
        print(f"ledger loaded: last_date {state['last_date']}, "
              f"cash {state['account']['cash']:.2f}, "
              f"{len(state['positions'])} open, {len(state['arms'])} armed")
    else:
        state = seed_state(qual, frames)
        print(f"seeded: {len(state['arms'])} arms, "
              f"{len(state['pending_entries'])} pending entries")

    master = frames["SPY"].index
    new_days = [str(d.date()) for d in master if str(d.date()) > state["last_date"]]
    if not new_days:
        refresh_marks(state, frames)
        print("no new trading days — marks refreshed, ledger dates unchanged")
        STATE_PATH.write_text(json.dumps(state, indent=1))
        return

    by_pair = {}
    for _, row in qual.iterrows():
        by_pair.setdefault((row["ticker"], row["signal"]), row["etf"])

    for day in new_days:
        log = state["log"]
        # 1) pending entries at today's open
        still_pending = []
        equity_prev = state["equity_log"][-1]["equity"]
        for pend in state["pending_entries"]:
            t = pend["ticker"]
            df = frames.get(t)
            pi = pos_of(df, day) if df is not None else None
            if pi is None:
                still_pending.append(pend)  # ticker didn't trade today; retry next day
                continue
            if any(pos["ticker"] == t for pos in state["positions"]):
                log.append(f"{day} SKIP {t} ({pend['signal']}): already held")
                continue
            if len(state["positions"]) >= MAX_POS:
                log.append(f"{day} MISS {t} ({pend['signal']}): no free slot")
                continue
            open_px = float(df["Open"].values[pi])
            alloc = equity_prev / MAX_POS
            shares = alloc / (open_px * (1 + COST))
            state["account"]["cash"] -= shares * open_px * (1 + COST)
            exit_i = pi + HOLD_DAYS
            exit_due = str(df.index[min(exit_i, len(df) - 1)].date()) \
                if exit_i < len(df) else None
            state["positions"].append({
                **pend, "entry_date": day, "entry_px": round(open_px, 4),
                "shares": shares, "exit_due": exit_due,
            })
            log.append(f"{day} BUY {t} @ {open_px:.2f} ({pend['etf']} {pend['signal']}, "
                       f"ETF fired {pend['etf_fire']}, stock fired {pend['stock_fire']})")
        state["pending_entries"] = still_pending

        # 2) exits at today's close
        remaining = []
        for pos in state["positions"]:
            if not pos.get("exit_due"):
                # entered when the frame was too short to date the exit;
                # date it now that more bars exist
                df0 = frames[pos["ticker"]]
                ep = pos_of(df0, pos["entry_date"])
                if ep is not None and ep + HOLD_DAYS < len(df0):
                    pos["exit_due"] = str(df0.index[ep + HOLD_DAYS].date())
            if pos.get("exit_due") == day:
                df = frames[pos["ticker"]]
                px = close_at(df, day)
                proceeds = pos["shares"] * px * (1 - COST)
                state["account"]["cash"] += proceeds
                net = (px * (1 - COST)) / (pos["entry_px"] * (1 + COST)) - 1
                state["closed"].append({
                    **{k: pos[k] for k in ("ticker", "etf", "signal", "etf_fire",
                                           "stock_fire", "entry_date", "entry_px")},
                    "exit_date": day, "exit_px": round(px, 4),
                    "net_pct": round(float(net) * 100, 2),
                })
                log.append(f"{day} SELL {pos['ticker']} @ {px:.2f} ({net * 100:+.1f}%)")
            else:
                remaining.append(pos)
        state["positions"] = remaining

        # 3) signals after the close: arms and stock fires
        for (t, sig), e in by_pair.items():
            df, edf = frames.get(t), frames.get(e)
            if df is None or edf is None:
                continue
            col = f"sig_{sig}"
            if col not in df.columns or col not in edf.columns:
                continue
            pi = pos_of(df, day)
            if pi is None:
                continue
            s_trig = triggers(df[col].values)
            stock_fired = pi in set(s_trig.tolist())
            ei = pos_of(edf, day)
            etf_fired = False
            if ei is not None:
                e_trig = triggers(edf[col].values)
                etf_fired = ei in set(e_trig.tolist())
            key = (t, e, sig)
            arm = next((a for a in state["arms"]
                        if (a["ticker"], a["etf"], a["signal"]) == key), None)
            if etf_fired:
                new_arm = {"ticker": t, "etf": e, "signal": sig, "etf_fire": day}
                if arm:
                    state["arms"].remove(arm)
                state["arms"].append(new_arm)
                arm = new_arm
                log.append(f"{day} ARM {t}×{e} {sig} (ETF fired)")
            if stock_fired and arm is not None:
                arm_pos = pos_of(df, arm["etf_fire"])
                if arm_pos is not None and pi - arm_pos <= ARM_WINDOW:
                    state["pending_entries"].append({
                        "ticker": t, "etf": e, "signal": sig,
                        "etf_fire": arm["etf_fire"], "stock_fire": day})
                    state["arms"].remove(arm)
                    log.append(f"{day} FIRE {t} ({sig}) while armed → entry at next open")
        # expire old arms
        kept_arms = []
        for a in state["arms"]:
            df = frames.get(a["ticker"])
            ap = pos_of(df, a["etf_fire"]) if df is not None else None
            dp = pos_of(df, day) if df is not None else None
            if ap is not None and dp is not None and dp - ap > ARM_WINDOW:
                log.append(f"{day} EXPIRE arm {a['ticker']}×{a['etf']} {a['signal']} "
                           f"(ETF fired {a['etf_fire']}, stock never followed)")
            else:
                kept_arms.append(a)
        state["arms"] = kept_arms

        # 4) mark to market
        eq = state["account"]["cash"]
        for pos in state["positions"]:
            px = close_at(frames[pos["ticker"]], day)
            if px:
                pos["last_px"] = round(px, 4)
                pos["unreal_pct"] = round((px / pos["entry_px"] - 1) * 100, 2)
                eq += pos["shares"] * px
        state["equity_log"].append({"date": day, "equity": round(float(eq), 3)})
        state["last_date"] = day
        print(f"{day}: equity {eq:.2f} | {len(state['positions'])} open | "
              f"{len(state['arms'])} armed | {len(state['pending_entries'])} pending")

    refresh_marks(state, frames)
    state["log"] = state["log"][-120:]
    STATE_PATH.write_text(json.dumps(state, indent=1))
    print(f"ledger saved: last_date {state['last_date']}, "
          f"closed trades {len(state['closed'])}")


if __name__ == "__main__":
    main()
