"""Altare wrapper for the Penny Watcher forward paper-trading engine.

Altare app hosting expects a long-running app that listens on 0.0.0.0:$PORT.
This wrapper provides exactly that, with the daily engine inside:

  - GET /           human status page (ledger date, equity, positions)
  - GET /state.json the raw ledger (output/paper_state.json)
  - GET /health     "ok"
  - GET /run?token=…  trigger one engine run now (only if RUN_TOKEN is set
                      in the environment and matches)
  - scheduler       runs paper_daily.py once a day, Tue–Sat at 07:14 HKT
                    (the standing schedule from the old cron), and once on
                    startup when RUN_ON_START=1 (default) so a fresh deploy
                    immediately catches the ledger up.

The engine itself (paper_daily.py) is untouched: it fetches Nasdaq bars,
books each new trading day once, and writes output/paper_state.json.
"""
import json
import os
import subprocess
import sys
import threading
from datetime import datetime, time as dtime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo

ROOT = Path(__file__).parent
STATE = ROOT / "output" / "paper_state.json"
RUN_LOG = ROOT / "output" / "last_run.log"
LAST_RUN_FILE = ROOT / "output" / "last_scheduler_run.txt"
HKT = ZoneInfo("Asia/Hong_Kong")
RUN_AT = dtime(7, 14)          # 07:14 HKT, Tue–Sat (weekday 1..5 in HKT)
PORT = int(os.environ.get("PORT") or os.environ.get("APP_PORT") or 8080)
if len(sys.argv) > 1 and sys.argv[1].isdigit():
    PORT = int(sys.argv[1])  # e.g. startup command: python3 app.py <allocated port>

_lock = threading.Lock()
_running = False
_last_result = "not run yet"


def run_engine(reason: str):
    global _running, _last_result
    with _lock:
        if _running:
            return "already running"
        _running = True
    try:
        with open(RUN_LOG, "ab") as log:
            log.write(f"\n===== run ({reason}) {datetime.now(HKT).isoformat()} =====\n".encode())
            proc = subprocess.run(
                [sys.executable, str(ROOT / "paper_daily.py")],
                cwd=str(ROOT), stdout=log, stderr=subprocess.STDOUT,
                timeout=1800)
        _last_result = f"{reason}: exit {proc.returncode} at {datetime.now(HKT).isoformat()}"
    except Exception as e:  # timeout etc.
        _last_result = f"{reason}: FAILED {e!r}"
    finally:
        with _lock:
            _running = False
    return _last_result


def scheduler():
    if os.environ.get("RUN_ON_START", "1") == "1":
        threading.Thread(target=run_engine, args=("startup",), daemon=True).start()
    while True:
        try:
            now = datetime.now(HKT)
            last = LAST_RUN_FILE.read_text().strip() if LAST_RUN_FILE.exists() else ""
            if (now.weekday() in (1, 2, 3, 4, 5)  # Tue–Sat HKT
                    and now.time() >= RUN_AT
                    and last != now.date().isoformat()):
                LAST_RUN_FILE.write_text(now.date().isoformat())
                run_engine("scheduled")
        except Exception:
            pass
        threading.Event().wait(60)


def status_html() -> str:
    try:
        s = json.loads(STATE.read_text())
        eq = s["equity_log"][-1]
        rows = "".join(
            f"<tr><td>{p['ticker']}</td><td>{p['entry_date']}</td>"
            f"<td>{p['entry_px']}</td><td>{p.get('last_px', '')}</td>"
            f"<td>{p.get('unreal_pct', '')}%</td><td>{p.get('exit_due', '')}</td></tr>"
            for p in s["positions"])
        log_tail = "<br>".join(s["log"][-15:])
        body = f"""<h1>Penny Watcher — LIVE PAPER (Altare)</h1>
<p>Ledger through <b>{s['last_date']}</b> · equity <b>{eq['equity']}</b> ({eq['date']})
· cash {s['account']['cash']:.2f} · open {len(s['positions'])}/10
· pending {len(s['pending_entries'])} · armed {len(s['arms'])}
· closed {len(s['closed'])}</p>
<p>Last engine run: {_last_result} · running: {_running}</p>
<table border="1" cellspacing="0" cellpadding="4">
<tr><th>Ticker</th><th>Entry</th><th>Entry px</th><th>Last px</th><th>Unreal</th><th>Exit due</th></tr>
{rows}</table>
<h3>Recent log</h3><p>{log_tail}</p>"""
    except Exception as e:
        body = f"<h1>Penny Watcher — LIVE PAPER (Altare)</h1><p>state unreadable: {e}</p>"
    return f"<!doctype html><meta charset='utf-8'><title>Paper — Altare</title>{body}"


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, ctype, data: bytes):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/health":
            self._send(200, "text/plain", b"ok")
        elif u.path == "/state.json":
            self._send(200, "application/json", STATE.read_bytes() if STATE.exists() else b"{}")
        elif u.path == "/runlog":
            self._send(200, "text/plain", RUN_LOG.read_bytes()[-20000:] if RUN_LOG.exists() else b"")
        elif u.path == "/run":
            token = os.environ.get("RUN_TOKEN", "")
            given = parse_qs(u.query).get("token", [""])[0]
            if not token or given != token:
                self._send(403, "text/plain", b"forbidden (set RUN_TOKEN and pass ?token=)")
            else:
                threading.Thread(target=run_engine, args=("manual",), daemon=True).start()
                self._send(202, "text/plain", b"run started")
        else:
            self._send(200, "text/html; charset=utf-8", status_html().encode())


if __name__ == "__main__":
    threading.Thread(target=scheduler, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
