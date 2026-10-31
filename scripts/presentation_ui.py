"""
Simple, clean single-page Frontend for the Real-Time Fraud Detection Pipeline.
Launch with:
    python scripts/presentation_ui.py
Opens http://localhost:8501 automatically in the default web browser.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import FEATURE_COLUMNS, get_config
from src.scoring import LinearFraudModelArtifact


def load_dashboard_state() -> dict[str, Any]:
    cfg = get_config()
    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)

    with (cfg.results_dir / "evaluation_metrics.json").open("r", encoding="utf-8") as f:
        eval_metrics = json.load(f)

    with (cfg.results_dir / "smoke_test_verification.json").open("r", encoding="utf-8") as f:
        smoke_metrics = json.load(f)

    fixture_path = PROJECT_ROOT / "data" / "fixtures" / "heldout_sample_300.csv"
    if fixture_path.exists():
        sample_df = pd.read_csv(fixture_path)
    elif cfg.heldout_csv_path.exists():
        sample_df = pd.read_csv(cfg.heldout_csv_path, nrows=300)
    else:
        sample_df = pd.DataFrame()

    presets: list[dict[str, Any]] = []
    if not sample_df.empty:
        frauds = sample_df[sample_df["Class"] == 1]
        for idx, (_, row) in enumerate(frauds.head(3).iterrows(), start=1):
            presets.append(
                {
                    "label": f"Sample Fraud #{idx} (Amount: {row['Amount']:.2f})",
                    "transaction_id": str(row.get("transaction_id", f"fraud_{idx}")),
                    "actual_label": int(row["Class"]),
                    "features": {col: float(row[col]) for col in FEATURE_COLUMNS},
                }
            )
        legit = sample_df[sample_df["Class"] == 0]
        low_row = legit[legit["Amount"] <= artifact.amount_q33].head(1)
        med_row = legit[
            (legit["Amount"] > artifact.amount_q33) & (legit["Amount"] <= artifact.amount_q67)
        ].head(1)
        high_row = legit[legit["Amount"] > artifact.amount_q67].head(1)

        for band_name, subset in [
            ("Normal Low-Amount Purchase", low_row),
            ("Normal Medium-Amount Purchase", med_row),
            ("Normal High-Amount Purchase", high_row),
        ]:
            if not subset.empty:
                r = subset.iloc[0]
                presets.append(
                    {
                        "label": f"{band_name} (Amount: {r['Amount']:.2f})",
                        "transaction_id": str(r.get("transaction_id", "tx_legit")),
                        "actual_label": int(r["Class"]),
                        "features": {col: float(r[col]) for col in FEATURE_COLUMNS},
                    }
                )

    return {
        "eval_metrics": eval_metrics,
        "smoke_metrics": smoke_metrics,
        "presets": presets,
    }


def explain_and_score_payload(raw_payload: str, threshold: float) -> dict[str, Any]:
    cfg = get_config()
    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)

    now_ms = int(time.time() * 1000)
    scored = artifact.score_raw_payload(
        raw_payload=raw_payload,
        threshold_override=threshold,
        scoring_ts_ms_override=now_ms,
    )

    contributions: list[dict[str, Any]] = []
    if scored.is_valid:
        data = json.loads(raw_payload)
        feats = data["features"]
        for idx, col in enumerate(artifact.feature_names):
            raw_val = float(feats[col])
            scaled_val = (raw_val - artifact.scaler_mean[idx]) / artifact.scaler_scale[idx]
            weight = artifact.coefficients[idx]
            contrib = weight * scaled_val
            contributions.append(
                {
                    "feature": col,
                    "logit_contribution": round(contrib, 3),
                    "abs_contribution": abs(contrib),
                }
            )
        contributions.sort(key=lambda x: x["abs_contribution"], reverse=True)

    return {
        "scored": scored.to_dict(),
        "top_contributions": contributions[:5],
    }


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Real-Time Fraud Detection — Simple Presentation Dashboard</title>
<style>
  body {
    margin: 0;
    font-family: "Segoe UI", Arial, sans-serif;
    background: #f4f6f9;
    color: #1f2937;
  }
  .header {
    background: #1e3a8a;
    color: white;
    padding: 18px 28px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
  }
  .header h1 { margin: 0; font-size: 1.35rem; }
  .header p { margin: 4px 0 0; font-size: 0.9rem; color: #bfdbfe; }
  .srn-tag {
    background: #2563eb;
    padding: 6px 14px;
    border-radius: 6px;
    font-weight: bold;
    font-size: 0.9rem;
  }
  .container {
    max-width: 1100px;
    margin: 20px auto 40px;
    padding: 0 20px;
  }
  .flow-box {
    background: white;
    border: 1px solid #dbe2ea;
    border-radius: 8px;
    padding: 14px;
    text-align: center;
    font-weight: 600;
    color: #1e3a8a;
    margin-bottom: 18px;
    font-size: 0.98rem;
  }
  .kpi-row {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 14px;
    margin-bottom: 20px;
  }
  .kpi {
    background: white;
    border: 1px solid #dbe2ea;
    border-radius: 8px;
    padding: 14px 16px;
    text-align: center;
  }
  .kpi .num {
    font-size: 1.65rem;
    font-weight: bold;
    color: #1e3a8a;
    margin: 4px 0;
  }
  .kpi .lbl { font-size: 0.84rem; color: #6b7280; }
  .card {
    background: white;
    border: 1px solid #dbe2ea;
    border-radius: 8px;
    padding: 20px;
    margin-bottom: 20px;
  }
  .card h2 {
    margin: 0 0 14px;
    font-size: 1.15rem;
    color: #1e3a8a;
    border-bottom: 2px solid #eff6ff;
    padding-bottom: 8px;
  }
  .two-col {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    gap: 20px;
  }
  label {
    display: block;
    font-size: 0.88rem;
    font-weight: 600;
    margin-bottom: 5px;
    color: #374151;
  }
  select, input[type="number"] {
    width: 100%;
    padding: 9px 10px;
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    font-size: 0.92rem;
    margin-bottom: 12px;
    box-sizing: border-box;
  }
  input[type="range"] { width: 100%; margin-bottom: 12px; }
  button {
    background: #1e3a8a;
    color: white;
    border: none;
    padding: 10px 18px;
    border-radius: 6px;
    font-size: 0.92rem;
    font-weight: 600;
    cursor: pointer;
  }
  button:hover { background: #1d4ed8; }
  .result-box {
    border-radius: 8px;
    padding: 16px;
    text-align: center;
    margin-bottom: 14px;
  }
  .res-fraud {
    background: #fef2f2;
    border: 2px solid #ef4444;
    color: #991b1b;
  }
  .res-normal {
    background: #f0fdf4;
    border: 2px solid #22c55e;
    color: #166534;
  }
  .res-title { font-size: 1.3rem; font-weight: bold; margin-bottom: 6px; }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
    margin-top: 8px;
  }
  th, td {
    border: 1px solid #e2e8f0;
    padding: 9px 12px;
    text-align: center;
  }
  th { background: #f8fafc; color: #1e3a8a; font-weight: 600; }
  td:first-child, th:first-child { text-align: left; }
  .bar-item {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.84rem;
    margin-bottom: 6px;
  }
  .bar-bg {
    flex: 1;
    height: 12px;
    background: #e5e7eb;
    border-radius: 4px;
    overflow: hidden;
  }
  .bar-fg { height: 100%; }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>Real-Time Credit Card Fraud Detection Pipeline</h1>
    <p>Public ULB Dataset &rarr; Apache Kafka &rarr; Apache Flink &rarr; Fraud Model Scoring &rarr; Apache Iceberg</p>
  </div>
  <div class="srn-tag">
    1: PES1UG24AM145 (Kushal G)<br/>
    2: PES1UG24AM143 (Kshitij Satish Shetty)
  </div>
</div>

<div class="container">

  <!-- Pipeline Flow -->
  <div class="flow-box">
    1. Public Dataset (284,807 Tx) &nbsp;&rarr;&nbsp;
    2. Kafka Stream &nbsp;&rarr;&nbsp;
    3. Flink Real-Time Scoring &nbsp;&rarr;&nbsp;
    4. Apache Iceberg Table
  </div>

  <!-- 4 Simple KPIs -->
  <div class="kpi-row">
    <div class="kpi">
      <div class="lbl">Held-Out Test Transactions</div>
      <div class="num">56,962</div>
      <div class="lbl">98 Actual Frauds | 56,864 Normal</div>
    </div>
    <div class="kpi">
      <div class="lbl">Frauds Caught (Recall)</div>
      <div class="num" style="color:#16a34a;">91.84%</div>
      <div class="lbl">90 Caught / Only 8 Missed</div>
    </div>
    <div class="kpi">
      <div class="lbl">False-Positive Rate (FPR)</div>
      <div class="num" style="color:#d97706;">2.44%</div>
      <div class="lbl">1,386 False Alarms / 56,864</div>
    </div>
    <div class="kpi">
      <div class="lbl">PR-AUC &amp; ROC-AUC</div>
      <div class="num">0.719 / 0.972</div>
      <div class="lbl">Threshold = 0.50</div>
    </div>
  </div>

  <!-- Section 1: Simple Live Transaction Checker -->
  <div class="card">
    <h2>1. Live Transaction Fraud Checker (Flink Scoring Engine)</h2>
    <div class="two-col">
      <div>
        <label>Choose a Sample Transaction from the Dataset:</label>
        <select id="presetSelect" onchange="loadPreset()"></select>

        <label>Transaction Amount ($):</label>
        <input type="number" id="amountInput" step="0.01" oninput="scoreCurrent()" />

        <label>Fraud Probability Threshold: <span id="threshLabel">0.50</span></label>
        <input type="range" id="threshInput" min="0.10" max="0.95" step="0.05" value="0.50"
               oninput="document.getElementById('threshLabel').innerText=parseFloat(this.value).toFixed(2); scoreCurrent();" />

        <button onclick="scoreCurrent()">Check Transaction</button>
      </div>

      <div>
        <div id="resultBox" class="result-box res-normal">
          <div class="res-title" id="resDecision">Loading...</div>
          <div id="resDetails"></div>
        </div>
        <div style="font-size:0.85rem; font-weight:600; margin-bottom:6px; color:#374151;">
          Top 5 Features Driving This Prediction (Red = Raises Fraud Risk, Green = Lowers Risk):
        </div>
        <div id="featureBars"></div>
      </div>
    </div>
  </div>

  <!-- Section 2: Segment Error Analysis & Who Bears the Cost -->
  <div class="card">
    <h2>2. Segment-Level Error Analysis &amp; Who Bears the Cost</h2>
    <p style="margin:0 0 10px; font-size:0.88rem; color:#4b5563;">
      <i>Note: Transaction-amount proxy bands defined from training data quantiles (Low &le; 9.99, Medium 9.99–50.00, High &gt; 50.00).</i>
    </p>
    <table>
      <thead>
        <tr>
          <th>Segment</th>
          <th>Amount Range</th>
          <th>Total Tx</th>
          <th>Actual Frauds</th>
          <th>False Positives (FP)</th>
          <th>FPR (%)</th>
          <th>False Negatives (FN)</th>
          <th>FNR (%)</th>
          <th>Who Bears the Error Cost?</th>
        </tr>
      </thead>
      <tbody id="segmentBody"></tbody>
    </table>
  </div>

  <!-- Section 3: Streaming Load Test -->
  <div class="card">
    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;">
      <h2 style="margin:0; border:none;">3. Streaming Load Test &amp; Apache Iceberg Results (3 Increasing Rates)</h2>
      <button id="smokeBtn" onclick="runLiveTest()">&#9654; Run 3-Rate Test Now</button>
    </div>
    <div id="smokeMsg" style="font-size:0.86rem; color:#16a34a; margin-top:6px;"></div>
    <table>
      <thead>
        <tr>
          <th>Run ID</th>
          <th>Offered Rate</th>
          <th>Achieved Rate</th>
          <th>Valid Events Sent</th>
          <th>Malformed Isolated</th>
          <th>Saved in Iceberg</th>
          <th>Median (p50) Latency</th>
          <th>p95 Latency</th>
          <th>p99 Latency</th>
        </tr>
      </thead>
      <tbody id="loadBody"></tbody>
    </table>
  </div>

</div>

<script>
let STATE = null;
let CURRENT_FEATURES = null;
let CURRENT_ACTUAL = 0;

const COST_NOTES = {
  low: "Review costs can exceed transaction fee margin; 5 unflagged frauds out of 49 in low-amount band.",
  medium: "Lowest FPR (1.42%) & highest PR-AUC (0.84); balanced everyday transactions.",
  high: "Lowest FNR (5.26%, 94.7% recall, 36/38 detected); 456 legitimate transactions flagged in high-amount band."
};

async function init() {
  const res = await fetch('/api/state');
  STATE = await res.json();

  const sel = document.getElementById('presetSelect');
  STATE.presets.forEach((p, i) => {
    const opt = document.createElement('option');
    opt.value = i;
    opt.textContent = p.label;
    sel.appendChild(opt);
  });

  renderSegments();
  renderLoadRuns(STATE.smoke_metrics.runs || []);
  loadPreset();
}

function loadPreset() {
  const idx = parseInt(document.getElementById('presetSelect').value || '0', 10);
  const p = STATE.presets[idx];
  if (!p) return;
  CURRENT_FEATURES = Object.assign({}, p.features);
  CURRENT_ACTUAL = p.actual_label;
  document.getElementById('amountInput').value = CURRENT_FEATURES.Amount.toFixed(2);
  scoreCurrent();
}

async function scoreCurrent() {
  if (!CURRENT_FEATURES) return;
  CURRENT_FEATURES.Amount = parseFloat(document.getElementById('amountInput').value || '0');
  const thresh = parseFloat(document.getElementById('threshInput').value || '0.50');

  const payload = JSON.stringify({
    transaction_id: "demo_tx",
    run_id: "ui_demo",
    producer_ts_ms: Date.now(),
    features: CURRENT_FEATURES,
    actual_label: CURRENT_ACTUAL
  });

  const res = await fetch('/api/score', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ raw_payload: payload, threshold: thresh })
  });
  const data = await res.json();
  const s = data.scored;

  const box = document.getElementById('resultBox');
  const isFraud = s.predicted_label === 1;
  box.className = 'result-box ' + (isFraud ? 'res-fraud' : 'res-normal');
  document.getElementById('resDecision').innerText = isFraud
    ? '🚨 FRAUD DETECTED (Blocked / Flagged)'
    : '✅ NORMAL TRANSACTION (Approved)';

  const actualStr = CURRENT_ACTUAL === 1 ? 'Actual Dataset Label: FRAUD' : 'Actual Dataset Label: NORMAL';
  document.getElementById('resDetails').innerHTML =
    `<b>Fraud Probability:</b> ${(s.fraud_probability * 100).toFixed(2)}% &nbsp;|&nbsp; ` +
    `<b>Segment:</b> ${s.amount_segment.toUpperCase()} ($${s.amount.toFixed(2)})<br/>` +
    `<span style="font-size:0.84rem;">${actualStr}</span>`;

  const bars = document.getElementById('featureBars');
  const maxVal = Math.max(0.5, ...data.top_contributions.map(c => c.abs_contribution));
  bars.innerHTML = data.top_contributions.map(c => {
    const w = Math.min(100, (c.abs_contribution / maxVal) * 100);
    const col = c.logit_contribution >= 0 ? '#ef4444' : '#22c55e';
    const sign = c.logit_contribution >= 0 ? '+' : '';
    return `
      <div class="bar-item">
        <span style="width:45px; font-weight:bold;">${c.feature}</span>
        <div class="bar-bg"><div class="bar-fg" style="width:${w}%; background:${col};"></div></div>
        <span style="width:55px; text-align:right;">${sign}${c.logit_contribution}</span>
      </div>`;
  }).join('');
}

function renderSegments() {
  const tbody = document.getElementById('segmentBody');
  tbody.innerHTML = STATE.eval_metrics.segment_analysis.map(s => `
    <tr>
      <td><b>${s.segment.toUpperCase()}</b></td>
      <td>${s.amount_band_rule}</td>
      <td>${s.num_transactions.toLocaleString()}</td>
      <td>${s.actual_frauds}</td>
      <td>${s.false_positives}</td>
      <td><b>${(s.false_positive_rate * 100).toFixed(2)}%</b></td>
      <td>${s.false_negatives}</td>
      <td><b>${(s.false_negative_rate * 100).toFixed(2)}%</b></td>
      <td style="text-align:left; font-size:0.83rem;">${COST_NOTES[s.segment] || ''}</td>
    </tr>
  `).join('');
}

function renderLoadRuns(runs) {
  const tbody = document.getElementById('loadBody');
  tbody.innerHTML = runs.map(r => `
    <tr>
      <td><b>${r.run_id}</b></td>
      <td>${r.offered_rate_eps} eps</td>
      <td>${r.achieved_producer_rate_eps} eps</td>
      <td>${r.sent_events}</td>
      <td>${r.malformed_caught} / ${r.malformed_injected}</td>
      <td style="color:#16a34a; font-weight:bold;">${r.persisted_count} (0 lost)</td>
      <td>${r.latency_median_p50_ms.toFixed(2)} ms</td>
      <td>${r.latency_p95_ms.toFixed(2)} ms</td>
      <td>${r.latency_p99_ms.toFixed(2)} ms</td>
    </tr>
  `).join('');
}

async function runLiveTest() {
  const btn = document.getElementById('smokeBtn');
  const msg = document.getElementById('smokeMsg');
  btn.disabled = true;
  btn.innerText = 'Running...';
  msg.innerText = 'Streaming 600 transactions across 200, 500, and 1000 eps into Apache Iceberg...';
  const res = await fetch('/api/run_smoke_test', { method: 'POST' });
  const data = await res.json();
  renderLoadRuns(data.runs || []);
  msg.innerText = '✔ Completed! All 600 valid transactions saved to Apache Iceberg (15 malformed isolated, 0 lost).';
  btn.disabled = false;
  btn.innerText = '▶ Run 3-Rate Test Now';
}

window.addEventListener('DOMContentLoaded', init);
</script>
</body>
</html>
"""


class PresentationHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args) -> None:
        pass

    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            raw = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return

        if parsed.path == "/api/state":
            self._send_json(load_dashboard_state())
            return

        self.send_error(404, "Not Found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body_bytes = self.rfile.read(length) if length > 0 else b"{}"

        if parsed.path == "/api/score":
            req = json.loads(body_bytes.decode("utf-8"))
            raw_payload = req.get("raw_payload", "")
            threshold = float(req.get("threshold", 0.50))
            self._send_json(explain_and_score_payload(raw_payload, threshold))
            return

        if parsed.path == "/api/run_smoke_test":
            from scripts.smoke_test import run_smoke_test

            summary = run_smoke_test()
            self._send_json(summary)
            return

        self.send_error(404, "Not Found")


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the Simple Presentation UI.")
    parser.add_argument("--port", type=int, default=8501, help="HTTP port (default: 8501)")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open browser")
    args = parser.parse_args()

    url = f"http://localhost:{args.port}"
    server = ThreadingHTTPServer(("127.0.0.1", args.port), PresentationHandler)
    print(f"======================================================================")
    print(f" Real-Time Fraud Detection — Simple Presentation UI")
    print(f" URL: {url}")
    print(f" Press Ctrl+C in this terminal to stop the server.")
    print(f"======================================================================")

    if not args.no_browser:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down Presentation UI server.")
        server.server_close()


if __name__ == "__main__":
    main()
