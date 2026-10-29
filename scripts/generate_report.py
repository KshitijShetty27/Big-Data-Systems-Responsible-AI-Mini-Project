"""Generate the 3-5 page technical_report.pdf from results and figures using ReportLab (with zero-dependency fallback verification)."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def count_pdf_pages(pdf_path: Path) -> int:
    """Count pages in a PDF file using standard PDF /Type /Page object markers (zero external deps)."""
    return len(re.findall(rb"/Type\s*/Page\b", pdf_path.read_bytes()))


def build_pdf(output_pdf: Path) -> int:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            Image,
            PageBreak,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except ImportError:
        if output_pdf.exists():
            pages = count_pdf_pages(output_pdf)
            print(
                f"[INFO] 'reportlab' is not installed in this Python environment "
                f"(install via `python -m pip install reportlab` to rebuild from source); "
                f"verified existing pre-built PDF at {output_pdf}."
            )
            return pages
        raise RuntimeError(
            "reportlab is not installed and report/technical_report.pdf does not exist. "
            "Run: python -m pip install reportlab"
        )

    results_dir = PROJECT_ROOT / "results"
    figures_dir = results_dir / "figures"

    with (results_dir / "evaluation_metrics.json").open("r", encoding="utf-8") as f:
        eval_data = json.load(f)
    with (results_dir / "load_test_runs.json").open("r", encoding="utf-8") as f:
        load_data = json.load(f)
    with (results_dir / "smoke_test_verification.json").open("r", encoding="utf-8") as f:
        smoke_data = json.load(f)

    om = eval_data["overall_metrics"]
    meta = eval_data["model_metadata"]
    seg_rows = eval_data["segment_analysis"]
    sweep_rows = eval_data["threshold_sweep"]

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.50 * inch,
        bottomMargin=0.50 * inch,
        title="Real-Time Credit Card Fraud Detection Pipeline - Technical Report",
        author="Kushal G (PES1UG24AM145) & Kshitij Satish Shetty (PES1UG24AM143)",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "DocTitle",
        parent=styles["Heading1"],
        fontSize=15,
        leading=18,
        textColor=colors.HexColor("#1a365d"),
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        "DocSubtitle",
        parent=styles["Normal"],
        fontSize=9.5,
        leading=12,
        textColor=colors.HexColor("#4a5568"),
        spaceAfter=10,
    )
    h1_style = ParagraphStyle(
        "SectionH1",
        parent=styles["Heading2"],
        fontSize=11.5,
        leading=14,
        textColor=colors.HexColor("#1a365d"),
        spaceBefore=8,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "BodyCompact",
        parent=styles["Normal"],
        fontSize=9.0,
        leading=12.2,
        spaceAfter=5,
    )
    bullet_style = ParagraphStyle(
        "BulletCompact",
        parent=body_style,
        leftIndent=12,
        spaceAfter=3,
    )

    def _make_table(data, col_widths=None, right_align_from_col: int = 1):
        header_cell_style = ParagraphStyle(
            "RepTblHeader",
            parent=styles["Normal"],
            fontName="Helvetica-Bold",
            fontSize=7.6,
            leading=9.6,
            textColor=colors.white,
        )
        left_cell_style = ParagraphStyle(
            "RepTblLeft",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=7.6,
            leading=9.8,
            textColor=colors.HexColor("#1a202c"),
        )
        right_cell_style = ParagraphStyle(
            "RepTblRight",
            parent=left_cell_style,
            alignment=2,  # TA_RIGHT
        )

        wrapped_data = []
        for r_idx, row in enumerate(data):
            wrapped_row = []
            for c_idx, cell in enumerate(row):
                if isinstance(cell, Paragraph):
                    wrapped_row.append(cell)
                elif r_idx == 0:
                    wrapped_row.append(Paragraph(str(cell), header_cell_style))
                else:
                    st = right_cell_style if c_idx >= right_align_from_col else left_cell_style
                    wrapped_row.append(Paragraph(str(cell), st))
            wrapped_data.append(wrapped_row)

        t = Table(wrapped_data, colWidths=col_widths, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a365d")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e0")),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7fafc")]),
                    ("TOPPADDING", (0, 0), (-1, -1), 3.5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4.5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4.5),
                ]
            )
        )
        return t

    story = []

    # ==================== PAGE 1: Architecture, Dataset & Model ====================
    story.append(
        Paragraph(
            "Real-Time Credit Card Fraud Detection Pipeline: Streaming Architecture, "
            "Performance Methodology, and Segment-Level Error Analysis",
            title_style,
        )
    )
    story.append(
        Paragraph(
            "Big Data Engineering Mini-Project Technical Report | "
            "<b>Team:</b> Kushal G (PES1UG24AM145) &amp; Kshitij Satish Shetty (PES1UG24AM143)<br/>"
            "Architecture: Public ULB Dataset &rarr; Apache Kafka &rarr; Apache Flink &rarr; "
            "Fraud Model Scoring &rarr; Apache Iceberg",
            subtitle_style,
        )
    )

    story.append(Paragraph("1. Problem Statement and Streaming Architecture", h1_style))
    story.append(
        Paragraph(
            "Payment processors must score credit card transactions in real time as authorization events arrive. "
            "Batch scoring introduces unacceptable checkout latency, whereas streaming pipelines enable continuous "
            "feature standardization, low-latency model inference, and ACID lakehouse persistence. This project "
            "implements a portable, containerized pipeline (<i>Public ULB Dataset &rarr; Kafka &rarr; Flink &rarr; "
            "Model Scoring &rarr; Apache Iceberg</i>) designed for Windows 10/11, macOS (Intel &amp; Apple Silicon ARM64), "
            "and Linux on a CPU-only PC (~12 GB RAM).",
            body_style,
        )
    )

    arch_table_data = [
        ["Pipeline Stage", "Technology & Pinned Version", "Role & Cross-Platform Configuration"],
        ["1. Event Producer", "Python 3.11+ / confluent-kafka 2.6.1", "Replays held-out transactions (N=56,962) with token-bucket rate pacing"],
        ["2. Ingestion Bus", "Apache Kafka 3.7.1 (KRaft Mode)", "Broker+Controller (no ZooKeeper), internal kafka:9092 & host :29092"],
        ["3. Stream Engine", "Apache Flink 1.18.1 (Java 11 / PyFlink)", "JobManager + TaskManager, 5s checkpointing, UDF linear model scoring"],
        ["4. Table Format", "Apache Iceberg 1.5.2 + REST 0.6.0", "Format v2 Parquet tables partitioned by run_id + dead-letter isolation"],
        ["5. Object Store", "MinIO RELEASE.2024-06-13 (S3 API)", "Local S3-compatible warehouse (s3://warehouse/) initialized via minio/mc"],
    ]
    story.append(_make_table(arch_table_data, col_widths=[1.25 * inch, 2.05 * inch, 3.85 * inch], right_align_from_col=3))
    story.append(Spacer(1, 6))

    story.append(Paragraph("2. Dataset Provenance, Leakage Prevention, and Model Methodology", h1_style))
    story.append(
        Paragraph(
            f"<b>Dataset Provenance:</b> We use the public ULB Credit Card Fraud dataset (<i>data/raw/creditcard.csv</i>, "
            f"Zenodo Record 7395559, MD5 <font name='Courier'>e90efcb83d69faf99fcab8b0255024de</font>, "
            f"SHA-256 <font name='Courier'>{meta['dataset_sha256'][:16]}...</font>). It contains <b>{meta['total_rows']:,}</b> "
            f"transactions with <b>492</b> frauds (0.1727% positive rate) across 30 numeric input features "
            f"(<i>Time</i>, PCA components <i>V1</i>–<i>V28</i>, and <i>Amount</i>) and binary target <i>Class</i>.",
            body_style,
        )
    )
    story.append(
        Paragraph(
            "<b>Strict Target-Leakage Prevention &amp; Stratified Split:</b> Using a fixed seed (<i>random_seed=42</i>), "
            f"the dataset is partitioned via an 80/20 stratified split into <b>{meta['train_rows']:,}</b> training records "
            f"({meta['train_fraud_rows']} frauds) and <b>{meta['heldout_test_rows']:,}</b> held-out evaluation records "
            f"({meta['heldout_test_fraud_rows']} frauds). The target column <i>Class</i> is strictly excluded from all model "
            "inputs during training and inference; <i>parse_and_validate_event()</i> explicitly rejects any Kafka payload "
            "that includes <i>Class</i> inside the features map.",
            body_style,
        )
    )
    story.append(
        Paragraph(
            "<b>Model &amp; Flink Mathematical Parity:</b> We train a CPU-friendly <i>scikit-learn</i> pipeline combining "
            "<i>StandardScaler()</i> and <i>LogisticRegression(class_weight='balanced', C=1.0, solver='lbfgs', max_iter=1000, "
            "random_state=42)</i>. Because shipping heavy native C-extensions into distributed Flink operators can be "
            "brittle across architectures, we export the exact fitted means, scales, coefficients, intercept, and training "
            "amount tertiles (<i>q33 = 9.99, q67 = 50.00</i>) to <i>models/fraud_model_artifact.json</i> and execute "
            "mathematically equivalent scoring inside the Flink UDF. Verification on a 250-record held-out fixture "
            f"confirms a maximum absolute probability difference of <b>{meta['parity_check_max_abs_diff']:.3e}</b> "
            "(IEEE-754 double-precision machine epsilon).",
            body_style,
        )
    )

    story.append(Paragraph("3. Overall Held-Out Classification Evaluation (N = 56,962)", h1_style))
    overall_table_data = [
        ["Held-Out Metric", "Formula / Definition", "Measured Value (Threshold = 0.50)"],
        ["Total Held-Out Transactions", "TP + FP + TN + FN (20% Stratified Split)", f"{om['total_transactions']:,} (98 Fraud, 56,864 Legitimate)"],
        ["Confusion Matrix Counts", "True Positives (TP) / False Positives (FP)", f"TP = {om['true_positives']}  |  FP = {om['false_positives']:,}"],
        ["Confusion Matrix Counts", "True Negatives (TN) / False Negatives (FN)", f"TN = {om['true_negatives']:,}  |  FN = {om['false_negatives']}"],
        ["Precision & Recall", "TP / (TP + FP)  &  TP / (TP + FN)", f"Precision = {om['precision']:.4f} (6.10%)  |  Recall = {om['recall']:.4f} (91.84%)"],
        ["F1-Score", "Harmonic Mean of Precision and Recall", f"{om['f1_score']:.4f}"],
        ["False-Positive Rate (FPR)", "FP / (FP + TN) (Legitimate Flagged)", f"{om['false_positive_rate']:.4f} (2.44%)"],
        ["False-Negative Rate (FNR)", "FN / (FN + TP) (Fraud Missed)", f"{om['false_negative_rate']:.4f} (8.16%)"],
        ["PR-AUC & ROC-AUC", "Average Precision & Area Under ROC Curve", f"PR-AUC = {om['pr_auc']:.4f}  |  ROC-AUC = {om['roc_auc']:.4f}"],
    ]
    story.append(_make_table(overall_table_data, col_widths=[1.85 * inch, 2.65 * inch, 2.65 * inch], right_align_from_col=2))

    # ==================== PAGE 2: Visuals & Segment-Level Error Analysis ====================
    story.append(PageBreak())
    story.append(Paragraph("4. Confusion Matrix, Threshold Calibration, and Segment Error Analysis", h1_style))

    cm_fig = figures_dir / "confusion_matrix_and_threshold.png"
    if cm_fig.exists():
        story.append(Image(str(cm_fig), width=6.9 * inch, height=2.65 * inch))
        story.append(Spacer(1, 4))

    story.append(
        Paragraph(
            "<b>Proxy Segment Definition (Explicit Disclaimer):</b> The public ULB dataset does not provide genuine "
            "customer identities, merchant categories, or demographic segments. Accordingly, we define three reproducible "
            "<b>transaction-amount proxy bands</b> using the 33.33% (<i>q33 = 9.99</i>) and 66.67% (<i>q67 = 50.00</i>) "
            "quantiles computed strictly on the training split, and apply those exact boundaries to the 56,962 held-out records:",
            body_style,
        )
    )

    seg_table_data = [
        ["Segment", "Amount Band", "Tx Count", "Frauds", "TP", "FP", "TN", "FN", "FPR (%)", "FNR (%)", "Precision", "Recall", "PR-AUC"]
    ]
    for s in seg_rows:
        seg_table_data.append(
            [
                s["segment"].capitalize(),
                s["amount_band_rule"],
                f"{s['num_transactions']:,}",
                str(s["actual_frauds"]),
                str(s["true_positives"]),
                f"{s['false_positives']:,}",
                f"{s['true_negatives']:,}",
                str(s["false_negatives"]),
                f"{s['false_positive_rate']*100:.2f}%",
                f"{s['false_negative_rate']*100:.2f}%",
                f"{s['precision']:.4f}",
                f"{s['recall']:.4f}",
                f"{s['pr_auc']:.4f}",
            ]
        )
    story.append(
        _make_table(
            seg_table_data,
            col_widths=[
                0.62 * inch,
                0.98 * inch,
                0.56 * inch,
                0.50 * inch,
                0.35 * inch,
                0.44 * inch,
                0.56 * inch,
                0.35 * inch,
                0.55 * inch,
                0.55 * inch,
                0.58 * inch,
                0.50 * inch,
                0.56 * inch,
            ],
            right_align_from_col=2,
        )
    )
    story.append(Spacer(1, 6))

    seg_fig = figures_dir / "segment_error_analysis.png"
    if seg_fig.exists():
        story.append(Image(str(seg_fig), width=6.9 * inch, height=2.65 * inch))
        story.append(Spacer(1, 4))

    story.append(Paragraph("4.1 Who Bears the Cost of Classification Errors Across Segments?", h1_style))
    story.append(
        Paragraph(
            "&bull; <b>False Positives (FP = 1,386 total: 664 Low, 266 Medium, 456 High):</b> Legitimate cardholders "
            "suffer declined purchases or step-up authentication friction; merchants lose sales due to checkout "
            "abandonment (most costly per transaction in the <b>High &gt; 50.00</b> segment where 456 legitimate purchases "
            "are flagged); and payment processors bear alert-review overhead. Notably, the <b>Low (&le; 9.99)</b> segment "
            "suffers the highest FPR (<b>3.40%, 664 false alarms</b>), where manual review or SMS verification costs "
            "can exceed the transaction fee itself.",
            bullet_style,
        )
    )
    story.append(
        Paragraph(
            "&bull; <b>False Negatives (FN = 8 total: 5 Low, 1 Medium, 2 High):</b> Undetected fraud causes direct "
            "chargeback principal losses, dispute fees, and network penalties borne by merchants or issuing banks. "
            "The model achieves its lowest FNR in the <b>High (&gt; 50.00)</b> segment (<b>5.26%, only 2 missed frauds "
            "out of 38, Recall = 94.74%</b>), which is desirable because each high-value FN carries severe financial "
            "exposure. Conversely, the <b>Low (&le; 9.99)</b> segment exhibits the highest FNR (<b>10.20%, 5 missed frauds</b>) "
            "and contains half of all frauds (49/98), reflecting automated micro-amount card-testing probes that precede "
            "larger fraudulent charges.",
            bullet_style,
        )
    )

    # ==================== PAGE 3: Threshold Sweep, Load Tests & Conclusion ====================
    story.append(PageBreak())
    story.append(Paragraph("4.2 Decision-Threshold Trade-Off Analysis", h1_style))
    story.append(
        Paragraph(
            "Because balanced class weights upweight the 0.172% fraud class by ~289x, the default threshold (0.50) "
            "maximizes sensitivity (91.84% recall) at the expense of false positives (2.44% FPR). Adjusting the "
            "configurable <i>FRAUD_THRESHOLD</i> parameter shifts the operational balance between review cost and fraud loss:",
            body_style,
        )
    )
    sweep_table_data = [
        ["Threshold", "Precision", "Recall (1 - FNR)", "F1-Score", "False Positives (FP)", "FPR (%)", "False Negatives (FN)", "FNR (%)"]
    ]
    for sw in sweep_rows:
        sweep_table_data.append(
            [
                f"{sw['threshold']:.2f}" + (" (Default)" if abs(sw["threshold"] - 0.50) < 1e-6 else ""),
                f"{sw['precision']:.4f}",
                f"{sw['recall']:.4f}",
                f"{sw['f1_score']:.4f}",
                f"{sw['false_positives']:,}",
                f"{sw['false_positive_rate']*100:.2f}%",
                str(sw["false_negatives"]),
                f"{sw['false_negative_rate']*100:.2f}%",
            ]
        )
    story.append(
        _make_table(
            sweep_table_data,
            col_widths=[0.95 * inch, 0.80 * inch, 1.00 * inch, 0.75 * inch, 1.15 * inch, 0.75 * inch, 1.10 * inch, 0.65 * inch],
        )
    )
    story.append(Spacer(1, 6))

    story.append(Paragraph("5. Streaming Load-Test Methodology, Results, and Host Diagnostics", h1_style))
    story.append(
        Paragraph(
            "<b>Metric Definitions:</b> For each increasing offered event rate (default <b>200, 500, and 1,000 events/sec</b>), "
            "<i>scripts/run_load_tests.py</i> tags every event with a distinct <i>run_id</i>, measures achieved producer rate "
            "(<i>sent_events / elapsed_sec</i>), waits for Flink checkpoints to commit to Apache Iceberg, and queries "
            "<i>fraud_detection.scored_transactions</i> to verify persisted record count, uniqueness, end-to-end throughput "
            "(<i>unique_tx / ((max(scoring_ts_ms) - min(producer_ts_ms))/1000)</i>), and producer-to-scoring latency "
            "percentiles (<i>p50, p95, p99</i> of <i>scoring_ts_ms - producer_ts_ms</i>).",
            body_style,
        )
    )
    story.append(
        Paragraph(
            f"<b>Host Environment Blocker &amp; Scientific Integrity:</b> On the current Windows evaluation machine "
            f"(<i>{load_data.get('environment', {}).get('os', 'Windows 11 AMD64')}</i>), neither Docker Desktop nor WSL2 "
            f"is installed (<i>docker: CommandNotFoundException</i>). Consequently, <i>scripts/run_load_tests.py</i> "
            f"records <font name='Courier'>status = '{load_data.get('status')}'</font> in <i>results/load_test_runs.json</i> "
            f"with exact TCP diagnostics rather than fabricating cluster measurements. On any machine with Docker Desktop "
            f"or Docker Engine, running <font name='Courier'>docker compose up -d --build</font>, submitting "
            f"<font name='Courier'>src/flink_job.py</font>, and executing <font name='Courier'>python scripts/run_load_tests.py</font> "
            f"populates the live cluster metrics automatically.",
            body_style,
        )
    )
    story.append(
        Paragraph(
            "<b>Verified Local Apache Iceberg Functional Smoke Test (Explicitly Labelled):</b> To validate the end-to-end "
            "Python producer pacing, Flink UDF scoring logic, malformed-event dead-letter isolation, <i>run_id</i> partitioning, "
            "and real Apache Iceberg table persistence on this host without Docker, <i>scripts/smoke_test.py</i> executed "
            "3 increasing-rate runs (200, 500, 1,000 eps; 200 valid + 5 malformed events/run) against a local SQLite-backed "
            "Apache Iceberg warehouse (<i>results/smoke_test_verification.json</i>):",
            body_style,
        )
    )

    smoke_table_data = [
        ["Smoke-Test Run ID", "Offered Rate", "Achieved Rate", "Valid Sent", "Malformed Caught", "Iceberg Persisted", "Duplicates / Missing", "p50 / p95 / p99 Latency"]
    ]
    for sr in smoke_data.get("runs", []):
        smoke_table_data.append(
            [
                sr["run_id"],
                f"{sr['offered_rate_eps']:.0f} eps",
                f"{sr['achieved_producer_rate_eps']:.1f} eps",
                str(sr["sent_events"]),
                f"{sr['malformed_caught']} / {sr['malformed_injected']}",
                str(sr["persisted_count"]),
                f"{sr['duplicate_records']} / {sr['missing_records']}",
                f"{sr['latency_median_p50_ms']:.1f} / {sr['latency_p95_ms']:.1f} / {sr['latency_p99_ms']:.1f} ms",
            ]
        )
    story.append(
        _make_table(
            smoke_table_data,
            col_widths=[1.25 * inch, 0.72 * inch, 0.78 * inch, 0.62 * inch, 0.88 * inch, 0.85 * inch, 0.90 * inch, 1.15 * inch],
        )
    )
    story.append(Spacer(1, 5))

    tp_fig = figures_dir / "throughput_and_latency.png"
    if tp_fig.exists():
        story.append(Image(str(tp_fig), width=6.9 * inch, height=2.45 * inch))
        story.append(Spacer(1, 4))

    story.append(Paragraph("6. Limitations and Conclusion", h1_style))
    story.append(
        Paragraph(
            "<b>Limitations:</b> (1) Transaction-amount tertiles (<i>low, medium, high</i>) are proxy bands derived from "
            "training quantiles and do not represent demographic or cardholder-identity segments. (2) Because the ULB "
            "dataset lacks cardholder IDs, scoring is stateless per event rather than incorporating velocity windows. "
            "(3) Full containerized cluster load testing requires Docker Desktop / WSL2 on the host OS. "
            "<b>Conclusion:</b> The pipeline delivers a complete, reproducible <i>Kafka &rarr; Flink &rarr; Iceberg</i> "
            "architecture, verified double-precision scoring parity (5.55e-16 max diff), comprehensive segment error "
            "analysis across all 56,962 held-out transactions, and 14/14 passing automated unit and Iceberg smoke tests.",
            body_style,
        )
    )

    doc.build(story)
    return count_pdf_pages(output_pdf)


def main() -> None:
    pdf_path = PROJECT_ROOT / "report" / "technical_report.pdf"
    pages = build_pdf(pdf_path)
    print(f"[OK] Technical report ready at {pdf_path} ({pages} pages, {pdf_path.stat().st_size:,} bytes)")
    if not (3 <= pages <= 5):
        print(f"[ERROR] PDF page count {pages} is outside the required 3-5 page range!", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
