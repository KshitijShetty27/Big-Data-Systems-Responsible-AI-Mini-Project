"""Generate RUN_COMMANDS.pdf and PRESENTATION_SCRIPT.pdf (plus DEMO_GUIDE.pdf) with embedded commands."""

from __future__ import annotations

from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _get_styles():
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "GuideTitle",
        parent=styles["Heading1"],
        fontSize=15,
        leading=18,
        textColor=colors.HexColor("#1a365d"),
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        "GuideSubtitle",
        parent=styles["Normal"],
        fontSize=9.5,
        leading=12,
        textColor=colors.HexColor("#4a5568"),
        spaceAfter=8,
    )
    h1_style = ParagraphStyle(
        "GuideH1",
        parent=styles["Heading2"],
        fontSize=11.5,
        leading=14,
        textColor=colors.HexColor("#1a365d"),
        spaceBefore=7,
        spaceAfter=4,
    )
    h2_style = ParagraphStyle(
        "GuideH2",
        parent=styles["Heading3"],
        fontSize=10.0,
        leading=13,
        textColor=colors.HexColor("#2b6cb0"),
        spaceBefore=6,
        spaceAfter=3,
    )
    body_style = ParagraphStyle(
        "GuideBody",
        parent=styles["Normal"],
        fontSize=9.1,
        leading=12.6,
        spaceAfter=4,
    )
    quote_style = ParagraphStyle(
        "GuideQuote",
        parent=body_style,
        leftIndent=10,
        rightIndent=8,
        textColor=colors.HexColor("#1a202c"),
        backColor=colors.HexColor("#edf2f7"),
        borderPadding=5,
        spaceBefore=3,
        spaceAfter=5,
    )
    code_style = ParagraphStyle(
        "GuideCode",
        fontName="Courier-Bold",
        fontSize=8.4,
        leading=11.0,
        textColor=colors.HexColor("#1e3a8a"),
        backColor=colors.HexColor("#f1f5f9"),
        leftIndent=8,
        rightIndent=8,
        borderPadding=5,
        spaceBefore=2,
        spaceAfter=5,
    )
    return title_style, subtitle_style, h1_style, h2_style, body_style, quote_style, code_style


def _make_table(data, col_widths=None):
    styles = getSampleStyleSheet()
    header_cell_style = ParagraphStyle(
        "TblHeaderCell",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8.2,
        leading=10.5,
        textColor=colors.white,
    )
    body_cell_style = ParagraphStyle(
        "TblBodyCell",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.1,
        leading=10.8,
        textColor=colors.HexColor("#1a202c"),
    )

    wrapped_data = []
    for row_idx, row in enumerate(data):
        wrapped_row = []
        for cell in row:
            if isinstance(cell, Paragraph):
                wrapped_row.append(cell)
            else:
                style = header_cell_style if row_idx == 0 else body_cell_style
                wrapped_row.append(Paragraph(str(cell), style))
        wrapped_data.append(wrapped_row)

    t = Table(wrapped_data, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a365d")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e0")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7fafc")]),
                ("TOPPADDING", (0, 0), (-1, -1), 4.5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4.5),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return t


def build_run_commands_pdf(output_pdf: Path) -> None:
    title_s, sub_s, h1_s, h2_s, body_s, _, code_s = _get_styles()
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.50 * inch,
        bottomMargin=0.50 * inch,
        title="Step-by-Step Run Commands Guide",
        author="Kushal G (PES1UG24AM145) & Kshitij Satish Shetty (PES1UG24AM143)",
    )
    story = []

    story.append(Paragraph("Step-by-Step Commands to Run the Project", title_s))
    story.append(
        Paragraph(
            "<b>Team Members:</b> 1. Kushal G (PES1UG24AM145) &nbsp;|&nbsp; 2. Kshitij Satish Shetty (PES1UG24AM143)<br/>"
            "Open PowerShell or Command Prompt (CMD) inside the <b>BigData_Fraud_Detection</b> folder and run these commands in order.",
            sub_s,
        )
    )

    story.append(Paragraph("PART A: Main Commands to Run on Any Laptop (No Docker Needed)", h1_s))

    cmds = [
        (
            "Command 1: Run Automated Tests (Checks Code & Model Accuracy)",
            "python -m pytest -v",
            "<b>What it does:</b> Checks that transaction JSON data is valid, proves the 'Class' fraud answer is never leaked "
            "into model inputs, checks that Flink gives the exact same fraud probabilities as scikit-learn, and checks Apache Iceberg storage.<br/>"
            "<b>What you will see:</b> <font name='Courier'>14 passed</font> in green.",
        ),
        (
            "Command 2: Run the 3-Speed Streaming Test into Apache Iceberg",
            "python scripts/smoke_test.py",
            "<b>What it does:</b> Streams 600 test transactions at 3 increasing speeds (<b>200, 500, and 1,000 transactions/sec</b>) "
            "through the Flink scoring function into a local <b>Apache Iceberg</b> table. Also injects 5 corrupted messages per run "
            "to prove our system isolates bad data without crashing.<br/>"
            "<b>What you will see:</b> <font name='Courier'>[OK] Smoke test PASSED across 3 rates: 600 total valid events persisted to Apache Iceberg...</font>",
        ),
        (
            "Command 3: Print Fraud Model Accuracy & 3-Segment Table",
            "python scripts/run_evaluation.py",
            "<b>What it does:</b> Evaluates all <b>56,962 held-out transactions</b> (containing 98 actual frauds) and prints the "
            "Confusion Matrix (TP=90, FP=1386, TN=55478, FN=8), Recall (91.84%), False-Positive Rate (2.44%), and the "
            "<b>Low (&le; $9.99)</b>, <b>Medium ($9.99–$50.00)</b>, and <b>High (&gt; $50.00)</b> segment error table.",
        ),
        (
            "Command 4: Verify the 4-Page Technical Report PDF",
            "python scripts/generate_report.py",
            "<b>What it does:</b> Verifies (or rebuilds) the 4-page <font name='Courier'>report/technical_report.pdf</font> report.",
        ),
        (
            "Command 5: Launch the Simple Presentation Web UI (Frontend)",
            "python scripts/presentation_ui.py",
            "<b>What it does:</b> Starts the simple web dashboard at <b>http://localhost:8501</b> and opens it automatically in "
            "your browser so you can show live transaction fraud checking, segment tables, and load-test runs.<br/>"
            "<i>(Press Ctrl + C in the terminal when you want to stop the UI server.)</i>",
        ),
    ]

    for title, cmd, desc in cmds:
        story.append(Paragraph(title, h2_s))
        story.append(Preformatted(cmd, code_s))
        story.append(Paragraph(desc, body_s))

    story.append(Paragraph("PART B: Optional Commands (Download Dataset &amp; Train From Scratch)", h1_s))
    story.append(
        Preformatted(
            "# 1. Download the 150 MB creditcard.csv dataset from Zenodo\n"
            "python scripts/download_dataset.py\n\n"
            "# 2. Train Logistic Regression model on 80% data and test on 20% data\n"
            "python scripts/train_model.py --threshold 0.50",
            code_s,
        )
    )

    story.append(Paragraph("PART C: Full Docker Cluster Commands (If Docker Desktop Is Running)", h1_s))
    story.append(
        Preformatted(
            "# 1. Start Kafka, Flink, Iceberg REST Catalog, and MinIO containers\n"
            "docker compose up -d --build\n"
            "docker compose ps\n\n"
            "# 2. Create the Kafka topic for transactions\n"
            "docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh \\\n"
            "  --bootstrap-server kafka:9092 --create --if-not-exists \\\n"
            "  --topic transactions.raw.v1 --partitions 2 --replication-factor 1\n\n"
            "# 3. Submit the Flink real-time fraud scoring job inside the Flink cluster\n"
            "docker exec -d fraud-flink-jobmanager ./bin/flink run \\\n"
            "  -py /opt/app/src/flink_job.py --bootstrap-servers kafka:9092 \\\n"
            "  --iceberg-rest-uri http://iceberg-rest:8181 --s3-endpoint http://minio:9000\n\n"
            "# 4. Run the 3-speed load test (200, 500, 1000 transactions/sec) & inspect Iceberg\n"
            "python scripts/run_load_tests.py --rates 200,500,1000 --events-per-run 3000\n"
            "python scripts/verify_iceberg.py --limit 10\n\n"
            "# 5. Stop Docker containers when finished\n"
            "docker compose down -v --remove-orphans",
            code_s,
        )
    )

    doc.build(story)


def build_presentation_script_pdf(output_pdf: Path) -> None:
    title_s, sub_s, h1_s, h2_s, body_s, quote_s, code_s = _get_styles()
    doc = SimpleDocTemplate(
        str(output_pdf),
        pagesize=A4,
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
        title="Complete Presentation Script with Commands (Start to Finish)",
        author="Kushal G (PES1UG24AM145) & Kshitij Satish Shetty (PES1UG24AM143)",
    )
    story = []

    story.append(Paragraph("Complete Presentation Script with Commands (Start to Finish)", title_s))
    story.append(
        Paragraph(
            "<b>Team Members:</b> 1. Kushal G (PES1UG24AM145) &nbsp;|&nbsp; 2. Kshitij Satish Shetty (PES1UG24AM143)<br/>"
            "Written in simple, clear English with the exact terminal commands inside every step.",
            sub_s,
        )
    )

    # Pre-Demo Setup
    story.append(Paragraph("Before Your Demo Starts (Run This 1 Command Before the Teacher Comes)", h1_s))
    story.append(
        Paragraph(
            "Open <b>Terminal #1</b> (PowerShell or CMD) inside <font name='Courier'>BigData_Fraud_Detection</font> and start the Simple Web UI:",
            body_s,
        )
    )
    story.append(Preformatted("python scripts/presentation_ui.py", code_s))
    story.append(
        Paragraph(
            "<i>(This opens <b>http://localhost:8501</b> in your browser. Open a <b>second terminal window (Terminal #2)</b> "
            "inside <font name='Courier'>BigData_Fraud_Detection</font> to run the live commands below.)</i>",
            body_s,
        )
    )

    # STEP 1
    story.append(Paragraph("STEP 1 (0:00 – 0:45) | Introduction &amp; Pipeline Flow — Kushal G (PES1UG24AM145)", h2_s))
    story.append(Paragraph("<b>What to Show on Screen:</b> Top banner of the Simple Web UI (<b>http://localhost:8501</b>).", body_s))
    story.append(
        Paragraph(
            "&ldquo;Good morning Sir/Ma'am. We are <b>Kushal (PES1UG24AM145)</b> and <b>Kshitij (PES1UG24AM143)</b>. "
            "Our project is a <b>Real-Time Credit Card Fraud Detection Pipeline</b>.<br/>"
            "When a customer swipes a credit card, the payment company has less than a second to decide whether it is a "
            "genuine purchase or fraud. Checking transactions in slow daily batches is too late because the money is already "
            "gone. So we built a 4-stage real-time streaming pipeline:<br/>"
            "1. First, we use the <b>public ULB Credit Card Fraud dataset</b>, which contains <b>284,807 credit card swipes</b> "
            "and <b>492 actual frauds</b> (only 0.17% are fraud—about 1 fraud in every 580 swipes).<br/>"
            "2. Second, our producer script replays the held-out test transactions as a live stream into <b>Apache Kafka</b>, "
            "which acts as our high-speed message pipe.<br/>"
            "3. Third, <b>Apache Flink</b> reads the stream non-stop, checks that the event data is valid, and scores each "
            "transaction in real time using our trained <b>Logistic Regression fraud model</b>.<br/>"
            "4. Fourth, Flink saves every scored transaction—along with its fraud probability and how many milliseconds "
            "scoring took—into an <b>Apache Iceberg</b> data warehouse table backed by MinIO storage.&rdquo;",
            quote_s,
        )
    )

    # STEP 2
    story.append(Paragraph("STEP 2 (0:45 – 1:30) | Model Training, No-Cheating Proof &amp; Code Tests — Kushal G (PES1UG24AM145)", h2_s))
    story.append(Paragraph("<b>Command to Run in Terminal #2:</b>", body_s))
    story.append(Preformatted("python -m pytest -v", code_s))
    story.append(
        Paragraph(
            "&ldquo;For our fraud model, we split the dataset <b>80% for training (227,845 rows)</b> and <b>20% for testing "
            "(56,962 rows)</b> using a fixed random seed (42) so our results are 100% reproducible. Because frauds are so rare, "
            "we used <b>StandardScaler</b> and <b>Logistic Regression with balanced class weights</b> so the model pays extra "
            "attention to fraud cases.<br/>"
            "We followed two strict rules:<br/>"
            "&bull; First, we <b>strictly excluded the 'Class' fraud answer</b> from all model inputs so the model can never "
            "cheat. If any message tries to pass 'Class' inside the input features, our validation guard immediately rejects it.<br/>"
            "&bull; Second, we exported the fitted scaling numbers and model weights into a JSON artifact "
            "(<font name='Courier'>models/fraud_model_artifact.json</font>) so Apache Flink calculates the exact same fraud "
            "probability natively. As you can see from our <b>14 automated tests passing in green</b>, Flink's math matches "
            "scikit-learn down to 16 decimal places (5.55 &times; 10<sup>-16</sup>).&rdquo;",
            quote_s,
        )
    )

    story.append(PageBreak())

    # STEP 3
    story.append(Paragraph("STEP 3 (1:30 – 2:15) | 3-Speed Streaming Load Test &amp; Iceberg Storage — Kshitij (PES1UG24AM143)", h2_s))
    story.append(
        Paragraph(
            "<b>Command to Run in Terminal #2</b> <i>(or click <b>'▶ Run 3-Rate Test Now'</b> in Section 3 of the Web UI)</i>:",
            body_s,
        )
    )
    story.append(Preformatted("python scripts/smoke_test.py", code_s))
    story.append(
        Paragraph(
            "&ldquo;Next, we tested how our pipeline handles increasing traffic speeds. We replayed held-out transactions at "
            "three increasing rates: <b>200, 500, and 1,000 transactions per second</b>.<br/>"
            "Every test run is tagged with its own <b>run_id</b>—and our Apache Iceberg table is partitioned by <b>run_id</b>—so "
            "transactions from different runs never mix. For every transaction, we subtract the producer send timestamp from "
            "the Flink scoring timestamp to measure the real latency in milliseconds (p50 median, p95, and p99).<br/>"
            "We also injected <b>5 corrupted/malformed events</b> in each run (such as negative transaction amounts or missing "
            "features). Instead of crashing the pipeline, Flink catches those 5 bad events and isolates them into a separate "
            "<b>malformed_events</b> table, while saving <b>100% of the valid transactions</b> into Apache Iceberg with "
            "<b>0 lost and 0 duplicate records</b>.&rdquo;",
            quote_s,
        )
    )

    # STEP 4
    story.append(Paragraph("STEP 4 (2:15 – 3:00) | Accuracy, Amount Segments &amp; Who Pays the Cost — Kshitij (PES1UG24AM143)", h2_s))
    story.append(
        Paragraph(
            "<b>Command to Run in Terminal #2</b> <i>(also show Section 1 &amp; Section 2 in the Web UI at http://localhost:8501)</i>:",
            body_s,
        )
    )
    story.append(Preformatted("python scripts/run_evaluation.py", code_s))
    story.append(
        Paragraph(
            "&ldquo;Finally, we evaluated our model on all <b>56,962 held-out test transactions</b>, which contain <b>98 real "
            "frauds</b> and <b>56,864 normal swipes</b>.<br/>"
            "Overall, at our default 0.50 threshold, the model catches <b>90 out of 98 frauds (91.84% Recall)</b> and misses "
            "only 8, with a <b>False-Positive Rate of just 2.44%</b> (1,386 false alarms) and a <b>PR-AUC of 0.7190</b>.<br/>"
            "Because the public ULB dataset is anonymized for privacy and does not have customer names or ages, we grouped "
            "transactions into <b>3 proxy segments based on Transaction Amount</b> using training-data quantiles "
            "(<b>q33 = $9.99</b> and <b>q67 = $50.00</b>):<br/>"
            "1. <b>Low Amount (&le; $9.99):</b> Highest <b>False-Positive Rate (3.40%, 664 false alarms)</b> and highest "
            "<b>False-Negative Rate (10.20%, 5 missed frauds)</b>. Half of all frauds (49 out of 98) happen here because "
            "hackers test stolen cards with tiny $1 swipes first. Here, <b>payment companies</b> pay the cost because reviewing "
            "a $2 false alarm costs more than the transaction fee, and <b>customers</b> get annoyed when small everyday "
            "purchases are blocked.<br/>"
            "2. <b>Medium Amount ($9.99 to $50.00):</b> Lowest False-Positive Rate (<b>1.42%</b>) and highest <b>PR-AUC (0.8423)</b>.<br/>"
            "3. <b>High Amount (&gt; $50.00):</b> Best fraud detection—catching <b>94.74% of frauds</b> (only <b>5.26% "
            "False-Negative Rate</b>, missing just 2 out of 38 frauds). In high-amount purchases, <b>merchants and banks</b> "
            "bear huge financial chargeback losses if a fraud is missed (False Negative), while <b>merchants</b> also lose big "
            "sales if a real customer is falsely blocked (False Positive).<br/>"
            "If a bank wants fewer false alarms, our threshold is configurable—raising it from 0.50 to 0.85 cuts false alarms "
            "by <b>72%</b> while still catching <b>89%</b> of all frauds. Thank you!&rdquo;",
            quote_s,
        )
    )

    # Quick Command Summary Box
    story.append(Paragraph("Quick Summary of All Commands (In Order)", h1_s))
    story.append(
        Preformatted(
            "python -m pytest -v                  # 1. Run all 14 automated unit & verification tests\n"
            "python scripts/smoke_test.py         # 2. Run 3-speed (200, 500, 1000 eps) Apache Iceberg test\n"
            "python scripts/run_evaluation.py     # 3. Print accuracy (56,962 rows) & Low/Medium/High segments\n"
            "python scripts/generate_report.py    # 4. Verify the 4-page technical report PDF\n"
            "python scripts/presentation_ui.py    # 5. Launch Simple Web UI at http://localhost:8501",
            code_s,
        )
    )

    # Common Viva Q&A
    story.append(Paragraph("Common Teacher Viva Questions &amp; Simple Answers", h1_s))
    viva_rows = [
        ["Teacher Question", "Simple, Clear Answer to Give"],
        [
            "1. Why use PR-AUC (0.719) instead of normal Accuracy?",
            "Because 99.83% of swipes are normal! A dummy model that says 'normal' for everything gets 99.83% accuracy while catching 0 frauds. PR-AUC measures how well we catch the rare 0.17% frauds without raising too many false alarms.",
        ],
        [
            "2. Why is Recall 91.84% while Precision is 6.10%?",
            "Because there are 578 normal swipes for every 1 fraud. Even a tiny 2.44% false-alarm rate on 56,864 normal swipes is 1,386 false alarms vs. 90 real frauds (90 / (90+1386) = 6.1%). Raising threshold to 0.85–0.95 boosts Precision to 39% and drops FPR to 0.24%.",
        ],
        [
            "3. Why group segments by Transaction Amount?",
            "The ULB dataset hides personal customer details using PCA (V1–V28) for privacy, so there are no customer IDs or demographics. We used training-data Amount quantiles ($9.99 and $50.00) as reproducible transaction-amount proxies.",
        ],
        [
            "4. Who pays for False Positives vs. False Negatives?",
            "False Positives (1,386): Cardholders get blocked at checkout, merchants lose sales, and payment companies pay review costs. False Negatives (8): Merchants and banks lose the stolen money and pay chargeback fees.",
        ],
        [
            "5. How do you stop different load-test runs from mixing?",
            "Every event has a unique run_id (e.g. 200eps, 500eps, 1000eps), and the Apache Iceberg table is partitioned by run_id.",
        ],
    ]
    story.append(_make_table(viva_rows, col_widths=[2.15 * inch, 5.00 * inch]))

    doc.build(story)


if __name__ == "__main__":
    run_pdf = PROJECT_ROOT / "RUN_COMMANDS.pdf"
    pres_pdf = PROJECT_ROOT / "PRESENTATION_SCRIPT.pdf"
    demo_pdf = PROJECT_ROOT / "DEMO_GUIDE.pdf"
    report_demo_pdf = PROJECT_ROOT / "report" / "demo_guide.pdf"

    build_run_commands_pdf(run_pdf)
    build_presentation_script_pdf(pres_pdf)
    build_presentation_script_pdf(demo_pdf)
    build_presentation_script_pdf(report_demo_pdf)

    print(f"[OK] Generated {run_pdf} ({run_pdf.stat().st_size:,} bytes)")
    print(f"[OK] Generated {pres_pdf} ({pres_pdf.stat().st_size:,} bytes)")
    print(f"[OK] Generated {demo_pdf} ({demo_pdf.stat().st_size:,} bytes)")
