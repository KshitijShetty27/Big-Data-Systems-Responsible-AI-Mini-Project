# Real-Time Credit Card Fraud Detection Pipeline: Streaming Architecture, Performance Methodology, and Segment-Level Error Analysis

**Course Mini-Project Technical Report**
- **Team Member 1:** Kushal G (`PES1UG24AM145`)
- **Team Member 2:** Kshitij Satish Shetty (`PES1UG24AM143`)

---

## 1. Problem Statement and Streaming Architecture

Payment processors must evaluate incoming credit card authorization requests in real time to intercept fraudulent transactions before settlement while minimizing friction for legitimate cardholders. Batch scoring introduces unacceptable delay during point-of-sale and e-commerce checkout, whereas streaming architectures allow sub-second feature standardization, linear probability inference, and transactional lakehouse persistence.

This project implements a complete, cross-platform, containerized streaming fraud detection pipeline following the required architecture:

$$\text{Public ULB Credit Card Dataset} \longrightarrow \text{Apache Kafka} \longrightarrow \text{Apache Flink} \longrightarrow \text{Fraud Model Scoring} \longrightarrow \text{Apache Iceberg}$$

### 1.1 Component Responsibilities and Pinned Stack

To guarantee reproducibility across Windows 10/11, macOS (Intel and Apple Silicon ARM64), and Linux on a CPU-only machine with ~12 GB RAM, all services are orchestrated via Docker Compose v2 (`docker-compose.yml`) using multi-architecture (`linux/amd64` and `linux/arm64`) images and internal Docker service DNS names:

1. **Transaction Replay Producer (`src/producer.py`)**: Reads the held-out evaluation split (`data/processed/heldout_transactions.csv`, $N = 56,962$) and publishes JSON events keyed by `transaction_id` to Kafka topic `transactions.raw.v1` at configurable offered rates ($200, 500, 1000$ events/sec) using high-precision token-bucket pacing.
2. **Apache Kafka (`apache/kafka:3.7.1`, KRaft Mode)**: Ingests raw transaction events without requiring a separate ZooKeeper ensemble, exposing `kafka:9092` internally to containers and `localhost:29092` to host tooling (`-Xmx512M -Xms256M`).
3. **Apache Flink (`flink:1.18.1-scala_2.12-java11`, JobManager & TaskManager)**: Executes the continuous PyFlink streaming job (`src/flink_job.py`) with 5-second checkpointing (`execution.checkpointing.interval: 5000ms`), bundled with pinned Maven connectors (`flink-sql-connector-kafka-3.1.0-1.18.jar`, `iceberg-flink-runtime-1.18-1.5.2.jar`, `iceberg-aws-bundle-1.5.2.jar`, and `hadoop-client-*-3.3.6.jar`).
4. **In-Stream Fraud Model Scoring (`src/scoring.py`)**: Inside the Flink Python UDF (`score_transaction_udf`), each raw JSON payload is schema-validated, standardized using exported `StandardScaler` parameters, scored via exact logistic regression dot-product + sigmoid math, assigned a transaction-amount segment (`low`, `medium`, `high`), and stamped with `scoring_ts_ms` and `latency_ms = scoring_ts_ms - producer_ts_ms`. Malformed payloads are never dropped silently or allowed to crash the pipeline; they are routed via a Flink `StatementSet` to `iceberg_catalog.fraud_detection.malformed_events`.
5. **Apache Iceberg REST Catalog (`tabulario/iceberg-rest:0.6.0`) & MinIO (`minio/minio:RELEASE.2024-06-13T22-53-53Z`)**: Persists scored records in Format Version 2 Parquet files partitioned by `run_id` inside `s3://warehouse/fraud_detection/scored_transactions`, ensuring zero mixing between separate load-test runs.

---

## 2. Dataset, Preprocessing, and Model Methodology

### 2.1 Public ULB Credit Card Fraud Dataset

We use the public ULB Machine Learning Group Credit Card Fraud dataset (`data/raw/creditcard.csv`, downloaded from Zenodo Record `7395559`, MD5 `e90efcb83d69faf99fcab8b0255024de`, SHA-256 `76274b691b16a6c4...`). The dataset comprises **284,807 transactions** recorded over 48 hours, with **492 actual frauds** (`0.1727%` positive class) and **284,315 legitimate transactions** (`99.8273%` negative class).

Each record contains 30 numeric input features (`Time`, PCA components `V1`–`V28`, and `Amount`) plus the binary target column `Class` (`1` = fraud, `0` = legitimate). To prevent target leakage:
- `Class` is strictly excluded from the feature matrix $X$ during training and from the `features` object in Kafka events.
- `parse_and_validate_event()` in `src/schema.py` explicitly rejects any event payload containing `"Class"` inside `features` (`target_leakage_class_in_features`).
- The actual fraud label is carried only as an external evaluation metadata field (`actual_label`) for post-hoc accuracy computation.

### 2.2 Stratified Split, Model Training, and Mathematical Parity

Using a fixed random seed (`random_seed = 42`), we perform an 80/20 stratified train/test split:
- **Training Split**: $227,845$ transactions ($394$ frauds, $0.1729\%$ fraud rate).
- **Held-Out Evaluation Split**: $56,962$ transactions ($98$ frauds, $0.1720\%$ fraud rate), saved to `data/processed/heldout_transactions.csv`.

To handle the $1:578$ class imbalance on a CPU-only budget, we train a `scikit-learn` pipeline combining `StandardScaler()` and `LogisticRegression(class_weight="balanced", C=1.0, solver="lbfgs", max_iter=1000, random_state=42)`. The fitted parameters ($\mu_j, \sigma_j, w_j$ for $j \in \{1,\dots,30\}$, intercept $b$, and training-split `Amount` tertiles $q_{33} = 9.99, q_{67} = 50.00$) are exported to `models/fraud_model_artifact.json`.

Inside Flink (`src/scoring.py`), the probability is computed using the numerically stable formulation:

$$z = b + \sum_{j=1}^{30} w_j \left(\frac{x_j - \mu_j}{\sigma_j}\right), \qquad \hat{p}(y=1 \mid \mathbf{x}) = \begin{cases} \frac{1}{1 + e^{-z}} & \text{if } z \ge 0 \\ \frac{e^z}{1 + e^z} & \text{if } z < 0 \end{cases}$$

Validating `LinearFraudModelArtifact.predict_proba_single` against `scikit-learn` `predict_proba` across a 250-record held-out fixture yields a maximum absolute difference of **$5.551 \times 10^{-16}$**, confirming exact IEEE-754 double-precision equivalence.

---

## 3. Overall Held-Out Classification Evaluation

Evaluating the trained model on all **56,962 held-out transactions** at the default configurable threshold $\tau = 0.50$ produces the following results (`results/evaluation_metrics.json`):

| Evaluation Metric | Formula / Definition | Measured Value ($\tau = 0.50$) |
|:------------------|:---------------------|-------------------------------:|
| **Total Held-Out Transactions** | $N = \text{TP} + \text{FP} + \text{TN} + \text{FN}$ | **56,962** (98 Fraud, 56,864 Legit) |
| **True Positives (TP)** | Fraudulent transactions correctly flagged ($\hat{y}=1, y=1$) | **90** |
| **False Positives (FP)** | Legitimate transactions incorrectly flagged ($\hat{y}=1, y=0$) | **1,386** |
| **True Negatives (TN)** | Legitimate transactions correctly cleared ($\hat{y}=0, y=0$) | **55,478** |
| **False Negatives (FN)** | Fraudulent transactions missed ($\hat{y}=0, y=1$) | **8** |
| **Precision** | $\text{TP} / (\text{TP} + \text{FP})$ | **0.0610** (6.10%) |
| **Recall (Sensitivity)** | $\text{TP} / (\text{TP} + \text{FN})$ | **0.9184** (91.84%) |
| **F1-Score** | $2 \cdot \text{Precision} \cdot \text{Recall} / (\text{Precision} + \text{Recall})$ | **0.1144** |
| **False-Positive Rate (FPR)** | $\text{FP} / (\text{FP} + \text{TN})$ | **0.0244** (2.44%) |
| **False-Negative Rate (FNR)** | $\text{FN} / (\text{FN} + \text{TP})$ | **0.0816** (8.16%) |
| **PR-AUC (Average Precision)** | Area under Precision–Recall curve | **0.7190** |
| **ROC-AUC** | Area under Receiver Operating Characteristic curve | **0.9721** |

---

## 4. Segment-Level Error Analysis and Cost Distribution

Because the public ULB dataset is PCA-anonymized and contains no genuine customer identities, merchant categories, or demographic attributes, we define three reproducible **transaction-amount proxy segments** using the 33.33% ($q_{33} = 9.99$) and 66.67% ($q_{67} = 50.00$) quantiles computed exclusively on the training split:

| Proxy Segment | Amount Boundary Rule | Held-Out Tx ($N$) | Actual Frauds | TP | FP | TN | FN | FPR (%) | FNR (%) | Precision | Recall | F1-Score | PR-AUC |
|:--------------|:---------------------|------------------:|--------------:|---:|----:|-------:|---:|--------:|--------:|----------:|-------:|---------:|-------:|
| **Low** | $\text{Amount} \le 9.99$ | 19,582 | 49 | 44 | 664 | 18,869 | 5 | **3.40%** | **10.20%** | 0.0621 | 0.8980 | 0.1162 | 0.8021 |
| **Medium** | $9.99 < \text{Amount} \le 50.00$ | 18,686 | 11 | 10 | 266 | 18,409 | 1 | **1.42%** | **9.09%** | 0.0362 | 0.9091 | 0.0697 | 0.8423 |
| **High** | $\text{Amount} > 50.00$ | 18,694 | 38 | 36 | 456 | 18,200 | 2 | **2.44%** | **5.26%** | 0.0732 | 0.9474 | 0.1358 | 0.6286 |

### 4.1 Who Bears the Cost of Classification Errors Across Segments?

1. **False Positives ($\text{FP} = 1,386$ total; $664$ Low, $266$ Medium, $456$ High)**:
   - **Cardholders** face immediate purchase declines or step-up authentication delays at checkout, degrading customer experience.
   - **Merchants** suffer cart abandonment and lost sales revenue when legitimate buyers are declined—an impact that is most acute in the **High ($\text{Amount} > 50.00$) segment** where $456$ legitimate higher-ticket purchases are flagged.
   - **Payment Processors & Issuing Banks** absorb alert-investigation and customer-support costs. In the **Low ($\text{Amount} \le 9.99$) segment**, where FPR peaks at **3.40% ($664$ false alarms)**, the operational cost of human review or SMS/OTP verification can exceed the transaction margin itself.
2. **False Negatives ($\text{FN} = 8$ total; $5$ Low, $1$ Medium, $2$ High)**:
   - **Merchants and Issuing Banks** bear direct principal chargeback losses, dispute processing fees, and card-network fraud-monitoring penalties when fraudulent transactions pass undetected.
   - In the **High ($\text{Amount} > 50.00$) segment**, the model achieves its lowest FNR (**5.26%, only 2 missed frauds out of 38**, Recall = **94.74%**). However, each missed fraud in this band (median amount $125.33$, max $12,910.93$) carries far greater direct financial loss per incident than a missed micro-transaction.
   - Conversely, the **Low ($\text{Amount} \le 9.99$) segment** contains half of all held-out frauds ($49$ of $98$) and experiences the highest FNR (**10.20%, 5 missed frauds**). Attackers frequently use sub-$10 transactions for automated "card testing" to validate stolen card numbers before executing large fraudulent purchases; thus, missing low-value probes creates downstream exposure across higher segments.
3. **Threshold Trade-Offs (`results/evaluation_metrics.json`)**:
   - Raising the decision threshold from $\tau = 0.50$ to $\tau = 0.85$ reduces false positives by $72\%$ (from $1,386$ to $384$, lowering FPR from $2.44\%$ to $0.68\%$) while increasing false negatives by only $3$ transactions (from $8$ to $11$, Recall = $88.78\%$, F1 = $0.3058$). At $\tau = 0.95$, FPR drops tenfold to $0.24\%$ ($136$ FPs, Precision = $39.01\%$, F1 = $0.5421$) with the same $11$ false negatives.

---

## 5. Streaming Load-Test Methodology, Telemetry, and Environment Diagnostics

### 5.1 Metric Definitions (`scripts/run_load_tests.py`)

Each load-test run $k \in \{1, 2, 3\}$ is assigned a unique `run_id` (`loadrun_{k}_{rate}eps_{timestamp}`) and evaluated on five core metrics:
1. **Configured / Offered Rate ($R_{\text{offered}}$)**: Target event emission rate in events/sec (default: $200, 500, 1000$ eps).
2. **Achieved Producer Rate ($R_{\text{achieved}}$)**: $\text{sent\_events} / (t_{\text{flush\_end}} - t_{\text{start}})$ measured upon Kafka producer flush completion.
3. **Persisted Record Count ($N_{\text{persisted}}$)**: Count of valid records committed to `iceberg_catalog.fraud_detection.scored_transactions` for `run_id`, plus integrity checks (`unique_transaction_ids`, `duplicate_records`, `missing_records`, and isolated `malformed_events`).
4. **End-to-End Throughput ($T_{\text{e2e}}$)**: $\text{unique\_transaction\_ids} / ((\max(\text{scoring\_ts\_ms}) - \min(\text{producer\_ts\_ms})) / 1000)$.
5. **End-to-End Latency Percentiles ($p_{50}, p_{95}, p_{99}$)**: Empirical percentiles of $\text{latency\_ms} = \text{scoring\_ts\_ms} - \text{producer\_ts\_ms}$ queried directly from the persisted Apache Iceberg table.

### 5.2 Execution Status on Development Host & Functional Smoke-Test Verification

- **Host Environment Blocker for Containerized Cluster**: On the current Windows 10/11 development host (`Windows-11-10.0.26200-SP0`, `AMD64`, Python `3.13.7`), neither Docker Desktop nor Windows Subsystem for Linux (WSL2) is installed (`docker: CommandNotFoundException`). Consequently, when `python scripts/run_load_tests.py` probes `localhost:29092` (Kafka) and `localhost:8181` (Iceberg REST), it records `status = "BLOCKED_DOCKER_UNAVAILABLE"` in `results/load_test_runs.json` and `results/run_metadata.json` rather than fabricating cluster telemetry.
- **Recovery Steps on Any Docker-Enabled Machine**: Install/start Docker Desktop (Windows/macOS) or Docker Engine (Linux), run `docker compose up -d --build`, submit the Flink job via `docker exec -d fraud-flink-jobmanager ./bin/flink run -py /opt/app/src/flink_job.py`, and execute `python scripts/run_load_tests.py --rates 200,500,1000 --events-per-run 3000`.
- **Verified Local Apache Iceberg Functional Smoke Test (`results/smoke_test_verification.json`)**: To verify the producer pacing, Flink UDF scoring logic, malformed-event dead-letter isolation, `run_id` partitioning, and real Apache Iceberg table commits/queries on this machine without Docker, `python scripts/smoke_test.py` executes a 3-rate ($200, 500, 1000$ eps, $200$ valid + $5$ malformed events/run) functional smoke test against a local SQLite-backed Apache Iceberg warehouse (`pyiceberg` + `FsspecFileIO`). All $600$ valid records across the 3 runs were persisted and queried back from Apache Iceberg with $0$ duplicates, $0$ missing records, and $15/15$ malformed events cleanly isolated (`is_smoke_test = true`).

---

## 6. Limitations and Conclusion

1. **Proxy Segments vs. True Customer Segments**: Transaction-amount tertiles (`low`, `medium`, `high`) are strictly transaction-size proxies and must not be conflated with demographic or cardholder-identity segments.
2. **Stateless Point-in-Time Scoring**: The logistic regression model scores each transaction independently using its 30 features; adding stateful Flink keyed windows (e.g., 5-minute card velocity) would require cardholder identifiers not present in the public ULB dataset.
3. **Summary**: This project delivers a verified, cross-platform `Kafka -> Flink -> Apache Iceberg` pipeline, exact mathematical scoring parity ($5.551 \times 10^{-16}$ max diff), thorough held-out and segment-level error analysis on all $56,962$ test records of the ULB dataset, automated unit and Iceberg smoke tests (`12/12` passing), and transparent reporting of host runtime constraints.
