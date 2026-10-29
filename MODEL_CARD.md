# Model & Data Card — Real-Time Credit Card Fraud Detection

**Team Members:**
- **Kushal G** (`PES1UG24AM145`)
- **Kshitij Satish Shetty** (`PES1UG24AM143`)

## 1. Dataset Source & Provenance

- **Dataset Name**: ULB Machine Learning Group Credit Card Fraud Detection Dataset (`creditcard.csv`).
- **Primary Sources**:
  - Zenodo Record: https://zenodo.org/records/7395559 (`https://zenodo.org/api/records/7395559/files/creditcard.csv/content`)
  - Kaggle Mirror: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
- **File Integrity**:
  - Size: `150,828,752` bytes
  - MD5 Checksum: `e90efcb83d69faf99fcab8b0255024de`
  - SHA-256 Digest: `76274b691b16a6c49d3f159c883398e03ccd6d1ee12d9d8ee38f4b4b98551a89`
- **Dataset Characteristics**:
  - Contains `284,807` credit card transactions made by European cardholders over a two-day period in September 2013.
  - Extreme class imbalance: `492` fraudulent transactions (`0.1727%` positive class) and `284,315` legitimate transactions (`99.8273%` negative class).
  - Features (`30` numeric input columns): `Time` (seconds elapsed since first transaction), `V1` through `V28` (anonymized principal components obtained via PCA), and `Amount` (transaction value).
  - Target column: `Class` (`1` = actual fraud, `0` = legitimate). The `Class` column is **strictly excluded** from all model training inputs, feature scaling, and real-time inference payloads (`parse_and_validate_event` explicitly rejects any event containing `"Class"` inside `features`).

---

## 2. Intended Use

- **Primary Purpose**: Real-time streaming fraud scoring inside an Apache Flink job (`Kafka -> Flink -> Apache Iceberg`) for educational and benchmarking analysis of streaming throughput, end-to-end latency, and segment-level classification errors.
- **Out-of-Scope Uses**: Production deployment for automated card blocking without human-in-the-loop review, demographic fairness auditing (since the dataset contains no demographic or customer identity attributes), or extrapolation to modern payment rails without retraining on recent, non-PCA-anonymized features.

---

## 3. Model Architecture & Preprocessing

- **Model Pipeline**: `scikit-learn` `Pipeline` consisting of:
  1. `StandardScaler()` fitted exclusively on the 30 input features (`Time`, `V1`–`V28`, `Amount`) of the training split.
  2. `LogisticRegression(class_weight="balanced", C=1.0, solver="lbfgs", max_iter=1000, random_state=42)`.
- **Train / Held-Out Split**:
  - Stratified 80% / 20% split (`test_size=0.20`, `random_state=42`, `stratify=y`).
  - **Training Set**: `227,845` transactions (`394` frauds, `0.1729%` fraud rate).
  - **Held-Out Evaluation Set**: `56,962` transactions (`98` frauds, `0.1720%` fraud rate), saved to `data/processed/heldout_transactions.csv` for streaming replay and evaluation.
- **Transaction-Amount Proxy Segment Boundaries**:
  - Computed exclusively on the training split `Amount` distribution at the 33.33% ($q_{33}$) and 66.67% ($q_{67}$) quantiles:
    - **Low Band**: $\text{Amount} \le 9.99$
    - **Medium Band**: $9.99 < \text{Amount} \le 50.00$
    - **High Band**: $\text{Amount} > 50.00$
- **Flink Model Artifact & Mathematical Parity**:
  - Saved as both `models/fraud_model.joblib` and portable JSON `models/fraud_model_artifact.json` containing `feature_names`, `scaler_mean`, `scaler_scale`, `coefficients`, `intercept`, `default_threshold`, and `amount_quantiles`.
  - Validated on a 250-transaction held-out test fixture (`tests/test_scoring.py`): maximum absolute probability difference between `scikit-learn` `predict_proba` and `LinearFraudModelArtifact.predict_proba_single` is **$5.551 \times 10^{-16}$** (IEEE-754 double-precision machine epsilon).

---

## 4. Decision Threshold & Calibration Trade-Offs

- **Default Threshold**: `0.50` (configurable via `FRAUD_THRESHOLD` in `.env` or `--threshold` CLI flag).
- Because `class_weight="balanced"` upweights the minority fraud class by roughly $\frac{284,807}{2 \times 492} \approx 289\times$, a threshold of `0.50` prioritizes high recall (`91.84%`) at the cost of a `2.44%` false-positive rate (`6.10%` precision).
- **Measured Threshold Sweep on Held-Out Set ($N = 56,962$)**:

| Decision Threshold | Precision | Recall ($1 - \text{FNR}$) | F1-Score | False Positives (FP) | False-Positive Rate (FPR) | False Negatives (FN) | False-Negative Rate (FNR) |
|-------------------:|----------:|--------------------------:|---------:|---------------------:|--------------------------:|---------------------:|--------------------------:|
| `0.10` | 0.0082 | 0.9490 | 0.0162 | 11,315 | 0.1990 (19.90%) | 5 | 0.0510 (5.10%) |
| `0.25` | 0.0213 | 0.9184 | 0.0416 | 4,144 | 0.0729 (7.29%) | 8 | 0.0816 (8.16%) |
| **`0.50` (Default)** | **0.0610** | **0.9184** | **0.1144** | **1,386** | **0.0244 (2.44%)** | **8** | **0.0816 (8.16%)** |
| `0.70` | 0.1213 | 0.9082 | 0.2139 | 645 | 0.0113 (1.13%) | 9 | 0.0918 (9.18%) |
| `0.85` | 0.1847 | 0.8878 | 0.3058 | 384 | 0.0068 (0.68%) | 11 | 0.1122 (11.22%) |
| `0.90` | 0.2472 | 0.8878 | 0.3867 | 265 | 0.0047 (0.47%) | 11 | 0.1122 (11.22%) |
| `0.95` | 0.3901 | 0.8878 | 0.5421 | 136 | 0.0024 (0.24%) | 11 | 0.1122 (11.22%) |

---

## 5. Actual Evaluation Metrics on Held-Out Set ($N = 56,962$)

### 5.1 Overall Held-Out Performance (`threshold = 0.50`)

- **Total Held-Out Transactions**: `56,962` (`98` actual frauds, `56,864` actual legitimate)
- **Confusion Matrix**:
  - True Positives ($\text{TP}$): `90`
  - False Positives ($\text{FP}$): `1,386`
  - True Negatives ($\text{TN}$): `55,478`
  - False Negatives ($\text{FN}$): `8`
- **Precision**: `0.0610` (`6.10%`)
- **Recall (Sensitivity)**: `0.9184` (`91.84%`)
- **F1-Score**: `0.1144`
- **False-Positive Rate ($\text{FPR} = \frac{\text{FP}}{\text{FP} + \text{TN}}$)**: `0.0244` (`2.44%`)
- **False-Negative Rate ($\text{FNR} = \frac{\text{FN}}{\text{FN} + \text{TP}}$)**: `0.0816` (`8.16%`)
- **Specificity ($\text{TNR}$)**: `0.9756` (`97.56%`)
- **PR-AUC (Average Precision)**: `0.7190`
- **ROC-AUC**: `0.9721`

### 5.2 Segment-Level Error Analysis (Transaction-Amount Proxies)

> **Explicit Disclaimer**: These segments (`low`, `medium`, `high`) are **transaction-amount proxies** derived from training-split quantiles, not verified customer-level or demographic segments.

| Segment | Amount Band Rule | Transactions ($N$) | Actual Frauds | True Positives (TP) | False Positives (FP) | True Negatives (TN) | False Negatives (FN) | FPR | FNR | Precision | Recall | F1-Score | PR-AUC |
|:--------|:-----------------|-------------------:|--------------:|--------------------:|---------------------:|--------------------:|---------------------:|-------:|-------:|----------:|-------:|---------:|-------:|
| **Low** | $\text{Amount} \le 9.99$ | 19,582 | 49 | 44 | 664 | 18,869 | 5 | 0.0340 (3.40%) | 0.1020 (10.20%) | 0.0621 | 0.8980 | 0.1162 | 0.8021 |
| **Medium** | $9.99 < \text{Amount} \le 50.00$ | 18,686 | 11 | 10 | 266 | 18,409 | 1 | 0.0142 (1.42%) | 0.0909 (9.09%) | 0.0362 | 0.9091 | 0.0697 | 0.8423 |
| **High** | $\text{Amount} > 50.00$ | 18,694 | 38 | 36 | 456 | 18,200 | 2 | 0.0244 (2.44%) | 0.0526 (5.26%) | 0.0732 | 0.9474 | 0.1358 | 0.6286 |

---

## 6. Limitations, Imbalance Considerations & Responsible-Use Cautions

1. **Severe Class Imbalance ($1:578$ Fraud-to-Legitimate Ratio)**:
   - Standard accuracy and ROC-AUC (`0.9721`) can look deceptively optimistic under extreme class imbalance because $99.83\%$ of records belong to the negative class. Evaluation must rely on **PR-AUC (`0.7190`)**, segment-specific **FPR/FNR**, and confusion-matrix volumes.
2. **Asymmetric Error Costs Across Transaction Segments**:
   - **False Positives (`1,386` overall; `664` in Low, `266` in Medium, `456` in High)**: Legitimate cardholders experience declined or delayed transactions (step-up authentication friction), merchants risk lost sales/cart abandonment (particularly painful in the High segment), and payment processors incur manual-review/alert-triage operational costs (which can exceed the transaction value in the Low $\le 9.99$ segment).
   - **False Negatives (`8` overall; `5` in Low, `1` in Medium, `2` in High)**: Undetected fraud results in direct principal loss, chargeback fees, and network penalties borne by merchants or issuing banks. While the High segment achieves the lowest FNR (`5.26%`, only `2` missed frauds), each missed high-value fraud carries substantially larger financial exposure than a missed micro-transaction, whereas missed Low-segment frauds (`5` missed) often represent automated card-testing probes preceding larger fraudulent charges.
3. **Lack of Customer-Level Context & Temporal Drift**:
   - The dataset spans only 48 hours from 2013 and omits cardholder IDs, device fingerprints, merchant category codes (MCC), and velocity aggregates. Real-world fraud systems should combine point-in-time linear scoring with stateful customer velocity features and periodic recalibration.
