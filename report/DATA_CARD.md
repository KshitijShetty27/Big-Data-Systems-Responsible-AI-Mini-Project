# Data Card — ULB Credit Card Fraud Detection Dataset

**Date:** 30 October 2026  
**Project:** Real-Time Credit Card Fraud Detection Pipeline  
**Team Members:**
- **Kushal G** (`PES1UG24AM145`)
- **Kshitij Satish Shetty** (`PES1UG24AM143`)

---

## 1. Dataset Overview and Provenance

- **Dataset Name**: Machine Learning Group (MLG) ULB Credit Card Fraud Detection Dataset.
- **Publishing Institution**: Machine Learning Group, Université Libre de Bruxelles (ULB), Brussels, Belgium.
- **Primary Repository**: Zenodo Record `7395559` (DOI: `10.5281/zenodo.7395559`).
  - URL: https://zenodo.org/records/7395559
- **Secondary Mirror**: Kaggle (`mlg-ulb/creditcardfraud`).
  - URL: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
- **License**: Open Data Commons Public Domain Dedication and License (PDDL) v1.0.

### File Verification & Cryptographic Integrity
| Property | Value |
|---|---|
| Canonical Path | `data/raw/creditcard.csv` |
| File Size | `150,828,752` bytes (~143.84 MB) |
| Row Count | `284,807` records (plus 1 header row) |
| Column Count | `31` columns (`Time`, `V1`–`V28`, `Amount`, `Class`) |
| MD5 Checksum | `e90efcb83d69faf99fcab8b0255024de` |
| SHA-256 Digest | `76274b691b16a6c49d3f159c883398e03ccd6d1ee12d9d8ee38f4b4b98551a89` |

---

## 2. Dataset Composition and Class Distribution

The dataset records cardholder transactions executed over two days in September 2013 by European cardholders.

| Class Label | Meaning | Record Count | Percentage |
|---|---|---:|---:|
| `0` | Legitimate Transaction | `284,315` | **99.8273%** |
| `1` | Fraudulent Transaction | `492` | **0.1727%** |
| **Total** | All Recorded Transactions | **284,807** | **100.0000%** |

### Severe Imbalance Context
Fraudulent transactions represent approximately 1 in every 578 events. Consequently:
- Conventional overall accuracy is misleading (a naive zero-classifier achieves $99.83\%$ accuracy while catching 0% of fraud).
- Evaluation must focus on **Precision-Recall AUC (PR-AUC)**, **Recall (Sensitivity)**, **False-Positive Rate (FPR)**, and absolute confusion matrix counts.

---

## 3. Input Features and Schema Specification

The dataset contains 30 input features and 1 binary label:

1. **`Time`** (`float64`): Elapsed time in seconds between the current transaction and the first recorded transaction in the dataset.
2. **`V1` through `V28`** (`float64`): Numerical features obtained through Principal Component Analysis (PCA) to preserve cardholder privacy, card numbers, personal data, and merchant identities.
3. **`Amount`** (`float64`): Transaction amount in currency units (Euros), ranging from `0.00` to `25,691.16` (mean: `88.35`, median: `22.00`).
4. **`Class`** (`int64`): Ground-truth fraud label (`1` for fraud, `0` for legitimate).

### Target Leakage Prevention
To guarantee scientific validity and prevent data leakage:
- The target label `Class` is strictly separated from the feature matrix before model training and preprocessing.
- In streaming Kafka payloads, `features` contains exclusively the 30 input columns. `schema.parse_and_validate_event()` explicitly rejects any event containing `Class` inside `features` as a schema violation (`target_leakage_class_in_features`).
- Ground-truth fraud status is carried only as an external benchmark field (`actual_label`) for post-hoc Iceberg verification.

---

## 4. Train / Held-Out Partitioning

Using a fixed random seed (`random_state=42`), the dataset is partitioned via an 80/20 stratified split:

| Partition | Record Count | Legitimate (`Class = 0`) | Fraud (`Class = 1`) | Fraud Prevalence |
|---|---:|---:|---:|---:|
| **Training Split** (80%) | `227,845` | `227,451` | `394` | **0.1729%** |
| **Held-Out Test Split** (20%) | `56,962` | `56,864` | `98` | **0.1720%** |
| **Total Dataset** | `284,807` | `284,315` | `492` | **0.1727%** |

The held-out evaluation transactions are stored at `data/processed/heldout_transactions.csv` and replayed by the Kafka streaming producer.

---

## 5. Transaction-Amount Proxy Segments

> **Explicit Disclaimer**: The ULB dataset contains no customer identifiers, age, gender, geographic region, device fingerprints, or merchant category codes (MCC). Accordingly, transaction segments are defined strictly using **transaction-amount proxy tertiles** derived from the training set:

- **Low Band** ($\text{Amount} \le 9.99$): $q_{33} = 9.99$ (33.33% quantile of training amounts).
- **Medium Band** ($9.99 < \text{Amount} \le 50.00$): Between $q_{33}$ and $q_{67} = 50.00$.
- **High Band** ($\text{Amount} > 50.00$): Above $q_{67} = 50.00$.

### Segment Distribution on Held-Out Set ($N = 56,962$)
| Proxy Segment | Amount Boundary Rule | Held-Out Count | Actual Frauds | Fraud Rate |
|---|---|---:|---:|---:|
| **Low** | $\text{Amount} \le 9.99$ | `19,582` | `49` | **0.250%** (50% of all frauds) |
| **Medium** | $9.99 < \text{Amount} \le 50.00$ | `18,686` | `11` | **0.059%** |
| **High** | $\text{Amount} > 50.00$ | `18,694` | `38` | **0.203%** |

---

## 6. Responsible Use and Limitations

1. **Academic and Benchmarking Scope**: The dataset represents a historical snapshot (September 2013) and does not reflect current e-commerce tokenization, 3D-Secure 2.0 flows, or modern fraud vectors.
2. **Stateless Limitations**: Due to the absence of cardholder or account identifiers, transactions must be evaluated independently; card-level velocity aggregation (e.g. 5-minute swipe velocity) cannot be computed directly from the dataset.
3. **No Demographic Extrapolations**: Because features are PCA components and amounts, this dataset cannot be used to assess demographic bias or algorithmic fairness across protected classes.
