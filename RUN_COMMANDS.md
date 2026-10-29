# Step-by-Step Commands to Run the Project

**Team Members:**
1. **Kushal G** — `PES1UG24AM145`
2. **Kshitij Satish Shetty** — `PES1UG24AM143`

---

## How to Open the Terminal
1. Extract `PES1UG24AM145.zip` into a folder (or use your existing `BigData_Fraud_Detection` folder).
2. Open **PowerShell** or **Command Prompt (CMD)** inside that folder.

---

## PART A: Commands to Run on Any Laptop (No Docker Needed)

Run these **5 commands** in this exact order during your demo:

### Command 1: Run Automated Tests (Checks all code & model accuracy)
```powershell
python -m pytest -v
```
- **What it does:** Checks that the data format is valid, proves the `Class` fraud answer is not leaked into the model inputs, checks that the Flink model gives the exact same probabilities as scikit-learn, and checks that Apache Iceberg saves records without mixing runs.
- **What you will see:** `14 passed` in green.

---

### Command 2: Run the 3-Rate Streaming Test into Apache Iceberg
```powershell
python scripts/smoke_test.py
```
- **What it does:** Sends 600 held-out credit card transactions at **3 increasing speeds** (**200**, **500**, and **1,000** transactions per second) through the Flink fraud scoring function and saves them into a local **Apache Iceberg** database table. It also sends 5 broken/corrupted messages per run to prove our system catches bad data without crashing.
- **What you will see:**
  `[OK] Smoke test PASSED across 3 rates: 600 total valid events persisted to Apache Iceberg, 5 malformed events isolated per run, 0 duplicates, 0 missing.`

---

### Command 3: Show the Fraud Model Accuracy & Segment Table
```powershell
python scripts/run_evaluation.py
```
- **What it does:** Evaluates all **56,962 test transactions** (which contain 98 real frauds) and prints:
  1. Overall Confusion Matrix (`TP=90`, `FP=1386`, `TN=55478`, `FN=8`), Recall (`91.84%`), and False-Positive Rate (`2.44%`).
  2. The **Low ($\le \$9.99$)**, **Medium ($\$9.99–\$50.00$)**, and **High ($> \$50.00$)** segment error table.

---

### Command 4: Verify the PDF Report & Results Notebook
```powershell
python scripts/generate_report.py
```
- **What it does:** Verifies (or rebuilds) the 4-page `report/technical_report.pdf` file.

---

### Command 5: Open the Simple Presentation Frontend (Web UI)
```powershell
python scripts/presentation_ui.py
```
- **What it does:** Starts the simple visual dashboard at **http://localhost:8501** and opens it automatically in your web browser so you can show the teacher live fraud predictions, segment tables, and load-test results.
- *(To stop the web UI later, press `Ctrl + C` in the terminal.)*

---

## PART B: Optional Commands (Only If Asked to Download Data & Retrain From Scratch)

If the teacher asks how the dataset was downloaded or how the model was trained initially:

```powershell
# 1. Download the 150 MB creditcard.csv dataset from Zenodo
python scripts/download_dataset.py

# 2. Train the Logistic Regression model on 80% data and test on 20% data
python scripts/train_model.py --threshold 0.50
```

---

## PART C: Full Docker Cluster Commands (Only If Docker Desktop Is Installed & Running)

If you are running on a laptop where **Docker Desktop** is installed and open:

```powershell
# 1. Start Kafka, Flink, Iceberg REST Catalog, and MinIO containers
docker compose up -d --build

# 2. Check that all containers are running
docker compose ps

# 3. Create the Kafka topic for transactions
docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic transactions.raw.v1 --partitions 2 --replication-factor 1

# 4. Start the Flink real-time fraud scoring job inside the Flink cluster
docker exec -d fraud-flink-jobmanager ./bin/flink run -py /opt/app/src/flink_job.py --bootstrap-servers kafka:9092 --iceberg-rest-uri http://iceberg-rest:8181 --s3-endpoint http://minio:9000

# 5. Run the 3-speed load test (200, 500, 1000 transactions/sec)
python scripts/run_load_tests.py --rates 200,500,1000 --events-per-run 3000 --settle-timeout-sec 60

# 6. Check the saved records inside Apache Iceberg
python scripts/verify_iceberg.py --limit 10

# 7. Stop Docker containers when finished
docker compose down -v --remove-orphans
```
