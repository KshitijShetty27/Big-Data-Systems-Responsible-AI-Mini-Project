# Complete Presentation & Explanation Script (Start to Finish)

**Team Members:**
1. **Kushal G** — `PES1UG24AM145`
2. **Kshitij Satish Shetty** — `PES1UG24AM143`

---

## PART 1: Understanding the Project in Simple Words (Read This First!)

Before presenting, here is what our project does in plain, simple English:

1. **What is the problem?**
   When someone swipes a credit card, the payment company has less than a second to decide: *"Is this a real customer or a thief?"* If we check transactions in slow daily batches, the thief has already escaped with the money. So we need a **real-time streaming pipeline**.
2. **What are the 5 building blocks of our pipeline?**
   - **Public Credit Card Dataset (`creditcard.csv`)**: A real dataset of **284,807 credit card transactions** where **492 are frauds** (only **0.17%** are fraud—meaning out of every 580 swipes, only 1 is fraud).
   - **Apache Kafka**: Think of Kafka as a **high-speed conveyor belt (message pipe)**. Our producer script takes test transactions and puts them onto the Kafka belt at different speeds (**200, 500, and 1,000 transactions per second**).
   - **Apache Flink**: Think of Flink as the **live brain** watching the Kafka conveyor belt. As soon as a transaction arrives, Flink checks that the data isn't broken, runs our Fraud Machine Learning formula, and calculates the fraud probability (from 0% to 100%).
   - **Fraud Model (Logistic Regression)**: We trained a lightweight Machine Learning model on 80% of the data (**227,845 swipes**) and saved 20% (**56,962 swipes**) for testing. Because frauds are so rare, we used **balanced class weights** so the model pays extra attention to fraud cases.
   - **Apache Iceberg (with MinIO S3)**: Think of Iceberg as a **modern Big Data warehouse table**. After Flink scores each transaction, it saves the result (along with how many milliseconds it took) into Apache Iceberg so analysts can query it anytime.

---

## PART 2: Step-by-Step Script to Speak to the Teacher (3 Minutes)

You can split the speaking evenly between **Kushal** (Minutes 0:00–1:30) and **Kshitij** (Minutes 1:30–3:00).

---

### STEP 1: Introduction & Architecture (0:00 – 0:45) — *Kushal Speaks*

**What to do on laptop:**
Keep the Simple Web UI (`http://localhost:8501`) or `README.md` open on screen.

**What to say:**
> *"Good morning Sir/Ma'am. We are Kushal (PES1UG24AM145) and Kshitij (PES1UG24AM143). Our project is a **Real-Time Credit Card Fraud Detection Pipeline**.*
>
> *In real-world payment systems, fraud must be caught the moment a card is swiped. So we built a 4-stage Big Data pipeline:*
> 1. *First, we take held-out transactions from the **public ULB Credit Card Fraud dataset** (which has 284,807 total transactions and 492 frauds).*
> 2. *Second, our producer replays these transactions as a live stream into **Apache Kafka**.*
> 3. *Third, **Apache Flink** reads the stream in real time, validates the data, and scores each transaction using our trained **Logistic Regression fraud model**.*
> 4. *Fourth, Flink writes the scored results and latency timestamps into an **Apache Iceberg** table backed by MinIO storage."*

---

### STEP 2: Model Training, No Cheating (Leakage), and Code Verification (0:45 – 1:30) — *Kushal Speaks*

**What to do on laptop:**
Switch to the terminal and run:
```powershell
python -m pytest -v
```

**What to say:**
> *"For our fraud model, we split the dataset 80% for training (227,845 rows) and 20% for testing (56,962 rows) using a fixed random seed so it is 100% reproducible.*
>
> *Two important rules were followed:*
> - *First, we **strictly excluded the `Class` target label** from the model inputs so the model can never cheat. If someone tries to send `Class` inside the input features, our validation code rejects it.*
> - *Second, we exported the trained scaling numbers and model weights into a JSON artifact (`models/fraud_model_artifact.json`) so Flink can calculate the exact same fraud probability natively. As you can see from our 14 automated tests passing here, the difference between scikit-learn and our Flink scoring math is practically zero ($5.55 \times 10^{-16}$)."*

---

### STEP 3: Live Streaming Speed Test & Apache Iceberg Storage (1:30 – 2:15) — *Kshitij Speaks*

**What to do on laptop:**
In the terminal, run:
```powershell
python scripts/smoke_test.py
```
*(Or click the **"▶ Run 3-Rate Test Now"** button inside Section 3 of the Web UI at `http://localhost:8501`.)*

**What to say:**
> *"Next, we tested how our pipeline handles increasing traffic speeds. We replayed held-out transactions at three increasing rates: **200, 500, and 1,000 events per second**.*
>
> *Every run has its own `run_id` so transactions from different tests never mix. For every transaction, we record the exact time the producer sent it and the exact time Flink scored it, giving us the real latency in milliseconds ($p_{50}, p_{95}, p_{99}$).*
>
> *We also injected 5 corrupted/malformed events in each run. Instead of crashing the pipeline, our Flink logic catches those 5 bad events and sends them to a separate `malformed_events` table, while saving 100% of the valid transactions into Apache Iceberg with 0 lost and 0 duplicate records."*

---

### STEP 4: Fraud Accuracy, Amount Segments & Who Pays the Cost (2:15 – 3:00) — *Kshitij Speaks*

**What to do on laptop:**
Show the **Simple Web UI (`http://localhost:8501`)**—test one Fraud transaction and one Normal transaction in Section 1, and point to the **Segment Table in Section 2** (or run `python scripts/run_evaluation.py` in the terminal).

**What to say:**
> *"Finally, we analyzed our model's accuracy across all **56,962 held-out transactions** (which contain 98 actual frauds).*
> - *Overall, at our default 0.50 threshold, the model catches **90 out of 98 frauds (91.84% Recall)** and misses only 8 frauds, with a **False-Positive Rate of just 2.44%** (1,386 false alarms out of 56,864 normal swipes) and a **PR-AUC of 0.719**.*
>
> *Because the public dataset is anonymized for privacy and doesn't have customer names or ages, we grouped transactions into **3 proxy segments based on Transaction Amount** using training-data quantiles:*
> 1. ***Low Amount ($\le \$9.99$)**: Has the highest False-Positive Rate (**3.40%**) and highest missed-fraud rate (**10.20%**). Half of all frauds (49 out of 98) happen here because hackers test stolen cards with tiny \$1 swipes first. Here, **payment companies** bear the cost because reviewing a \$2 false alarm costs more than the transaction fee, and **customers** get annoyed when small everyday purchases are blocked.*
> 2. ***Medium Amount ($\$9.99\text{ to }\$50.00$)**: Has the lowest False-Positive Rate (**1.42%**) and highest **PR-AUC (0.84)**.*
> 3. ***High Amount ($> \$50.00$)**: Has the best fraud detection—catching **94.74% of frauds** (only **5.26% False-Negative Rate**, missing just 2 out of 38 frauds). In high-amount purchases, **merchants and banks** bear huge financial losses if a fraud is missed (False Negative), while **merchants** also lose big sales if a real customer is falsely blocked (False Positive).*
>
> *If a bank wants fewer false alarms, our threshold is configurable—raising it from 0.50 to 0.85 cuts false alarms by **72%** while still catching **89%** of all frauds. Thank you!"*

---

## PART 3: Common Teacher Viva Questions & Simple Answers

1. **Q: Why did you use PR-AUC (`0.7190`) instead of regular Accuracy?**
   - **Answer:** Because 99.83% of the dataset is normal transactions! Even a dummy model that says "everything is normal" would get 99.83% accuracy while catching 0 frauds. **PR-AUC (Precision-Recall AUC)** focuses specifically on how well we catch the rare 0.17% fraud class without raising too many false alarms.

2. **Q: Why is Recall so high (`91.84%`) while Precision is `6.10%` at threshold `0.50`?**
   - **Answer:** Because we used `class_weight='balanced'` in Logistic Regression. Since there are 578 normal transactions for every 1 fraud, balanced weights tell the model that missing 1 fraud is much worse than flagging a normal transaction. Even a 2.44% false-positive rate on 56,864 normal swipes equals 1,386 false alarms compared to 90 real frauds, which makes Precision 6.10%. Raising the threshold to `0.85` or `0.95` increases Precision to `39%` and drops false alarms to `0.24%`.

3. **Q: How does Apache Flink talk to the scikit-learn model?**
   - **Answer:** Instead of loading a heavy Python pickle file that can break across different OSes or Java versions, `scripts/train_model.py` exports the exact feature means, standard deviations, weights, and intercept into `models/fraud_model_artifact.json`. Flink loads this JSON and computes the exact same scaling and sigmoid formula in microseconds.

4. **Q: Why are the segments based on Transaction Amount (`Low`, `Medium`, `High`)?**
   - **Answer:** The public ULB dataset uses PCA (`V1` to `V28`) to hide personal customer details for privacy, so there are no customer IDs or demographic columns. Therefore, we used the 33% ($9.99) and 67% ($50.00) quantiles of `Amount` from the training data as reproducible proxy segments, and clearly stated in our report that they are transaction-amount proxies, not customer demographics.

5. **Q: How do you prevent mixing data between the 3 load-test runs (`200`, `500`, `1000` eps)?**
   - **Answer:** Every event sent by the producer includes a unique `run_id`, and our Apache Iceberg table (`fraud_detection.scored_transactions`) is **partitioned by `run_id`**. When we measure throughput and latency, we query Iceberg filtering strictly by that `run_id`.
