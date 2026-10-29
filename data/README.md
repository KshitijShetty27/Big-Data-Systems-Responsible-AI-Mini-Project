# Dataset Directory (`data/`)

To keep the submission archive (`PES1UG24AM145.zip`) compact, the 150.8 MB raw ULB Credit Card Fraud dataset (`data/raw/creditcard.csv`) and the derived 20% held-out CSV split (`data/processed/heldout_transactions.csv`) are excluded from the ZIP archive.

## How to Obtain the Excluded Dataset

### Option 1: Automated Download (Recommended)
From the project root directory, run:

```powershell
# Windows PowerShell
python scripts/download_dataset.py

# macOS / Linux
python3 scripts/download_dataset.py
```

This downloads `creditcard.csv` (`150,828,752` bytes) directly from the official Zenodo repository (`https://zenodo.org/records/7395559`), verifies its MD5 checksum (`e90efcb83d69faf99fcab8b0255024de`), and saves it to `data/raw/creditcard.csv`.

### Option 2: Manual Download
Download `creditcard.csv` from either official repository:
1. **Zenodo**: https://zenodo.org/records/7395559
2. **Kaggle**: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

Place the file at:
```text
data/raw/creditcard.csv
```

## Regenerating the Stratified Held-Out Split
Once `data/raw/creditcard.csv` is present, run:
```bash
python scripts/train_model.py
```
This reproduces the exact stratified 80/20 split (`random_seed=42`) and writes the 56,962 held-out transactions to `data/processed/heldout_transactions.csv`.
