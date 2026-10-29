"""CLI entrypoint to train the fraud detection model, verify parity, and compute held-out evaluation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config
from src.evaluation import evaluate_heldout_and_segments
from src.model_training import compute_sha256, train_and_export_model
from src.scoring import LinearFraudModelArtifact


def main() -> None:
    cfg = get_config()
    parser = argparse.ArgumentParser(description="Train ULB credit card fraud detection model.")
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=cfg.raw_csv_path,
        help="Path to raw creditcard.csv (default: data/raw/creditcard.csv)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=cfg.fraud_threshold,
        help="Default probability threshold for binary classification (default: 0.50)",
    )
    args = parser.parse_args()

    if not args.input_csv.exists():
        print(
            f"[ERROR] Dataset not found at {args.input_csv}.\n"
            f"Run `python scripts/download_dataset.py` first, or download creditcard.csv from:\n"
            f"  - https://zenodo.org/records/7395559\n"
            f"  - https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud\n"
            f"and place it at {args.input_csv}.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"[INFO] Computing SHA-256 checksum for {args.input_csv} ...")
    sha256_hex = compute_sha256(args.input_csv)
    print(f"[INFO] Loading dataset from {args.input_csv} (sha256={sha256_hex[:16]}...) ...")
    df = pd.read_csv(args.input_csv)
    print(f"[INFO] Loaded {len(df):,} rows and {len(df.columns)} columns.")

    train_out = train_and_export_model(
        df=df,
        config=cfg,
        dataset_source_label="ULB Credit Card Fraud Dataset (Zenodo 7395559 / Kaggle mlg-ulb)",
        dataset_sha256=sha256_hex,
        save_heldout_csv=True,
    )
    meta = train_out["artifact"]["metadata"]
    quantiles = train_out["artifact"]["amount_quantiles"]
    print(
        f"[OK] Trained model saved to:\n"
        f"     - Joblib pipeline : {cfg.model_joblib_path}\n"
        f"     - Flink artifact  : {cfg.model_artifact_path}\n"
        f"     - Held-out split  : {cfg.heldout_csv_path} ({meta['heldout_test_rows']:,} rows)\n"
        f"     - Parity check    : max_abs_diff = {meta['parity_check_max_abs_diff']:.3e}\n"
        f"     - Amount quantiles: q33 = {quantiles['q33']:.2f}, q67 = {quantiles['q67']:.2f}"
    )

    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)
    heldout_df = pd.read_csv(cfg.heldout_csv_path)
    eval_res = evaluate_heldout_and_segments(
        heldout_df=heldout_df,
        artifact=artifact,
        threshold=args.threshold,
        output_dir=cfg.results_dir,
    )
    om = eval_res["overall_metrics"]
    print(
        f"[OK] Held-out evaluation complete (threshold={om['threshold']:.2f}):\n"
        f"     - Total Held-out : {om['total_transactions']:,} (Frauds: {om['actual_positive_fraud']})\n"
        f"     - Confusion Mat  : TP={om['true_positives']}, FP={om['false_positives']}, "
        f"TN={om['true_negatives']}, FN={om['false_negatives']}\n"
        f"     - Precision      : {om['precision']:.4f}\n"
        f"     - Recall         : {om['recall']:.4f}\n"
        f"     - F1-Score       : {om['f1_score']:.4f}\n"
        f"     - FPR / FNR      : {om['false_positive_rate']:.4f} / {om['false_negative_rate']:.4f}\n"
        f"     - PR-AUC / ROC   : {om['pr_auc']:.4f} / {om['roc_auc']:.4f}"
    )


if __name__ == "__main__":
    main()
