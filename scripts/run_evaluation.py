"""Run overall and segment-level classification evaluation on held-out or pre-computed results."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config
from src.evaluation import evaluate_heldout_and_segments
from src.scoring import LinearFraudModelArtifact


def main() -> None:
    cfg = get_config()
    parser = argparse.ArgumentParser(description="Evaluate held-out predictions and segment errors.")
    parser.add_argument("--heldout-csv", type=Path, default=cfg.heldout_csv_path)
    parser.add_argument("--model-artifact", type=Path, default=cfg.model_artifact_path)
    parser.add_argument("--threshold", type=float, default=cfg.fraud_threshold)
    parser.add_argument("--output-dir", type=Path, default=cfg.results_dir)
    args = parser.parse_args()

    if not args.model_artifact.exists():
        print(f"[ERROR] Model artifact not found at {args.model_artifact}", file=sys.stderr)
        sys.exit(1)

    display_cols = [
        "segment",
        "amount_band_rule",
        "num_transactions",
        "false_positives",
        "true_negatives",
        "false_positive_rate",
        "false_negatives",
        "false_negative_rate",
    ]

    if args.heldout_csv.exists():
        artifact = LinearFraudModelArtifact.from_json_file(args.model_artifact)
        heldout_df = pd.read_csv(args.heldout_csv)
        res = evaluate_heldout_and_segments(
            heldout_df=heldout_df,
            artifact=artifact,
            threshold=args.threshold,
            output_dir=args.output_dir,
        )
        print(f"[OK] Computed and saved evaluation metrics to {args.output_dir}")
        om = res["overall_metrics"]
        seg_df = pd.DataFrame(res["segment_analysis"])
    elif (args.output_dir / "evaluation_metrics.json").exists():
        with (args.output_dir / "evaluation_metrics.json").open("r", encoding="utf-8") as f:
            res = json.load(f)
        om = res["overall_metrics"]
        seg_df = pd.DataFrame(res["segment_analysis"])
        print(f"[OK] Loaded held-out evaluation results from {args.output_dir / 'evaluation_metrics.json'}")
    else:
        print(
            f"[ERROR] Held-out CSV not found at {args.heldout_csv}. "
            f"Run `python scripts/download_dataset.py` and `python scripts/train_model.py` first.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"\n=== Overall Held-Out Metrics (N={om['total_transactions']:,}, Threshold={om['threshold']:.2f}) ===\n"
        f"  Confusion Matrix : TP={om['true_positives']}, FP={om['false_positives']}, "
        f"TN={om['true_negatives']}, FN={om['false_negatives']}\n"
        f"  Precision        : {om['precision']:.4f} ({om['precision']*100:.2f}%)\n"
        f"  Recall           : {om['recall']:.4f} ({om['recall']*100:.2f}%)\n"
        f"  F1-Score         : {om['f1_score']:.4f}\n"
        f"  FPR / FNR        : {om['false_positive_rate']:.4f} ({om['false_positive_rate']*100:.2f}%) / "
        f"{om['false_negative_rate']:.4f} ({om['false_negative_rate']*100:.2f}%)\n"
        f"  PR-AUC / ROC-AUC : {om['pr_auc']:.4f} / {om['roc_auc']:.4f}\n"
    )
    print("=== Segment-Level Error Analysis (Transaction-Amount Proxies) ===")
    print(seg_df[display_cols].to_string(index=False))


if __name__ == "__main__":
    main()
