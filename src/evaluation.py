"""Overall classification evaluation and transaction-amount segment-level error analysis."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.config import FEATURE_COLUMNS, TARGET_COLUMN
from src.scoring import LinearFraudModelArtifact


def compute_binary_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_prob: np.ndarray | None = None,
) -> dict[str, Any]:
    """Compute confusion matrix, precision, recall, F1, FPR, FNR, and PR-AUC."""
    y_true_arr = np.asarray(y_true, dtype=int)
    y_pred_arr = np.asarray(y_pred, dtype=int)

    tn, fp, fn, tp = confusion_matrix(y_true_arr, y_pred_arr, labels=[0, 1]).ravel()
    total = int(tn + fp + fn + tp)

    precision = float(precision_score(y_true_arr, y_pred_arr, zero_division=0.0))
    recall = float(recall_score(y_true_arr, y_pred_arr, zero_division=0.0))
    f1 = float(f1_score(y_true_arr, y_pred_arr, zero_division=0.0))
    fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0
    fnr = float(fn / (fn + tp)) if (fn + tp) > 0 else 0.0
    specificity = float(tn / (tn + fp)) if (tn + fp) > 0 else 0.0

    pr_auc: float | None = None
    roc_auc: float | None = None
    if y_prob is not None and len(np.unique(y_true_arr)) > 1:
        y_prob_arr = np.asarray(y_prob, dtype=np.float64)
        pr_auc = float(average_precision_score(y_true_arr, y_prob_arr))
        roc_auc = float(roc_auc_score(y_true_arr, y_prob_arr))

    return {
        "total_transactions": total,
        "actual_positive_fraud": int(tp + fn),
        "actual_negative_legitimate": int(tn + fp),
        "predicted_positive_flagged": int(tp + fp),
        "predicted_negative_cleared": int(tn + fn),
        "true_positives": int(tp),
        "false_positives": int(fp),
        "true_negatives": int(tn),
        "false_negatives": int(fn),
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "false_positive_rate": fpr,
        "false_negative_rate": fnr,
        "specificity": specificity,
        "pr_auc": pr_auc,
        "roc_auc": roc_auc,
    }


def evaluate_heldout_and_segments(
    heldout_df: pd.DataFrame,
    artifact: LinearFraudModelArtifact,
    threshold: float | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Evaluate the held-out dataset using the Flink-compatible linear model artifact
    and compute segment-level error analysis across low, medium, and high amount bands.
    """
    active_threshold = float(threshold) if threshold is not None else artifact.default_threshold

    # Score using exact vectorized equivalent of LinearFraudModelArtifact.predict_proba_single
    X_mat = heldout_df[FEATURE_COLUMNS].to_numpy(dtype=np.float64)
    mean_vec = np.asarray(artifact.scaler_mean, dtype=np.float64)
    scale_vec = np.asarray(artifact.scaler_scale, dtype=np.float64)
    coef_vec = np.asarray(artifact.coefficients, dtype=np.float64)

    scaled_mat = (X_mat - mean_vec) / scale_vec
    logits = scaled_mat @ coef_vec + artifact.intercept
    probs = np.where(
        logits >= 0.0,
        1.0 / (1.0 + np.exp(-logits)),
        np.exp(logits) / (1.0 + np.exp(logits)),
    )
    preds = (probs >= active_threshold).astype(int)
    y_true = heldout_df[TARGET_COLUMN].to_numpy(dtype=int)
    amounts = heldout_df["Amount"].to_numpy(dtype=np.float64)

    segments = np.array([artifact.assign_amount_segment(float(a)) for a in amounts])

    overall_metrics = compute_binary_metrics(y_true=y_true, y_pred=preds, y_prob=probs)
    overall_metrics["threshold"] = active_threshold

    # Threshold trade-off curve for analysis and plotting
    precisions, recalls, pr_thresholds = precision_recall_curve(y_true, probs)
    candidate_thresholds = [0.10, 0.25, 0.50, 0.70, 0.85, 0.90, 0.95]
    threshold_sweep: list[dict[str, Any]] = []
    for cand_t in candidate_thresholds:
        cand_preds = (probs >= cand_t).astype(int)
        cand_m = compute_binary_metrics(y_true=y_true, y_pred=cand_preds, y_prob=None)
        threshold_sweep.append(
            {
                "threshold": cand_t,
                "precision": cand_m["precision"],
                "recall": cand_m["recall"],
                "f1_score": cand_m["f1_score"],
                "false_positives": cand_m["false_positives"],
                "false_positive_rate": cand_m["false_positive_rate"],
                "false_negatives": cand_m["false_negatives"],
                "false_negative_rate": cand_m["false_negative_rate"],
            }
        )

    segment_definitions = {
        "low": f"Amount <= {artifact.amount_q33:.2f}",
        "medium": f"{artifact.amount_q33:.2f} < Amount <= {artifact.amount_q67:.2f}",
        "high": f"Amount > {artifact.amount_q67:.2f}",
    }

    segment_rows: list[dict[str, Any]] = []
    for seg_name in ["low", "medium", "high"]:
        mask = segments == seg_name
        seg_y_true = y_true[mask]
        seg_y_pred = preds[mask]
        seg_probs = probs[mask]
        seg_amounts = amounts[mask]

        seg_metrics = compute_binary_metrics(
            y_true=seg_y_true, y_pred=seg_y_pred, y_prob=seg_probs
        )
        segment_rows.append(
            {
                "segment": seg_name,
                "amount_band_rule": segment_definitions[seg_name],
                "min_amount": float(np.min(seg_amounts)) if len(seg_amounts) > 0 else 0.0,
                "median_amount": float(np.median(seg_amounts)) if len(seg_amounts) > 0 else 0.0,
                "max_amount": float(np.max(seg_amounts)) if len(seg_amounts) > 0 else 0.0,
                "num_transactions": seg_metrics["total_transactions"],
                "actual_frauds": seg_metrics["actual_positive_fraud"],
                "actual_legitimate": seg_metrics["actual_negative_legitimate"],
                "true_positives": seg_metrics["true_positives"],
                "false_positives": seg_metrics["false_positives"],
                "true_negatives": seg_metrics["true_negatives"],
                "false_negatives": seg_metrics["false_negatives"],
                "false_positive_rate": seg_metrics["false_positive_rate"],
                "false_negative_rate": seg_metrics["false_negative_rate"],
                "precision": seg_metrics["precision"],
                "recall": seg_metrics["recall"],
                "f1_score": seg_metrics["f1_score"],
                "pr_auc": seg_metrics["pr_auc"],
            }
        )

    results_payload = {
        "dataset_note": (
            "The public ULB dataset does not contain customer identities or demographic segments. "
            "Transaction-amount bands (low, medium, high) defined from training-split tertiles "
            "(33.33% and 66.67% quantiles) are used strictly as transaction-amount proxies."
        ),
        "threshold": active_threshold,
        "amount_quantiles": {
            "q33": artifact.amount_q33,
            "q67": artifact.amount_q67,
        },
        "overall_metrics": overall_metrics,
        "segment_analysis": segment_rows,
        "threshold_sweep": threshold_sweep,
        "model_metadata": artifact.metadata,
    }

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        with (output_dir / "evaluation_metrics.json").open("w", encoding="utf-8") as f:
            json.dump(results_payload, f, indent=2)
        seg_df = pd.DataFrame(segment_rows)
        seg_df.to_csv(output_dir / "segment_error_analysis.csv", index=False)
        with (output_dir / "segment_error_analysis.json").open("w", encoding="utf-8") as f:
            json.dump(segment_rows, f, indent=2)

    return results_payload
