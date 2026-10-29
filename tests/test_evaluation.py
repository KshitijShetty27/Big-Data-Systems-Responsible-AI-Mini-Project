"""Unit tests for binary classification metrics and segment-level error analysis."""

from __future__ import annotations

import json
import numpy as np

from src.config import get_config
from src.evaluation import compute_binary_metrics


def test_compute_binary_metrics_exact_values() -> None:
    # TP=2, FP=1, TN=5, FN=2
    y_true = np.array([1, 1, 1, 1, 0, 0, 0, 0, 0, 0])
    y_pred = np.array([1, 1, 0, 0, 1, 0, 0, 0, 0, 0])
    y_prob = np.array([0.95, 0.85, 0.40, 0.20, 0.70, 0.30, 0.10, 0.05, 0.02, 0.01])

    m = compute_binary_metrics(y_true, y_pred, y_prob)
    assert m["true_positives"] == 2
    assert m["false_positives"] == 1
    assert m["true_negatives"] == 5
    assert m["false_negatives"] == 2
    assert abs(m["precision"] - (2.0 / 3.0)) < 1e-9
    assert abs(m["recall"] - 0.5) < 1e-9
    assert abs(m["false_positive_rate"] - (1.0 / 6.0)) < 1e-9
    assert abs(m["false_negative_rate"] - 0.5) < 1e-9
    assert m["pr_auc"] is not None and 0.0 <= m["pr_auc"] <= 1.0


def test_saved_evaluation_and_segment_consistency() -> None:
    cfg = get_config()
    eval_path = cfg.results_dir / "evaluation_metrics.json"
    assert eval_path.exists(), "evaluation_metrics.json must be generated before running test"

    with eval_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    overall = data["overall_metrics"]
    segments = data["segment_analysis"]
    assert [s["segment"] for s in segments] == ["low", "medium", "high"]

    assert sum(s["num_transactions"] for s in segments) == overall["total_transactions"]
    assert sum(s["false_positives"] for s in segments) == overall["false_positives"]
    assert sum(s["true_negatives"] for s in segments) == overall["true_negatives"]
    assert sum(s["false_negatives"] for s in segments) == overall["false_negatives"]
    assert sum(s["true_positives"] for s in segments) == overall["true_positives"]
