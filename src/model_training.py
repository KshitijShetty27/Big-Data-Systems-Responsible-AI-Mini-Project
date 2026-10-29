"""Model training, preprocessing, artifact export, and mathematical equivalence verification."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.config import FEATURE_COLUMNS, TARGET_COLUMN, PipelineConfig, get_config
from src.scoring import LinearFraudModelArtifact


def compute_sha256(path: Path) -> str:
    """Compute SHA-256 digest of a file."""
    sha = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def train_and_export_model(
    df: pd.DataFrame,
    config: PipelineConfig | None = None,
    dataset_source_label: str = "ULB Credit Card Fraud Dataset (creditcard.csv)",
    dataset_sha256: str = "",
    save_heldout_csv: bool = True,
) -> dict[str, Any]:
    """
    Train a class-weighted LogisticRegression model with StandardScaler on stratified split,
    export both joblib and portable Flink JSON artifact, and verify exact mathematical parity.
    """
    cfg = config or get_config()

    missing_cols = [c for c in [*FEATURE_COLUMNS, TARGET_COLUMN] if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Dataset is missing required columns: {missing_cols}")

    # Strictly exclude target label from model inputs
    X = df[FEATURE_COLUMNS].astype(np.float64).copy()
    y = df[TARGET_COLUMN].astype(int).copy()
    if TARGET_COLUMN in X.columns:
        raise AssertionError("Target leakage detected: Class present in feature matrix X")

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=cfg.test_size,
        random_state=cfg.random_seed,
        stratify=y,
    )

    # Compute reproducible transaction-amount segment quantiles on training data only
    q33 = float(np.quantile(X_train["Amount"].to_numpy(), 1.0 / 3.0))
    q67 = float(np.quantile(X_train["Amount"].to_numpy(), 2.0 / 3.0))

    pipeline = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    C=1.0,
                    solver="lbfgs",
                    max_iter=1000,
                    random_state=cfg.random_seed,
                ),
            ),
        ]
    )
    pipeline.fit(X_train, y_train)

    scaler: StandardScaler = pipeline.named_steps["scaler"]
    clf: LogisticRegression = pipeline.named_steps["classifier"]

    artifact_dict: dict[str, Any] = {
        "model_type": "StandardScaler + LogisticRegression(class_weight='balanced')",
        "feature_names": list(FEATURE_COLUMNS),
        "scaler_mean": [float(v) for v in scaler.mean_.tolist()],
        "scaler_scale": [float(v) for v in scaler.scale_.tolist()],
        "coefficients": [float(v) for v in clf.coef_[0].tolist()],
        "intercept": float(clf.intercept_[0]),
        "default_threshold": float(cfg.fraud_threshold),
        "amount_quantiles": {
            "q33": q33,
            "q67": q67,
            "definition": "33.33% and 66.67% quantiles of Amount computed exclusively on training split",
        },
        "metadata": {
            "dataset_source": dataset_source_label,
            "dataset_sha256": dataset_sha256,
            "random_seed": cfg.random_seed,
            "test_size_fraction": cfg.test_size,
            "total_rows": int(len(df)),
            "train_rows": int(len(X_train)),
            "train_fraud_rows": int(y_train.sum()),
            "train_fraud_rate": float(y_train.mean()),
            "heldout_test_rows": int(len(X_test)),
            "heldout_test_fraud_rows": int(y_test.sum()),
            "heldout_test_fraud_rate": float(y_test.mean()),
            "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        },
    }

    cfg.model_joblib_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.model_artifact_path.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(
        {
            "pipeline": pipeline,
            "feature_names": FEATURE_COLUMNS,
            "amount_quantiles": {"q33": q33, "q67": q67},
            "default_threshold": cfg.fraud_threshold,
            "metadata": artifact_dict["metadata"],
        },
        cfg.model_joblib_path,
    )

    with cfg.model_artifact_path.open("w", encoding="utf-8") as f:
        json.dump(artifact_dict, f, indent=2)

    # Validate mathematical parity between scikit-learn and portable Flink scoring artifact
    artifact = LinearFraudModelArtifact.from_dict(artifact_dict)
    fixture_df = X_test.iloc[: min(250, len(X_test))]
    sklearn_probs = pipeline.predict_proba(fixture_df)[:, 1]
    portable_probs = np.array(
        [
            artifact.predict_proba_single(row.to_dict())
            for _, row in fixture_df.iterrows()
        ],
        dtype=np.float64,
    )
    max_abs_diff = float(np.max(np.abs(sklearn_probs - portable_probs)))
    if max_abs_diff > 1e-12:
        raise RuntimeError(
            f"Mathematical parity check failed: max_abs_diff={max_abs_diff:.3e} > 1e-12"
        )
    artifact_dict["metadata"]["parity_check_fixture_size"] = int(len(fixture_df))
    artifact_dict["metadata"]["parity_check_max_abs_diff"] = max_abs_diff

    # Re-write artifact with parity verification metadata included
    with cfg.model_artifact_path.open("w", encoding="utf-8") as f:
        json.dump(artifact_dict, f, indent=2)

    if save_heldout_csv:
        cfg.heldout_csv_path.parent.mkdir(parents=True, exist_ok=True)
        heldout_df = X_test.copy()
        heldout_df[TARGET_COLUMN] = y_test.values
        heldout_df.insert(
            0,
            "transaction_id",
            [f"tx_{idx:06d}" for idx in X_test.index],
        )
        heldout_df.to_csv(cfg.heldout_csv_path, index=False)

    return {
        "artifact": artifact_dict,
        "X_train": X_train,
        "y_train": y_train,
        "X_test": X_test,
        "y_test": y_test,
        "pipeline": pipeline,
    }
