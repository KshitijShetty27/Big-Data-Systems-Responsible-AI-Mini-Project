"""Unit tests for mathematical scoring parity, threshold behavior, and amount segments."""

from __future__ import annotations

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.config import FEATURE_COLUMNS, PROJECT_ROOT, get_config
from src.schema import TransactionEvent
from src.scoring import LinearFraudModelArtifact


def _load_fixture_df() -> pd.DataFrame:
    cfg = get_config()
    fixture_path = PROJECT_ROOT / "data" / "fixtures" / "heldout_sample_300.csv"
    if fixture_path.exists():
        return pd.read_csv(fixture_path)
    assert cfg.heldout_csv_path.exists(), "Neither fixture nor held-out dataset found"
    return pd.read_csv(cfg.heldout_csv_path, nrows=300)


def test_model_artifact_matches_sklearn_on_heldout_fixture() -> None:
    cfg = get_config()
    assert cfg.model_artifact_path.exists(), "Model artifact must exist before running parity test"

    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)
    fixture_df = _load_fixture_df()
    X_fixture = fixture_df[FEATURE_COLUMNS]

    # Reconstruct exact scikit-learn pipeline from artifact parameters (cross-version safe)
    scaler = StandardScaler()
    scaler.mean_ = np.array(artifact.scaler_mean, dtype=np.float64)
    scaler.scale_ = np.array(artifact.scaler_scale, dtype=np.float64)
    scaler.var_ = scaler.scale_ ** 2
    scaler.n_features_in_ = len(FEATURE_COLUMNS)
    scaler.feature_names_in_ = np.array(FEATURE_COLUMNS, dtype=object)

    clf = LogisticRegression()
    clf.classes_ = np.array([0, 1])
    clf.coef_ = np.array([artifact.coefficients], dtype=np.float64)
    clf.intercept_ = np.array([artifact.intercept], dtype=np.float64)
    clf.n_features_in_ = len(FEATURE_COLUMNS)

    sklearn_pipeline = Pipeline([("scaler", scaler), ("classifier", clf)])
    sklearn_probs = sklearn_pipeline.predict_proba(X_fixture)[:, 1]

    portable_probs = np.array(
        [artifact.predict_proba_single(row.to_dict()) for _, row in X_fixture.iterrows()],
        dtype=np.float64,
    )

    max_diff = float(np.max(np.abs(sklearn_probs - portable_probs)))
    assert max_diff < 1e-12, f"Expected parity < 1e-12, got {max_diff}"

    if "sklearn_prob" in fixture_df.columns:
        recorded_diff = float(np.max(np.abs(fixture_df["sklearn_prob"].to_numpy() - portable_probs)))
        assert recorded_diff < 1e-12, f"Expected recorded parity < 1e-12, got {recorded_diff}"


def test_amount_segment_assignment_and_thresholds() -> None:
    cfg = get_config()
    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)

    assert artifact.assign_amount_segment(0.0) == "low"
    assert artifact.assign_amount_segment(artifact.amount_q33) == "low"
    assert artifact.assign_amount_segment((artifact.amount_q33 + artifact.amount_q67) / 2.0) == "medium"
    assert artifact.assign_amount_segment(artifact.amount_q67) == "medium"
    assert artifact.assign_amount_segment(artifact.amount_q67 + 0.01) == "high"

    feats = {col: 0.0 for col in FEATURE_COLUMNS}
    feats["Amount"] = artifact.amount_q67 + 100.0
    ev = TransactionEvent(
        transaction_id="run_test_tx_1",
        run_id="run_test",
        producer_ts_ms=1700000000100,
        features=feats,
        actual_label=0,
    )
    scored_low_thresh = artifact.score_raw_payload(
        ev.to_json(), threshold_override=0.0, scoring_ts_ms_override=1700000000145
    )
    scored_high_thresh = artifact.score_raw_payload(
        ev.to_json(), threshold_override=1.0, scoring_ts_ms_override=1700000000145
    )
    assert scored_low_thresh.is_valid is True
    assert scored_low_thresh.predicted_label == 1
    assert scored_high_thresh.predicted_label == 0
    assert scored_low_thresh.latency_ms == 45
    assert scored_low_thresh.amount_segment == "high"


def test_malformed_payload_produces_invalid_scored_record() -> None:
    cfg = get_config()
    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)
    rec = artifact.score_raw_payload("not a json string", scoring_ts_ms_override=1700000000500)
    assert rec.is_valid is False
    assert rec.predicted_label == -1
    assert rec.error_reason.startswith("malformed_json")
