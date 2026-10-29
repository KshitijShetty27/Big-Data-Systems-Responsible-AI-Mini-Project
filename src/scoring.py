"""Mathematically equivalent, portable fraud model scoring engine used inside Flink and offline validation."""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import FEATURE_COLUMNS
from src.schema import ScoredTransactionRecord, parse_and_validate_event


@dataclass(frozen=True)
class LinearFraudModelArtifact:
    """Portable representation of StandardScaler + LogisticRegression + quantile segment bounds."""

    feature_names: list[str]
    scaler_mean: list[float]
    scaler_scale: list[float]
    coefficients: list[float]
    intercept: float
    default_threshold: float
    amount_q33: float
    amount_q67: float
    metadata: dict[str, Any]

    @classmethod
    def from_json_file(cls, path: Path) -> LinearFraudModelArtifact:
        """Load model artifact from JSON file."""
        with Path(path).open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> LinearFraudModelArtifact:
        """Construct and validate artifact from dictionary."""
        feature_names = list(payload["feature_names"])
        if feature_names != FEATURE_COLUMNS:
            raise ValueError(
                f"Feature names mismatch. Expected {FEATURE_COLUMNS}, got {feature_names}"
            )
        n_features = len(feature_names)
        scaler_mean = [float(x) for x in payload["scaler_mean"]]
        scaler_scale = [float(x) for x in payload["scaler_scale"]]
        coefficients = [float(x) for x in payload["coefficients"]]
        if not (len(scaler_mean) == len(scaler_scale) == len(coefficients) == n_features):
            raise ValueError("Scaler parameters and coefficients must match feature length (30).")

        return cls(
            feature_names=feature_names,
            scaler_mean=scaler_mean,
            scaler_scale=scaler_scale,
            coefficients=coefficients,
            intercept=float(payload["intercept"]),
            default_threshold=float(payload["default_threshold"]),
            amount_q33=float(payload["amount_quantiles"]["q33"]),
            amount_q67=float(payload["amount_quantiles"]["q67"]),
            metadata=dict(payload.get("metadata", {})),
        )

    def assign_amount_segment(self, amount: float) -> str:
        """Assign transaction amount to low, medium, or high segment using training quantiles."""
        if amount <= self.amount_q33:
            return "low"
        if amount <= self.amount_q67:
            return "medium"
        return "high"

    def predict_proba_single(self, features: dict[str, float]) -> float:
        """
        Compute P(fraud | features) using exact StandardScaler + LogisticRegression formula.
        Never reads 'Class' even if present in dictionary.
        """
        logit = self.intercept
        for idx, col in enumerate(self.feature_names):
            x_val = float(features[col])
            scaled = (x_val - self.scaler_mean[idx]) / self.scaler_scale[idx]
            logit += self.coefficients[idx] * scaled

        # Numerically stable sigmoid function
        if logit >= 0.0:
            exp_neg = math.exp(-logit)
            return 1.0 / (1.0 + exp_neg)
        exp_pos = math.exp(logit)
        return exp_pos / (1.0 + exp_pos)

    def score_raw_payload(
        self,
        raw_payload: str | bytes,
        threshold_override: float | None = None,
        scoring_ts_ms_override: int | None = None,
    ) -> ScoredTransactionRecord:
        """
        Parse, validate, and score a single raw Kafka JSON payload.
        Returns a ScoredTransactionRecord with `is_valid=False` if malformed.
        """
        scoring_ts_ms = (
            int(scoring_ts_ms_override)
            if scoring_ts_ms_override is not None
            else int(time.time() * 1000)
        )
        threshold = (
            float(threshold_override)
            if threshold_override is not None
            else self.default_threshold
        )

        event, error_reason = parse_and_validate_event(raw_payload)
        if event is None or error_reason is not None:
            return ScoredTransactionRecord(
                transaction_id="INVALID",
                run_id="INVALID",
                time_sec=0.0,
                amount=0.0,
                amount_segment="unknown",
                v1=0.0,
                v2=0.0,
                v3=0.0,
                v4=0.0,
                v10=0.0,
                v12=0.0,
                v14=0.0,
                v17=0.0,
                actual_label=-1,
                predicted_label=-1,
                fraud_probability=0.0,
                threshold_used=threshold,
                producer_ts_ms=0,
                scoring_ts_ms=scoring_ts_ms,
                latency_ms=0,
                is_valid=False,
                error_reason=error_reason or "unknown_validation_error",
            )

        prob = self.predict_proba_single(event.features)
        pred_label = 1 if prob >= threshold else 0
        amount = float(event.features["Amount"])
        segment = self.assign_amount_segment(amount)
        latency_ms = max(0, scoring_ts_ms - event.producer_ts_ms)

        return ScoredTransactionRecord(
            transaction_id=event.transaction_id,
            run_id=event.run_id,
            time_sec=float(event.features["Time"]),
            amount=amount,
            amount_segment=segment,
            v1=float(event.features["V1"]),
            v2=float(event.features["V2"]),
            v3=float(event.features["V3"]),
            v4=float(event.features["V4"]),
            v10=float(event.features["V10"]),
            v12=float(event.features["V12"]),
            v14=float(event.features["V14"]),
            v17=float(event.features["V17"]),
            actual_label=int(event.actual_label) if event.actual_label is not None else -1,
            predicted_label=pred_label,
            fraud_probability=prob,
            threshold_used=threshold,
            producer_ts_ms=event.producer_ts_ms,
            scoring_ts_ms=scoring_ts_ms,
            latency_ms=latency_ms,
            is_valid=True,
            error_reason="",
        )
