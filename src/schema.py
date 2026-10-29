"""Consistent schema definitions and validation for transaction events and scored records."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from typing import Any

from src.config import FEATURE_COLUMNS


@dataclass(frozen=True)
class TransactionEvent:
    """Schema for raw credit card transaction events published to Kafka."""

    transaction_id: str
    run_id: str
    producer_ts_ms: int
    features: dict[str, float]
    actual_label: int | None = None

    def to_json(self) -> str:
        """Serialize event to JSON string without whitespace."""
        payload = {
            "transaction_id": self.transaction_id,
            "run_id": self.run_id,
            "producer_ts_ms": self.producer_ts_ms,
            "features": self.features,
            "actual_label": self.actual_label,
        }
        return json.dumps(payload, separators=(",", ":"))


@dataclass(frozen=True)
class ScoredTransactionRecord:
    """Schema for scored credit card transaction records persisted to Apache Iceberg."""

    transaction_id: str
    run_id: str
    time_sec: float
    amount: float
    amount_segment: str
    v1: float
    v2: float
    v3: float
    v4: float
    v10: float
    v12: float
    v14: float
    v17: float
    actual_label: int
    predicted_label: int
    fraud_probability: float
    threshold_used: float
    producer_ts_ms: int
    scoring_ts_ms: int
    latency_ms: int
    is_valid: bool = True
    error_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_and_validate_event(raw_payload: str | bytes) -> tuple[TransactionEvent | None, str | None]:
    """
    Parse and validate a raw JSON message from Kafka.

    Returns:
        (TransactionEvent, None) if valid, or (None, error_reason) if malformed.
    """
    if raw_payload is None:
        return None, "empty_payload"

    if isinstance(raw_payload, bytes):
        try:
            raw_payload = raw_payload.decode("utf-8")
        except UnicodeDecodeError:
            return None, "invalid_utf8"

    raw_payload = raw_payload.strip()
    if not raw_payload:
        return None, "empty_payload"

    try:
        data = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        return None, f"malformed_json:{exc.msg}"

    if not isinstance(data, dict):
        return None, "payload_not_json_object"

    tx_id = data.get("transaction_id")
    run_id = data.get("run_id")
    producer_ts = data.get("producer_ts_ms")
    features_raw = data.get("features")

    if not isinstance(tx_id, str) or not tx_id.strip():
        return None, "missing_or_invalid_transaction_id"
    if not isinstance(run_id, str) or not run_id.strip():
        return None, "missing_or_invalid_run_id"
    if not isinstance(producer_ts, (int, float)) or isinstance(producer_ts, bool) or producer_ts <= 0:
        return None, "missing_or_invalid_producer_ts_ms"
    if not isinstance(features_raw, dict):
        return None, "missing_or_invalid_features_object"

    # Never allow target column "Class" inside model input features
    if "Class" in features_raw:
        return None, "target_leakage_class_in_features"

    validated_features: dict[str, float] = {}
    for col in FEATURE_COLUMNS:
        if col not in features_raw:
            return None, f"missing_feature:{col}"
        val = features_raw[col]
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            return None, f"non_numeric_feature:{col}"
        fval = float(val)
        if math.isnan(fval) or math.isinf(fval):
            return None, f"nan_or_inf_feature:{col}"
        validated_features[col] = fval

    if validated_features["Amount"] < 0.0:
        return None, "negative_transaction_amount"

    actual_raw = data.get("actual_label")
    actual_label: int | None = None
    if actual_raw is not None:
        if isinstance(actual_raw, bool) or actual_raw not in (0, 1):
            return None, "invalid_actual_label"
        actual_label = int(actual_raw)

    return (
        TransactionEvent(
            transaction_id=tx_id.strip(),
            run_id=run_id.strip(),
            producer_ts_ms=int(producer_ts),
            features=validated_features,
            actual_label=actual_label,
        ),
        None,
    )
