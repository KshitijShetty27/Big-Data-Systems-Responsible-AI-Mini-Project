"""Unit tests for event parsing, validation, and target-leakage prevention."""

from __future__ import annotations

import json

from src.config import FEATURE_COLUMNS
from src.schema import TransactionEvent, parse_and_validate_event


def _make_valid_features(amount: float = 42.50) -> dict[str, float]:
    feats = {col: 0.1 * idx for idx, col in enumerate(FEATURE_COLUMNS)}
    feats["Time"] = 1234.0
    feats["Amount"] = amount
    return feats


def test_valid_transaction_event_roundtrip() -> None:
    feats = _make_valid_features(125.75)
    ev = TransactionEvent(
        transaction_id="run1_tx_00001",
        run_id="run1",
        producer_ts_ms=1700000000123,
        features=feats,
        actual_label=1,
    )
    raw_json = ev.to_json()
    parsed, err = parse_and_validate_event(raw_json)
    assert err is None
    assert parsed is not None
    assert parsed.transaction_id == "run1_tx_00001"
    assert parsed.run_id == "run1"
    assert parsed.producer_ts_ms == 1700000000123
    assert parsed.actual_label == 1
    assert parsed.features["Amount"] == 125.75


def test_rejects_malformed_json_and_empty() -> None:
    ev, err = parse_and_validate_event("")
    assert ev is None and err == "empty_payload"

    ev2, err2 = parse_and_validate_event("{not-valid-json")
    assert ev2 is None and err2 is not None and err2.startswith("malformed_json")


def test_rejects_target_leakage_in_features() -> None:
    feats = _make_valid_features()
    feats["Class"] = 1.0  # Forbidden target label inside input features
    payload = json.dumps(
        {
            "transaction_id": "tx_1",
            "run_id": "run_1",
            "producer_ts_ms": 1700000000000,
            "features": feats,
        }
    )
    ev, err = parse_and_validate_event(payload)
    assert ev is None
    assert err == "target_leakage_class_in_features"


def test_rejects_missing_feature_or_negative_amount() -> None:
    feats = _make_valid_features(-5.0)
    payload_neg = json.dumps(
        {
            "transaction_id": "tx_1",
            "run_id": "run_1",
            "producer_ts_ms": 1700000000000,
            "features": feats,
        }
    )
    ev, err = parse_and_validate_event(payload_neg)
    assert ev is None and err == "negative_transaction_amount"

    feats_missing = _make_valid_features(10.0)
    del feats_missing["V14"]
    payload_missing = json.dumps(
        {
            "transaction_id": "tx_1",
            "run_id": "run_1",
            "producer_ts_ms": 1700000000000,
            "features": feats_missing,
        }
    )
    ev2, err2 = parse_and_validate_event(payload_missing)
    assert ev2 is None and err2 == "missing_feature:V14"
