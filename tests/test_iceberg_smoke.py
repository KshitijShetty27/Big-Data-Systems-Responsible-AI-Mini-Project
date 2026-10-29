"""Unit test verifying Apache Iceberg persistence, run_id isolation, and query metrics."""

from __future__ import annotations

import shutil
from pathlib import Path

from src.config import FEATURE_COLUMNS
from src.iceberg_utils import (
    append_records_to_iceberg,
    ensure_iceberg_table,
    load_local_sql_iceberg_catalog,
    query_run_metrics_from_iceberg,
)
from src.schema import TransactionEvent
from src.scoring import LinearFraudModelArtifact
from src.config import get_config


def test_iceberg_persistence_and_run_isolation(tmp_path: Path) -> None:
    cfg = get_config()
    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)

    wh_dir = tmp_path / "iceberg_wh"
    catalog = load_local_sql_iceberg_catalog(wh_dir)
    table = ensure_iceberg_table(catalog, "fraud_detection", "scored_transactions")

    feats = {col: 0.0 for col in FEATURE_COLUMNS}
    feats["Amount"] = 25.0

    run_a_records = [
        artifact.score_raw_payload(
            TransactionEvent(
                transaction_id=f"runA_tx_{i}",
                run_id="runA",
                producer_ts_ms=1700000000000 + i * 10,
                features=feats,
                actual_label=0,
            ).to_json(),
            scoring_ts_ms_override=1700000000000 + i * 10 + 12,
        )
        for i in range(10)
    ]
    run_b_records = [
        artifact.score_raw_payload(
            TransactionEvent(
                transaction_id=f"runB_tx_{i}",
                run_id="runB",
                producer_ts_ms=1700000010000 + i * 10,
                features=feats,
                actual_label=1,
            ).to_json(),
            scoring_ts_ms_override=1700000010000 + i * 10 + 25,
        )
        for i in range(6)
    ]

    assert append_records_to_iceberg(table, run_a_records) == 10
    assert append_records_to_iceberg(table, run_b_records) == 6

    metrics_a = query_run_metrics_from_iceberg(table, "runA", expected_valid_events=10)
    metrics_b = query_run_metrics_from_iceberg(table, "runB", expected_valid_events=6)

    assert metrics_a["persisted_count"] == 10
    assert metrics_a["unique_transaction_ids"] == 10
    assert metrics_a["duplicate_records"] == 0
    assert metrics_a["missing_records"] == 0
    assert metrics_a["latency_median_p50_ms"] == 12.0

    assert metrics_b["persisted_count"] == 6
    assert metrics_b["unique_transaction_ids"] == 6
    assert metrics_b["actual_frauds"] == 6
    assert metrics_b["latency_median_p50_ms"] == 25.0

    shutil.rmtree(wh_dir, ignore_errors=True)
