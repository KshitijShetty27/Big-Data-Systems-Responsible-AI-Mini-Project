"""
Automated smoke test script:
Verifies event serialization, Flink UDF scoring logic, malformed-event isolation,
run_id isolation across three increasing rates, and Apache Iceberg table persistence
and querying using a local SQLite Iceberg catalog.
Explicitly labelled as a smoke test (never presented as Docker cluster load-test findings).
"""

from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config
from src.iceberg_utils import (
    append_records_to_iceberg,
    ensure_iceberg_table,
    load_local_sql_iceberg_catalog,
    query_run_metrics_from_iceberg,
)
from src.producer import replay_transactions_to_kafka
from src.schema import ScoredTransactionRecord
from src.scoring import LinearFraudModelArtifact


def run_smoke_test() -> dict[str, Any]:
    cfg = get_config()
    fixture_csv = PROJECT_ROOT / "data" / "fixtures" / "heldout_sample_300.csv"
    if not cfg.model_artifact_path.exists():
        raise RuntimeError(
            "Model artifact missing. Run `python scripts/train_model.py` first."
        )

    artifact = LinearFraudModelArtifact.from_json_file(cfg.model_artifact_path)
    if cfg.heldout_csv_path.exists():
        heldout_df = pd.read_csv(cfg.heldout_csv_path, nrows=1000)
    elif fixture_csv.exists():
        heldout_df = pd.read_csv(fixture_csv)
    else:
        raise RuntimeError(
            "Held-out dataset missing. Run `python scripts/train_model.py` first."
        )

    warehouse_dir = PROJECT_ROOT / "warehouse_local" / "smoke_test"
    if warehouse_dir.exists():
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    catalog = load_local_sql_iceberg_catalog(warehouse_dir)
    table = ensure_iceberg_table(catalog, cfg.iceberg_namespace, cfg.iceberg_table)

    smoke_rates = [200.0, 500.0, 1000.0]
    events_per_smoke_run = 200
    malformed_per_smoke_run = 5
    smoke_runs: list[dict[str, Any]] = []

    for idx, rate in enumerate(smoke_rates, start=1):
        run_id = f"smoke_run_{idx}_{int(rate)}eps"
        scored_buffer: list[ScoredTransactionRecord] = []
        malformed_buffer: list[ScoredTransactionRecord] = []

        def _in_process_flink_udf_bridge(_key: bytes, payload: bytes) -> None:
            rec = artifact.score_raw_payload(payload)
            if rec.is_valid:
                scored_buffer.append(rec)
            else:
                malformed_buffer.append(rec)

        prod_metrics = replay_transactions_to_kafka(
            heldout_df=heldout_df,
            run_id=run_id,
            offered_rate_eps=rate,
            max_events=events_per_smoke_run,
            inject_malformed_count=malformed_per_smoke_run,
            sink_callback=_in_process_flink_udf_bridge,
        )

        persisted_n = append_records_to_iceberg(table, scored_buffer)
        iceberg_metrics = query_run_metrics_from_iceberg(
            table=table,
            run_id=run_id,
            expected_valid_events=events_per_smoke_run,
        )

        assert persisted_n == events_per_smoke_run
        assert len(malformed_buffer) == malformed_per_smoke_run
        assert iceberg_metrics["unique_transaction_ids"] == events_per_smoke_run
        assert iceberg_metrics["duplicate_records"] == 0
        assert iceberg_metrics["missing_records"] == 0

        smoke_runs.append(
            {
                "run_id": run_id,
                "offered_rate_eps": rate,
                "achieved_producer_rate_eps": round(prod_metrics.achieved_rate_eps, 2),
                "sent_events": prod_metrics.sent_events,
                "malformed_injected": prod_metrics.malformed_injected,
                "malformed_caught": len(malformed_buffer),
                "persisted_count": iceberg_metrics["persisted_count"],
                "unique_transaction_ids": iceberg_metrics["unique_transaction_ids"],
                "duplicate_records": iceberg_metrics["duplicate_records"],
                "missing_records": iceberg_metrics["missing_records"],
                "e2e_throughput_rps": round(iceberg_metrics["e2e_throughput_rps"], 2),
                "latency_median_p50_ms": iceberg_metrics["latency_median_p50_ms"],
                "latency_p95_ms": iceberg_metrics["latency_p95_ms"],
                "latency_p99_ms": iceberg_metrics["latency_p99_ms"],
            }
        )

    # Verify total table rows equal sum across all 3 runs without cross-run contamination
    total_table_rows = len(table.scan().to_pandas())
    assert total_table_rows == len(smoke_rates) * events_per_smoke_run

    summary = {
        "is_smoke_test": True,
        "smoke_test_note": (
            "Explicitly labelled functional smoke test verifying producer serialization, "
            "Flink UDF linear scoring, malformed-event filtering, run_id isolation, and "
            "Apache Iceberg table commits/queries using a local SQLite Iceberg catalog. "
            "Not a substitute for Docker Kafka -> Flink -> Iceberg cluster load-test metrics."
        ),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_persisted_across_runs": total_table_rows,
        "runs": smoke_runs,
        "status": "PASSED",
    }

    cfg.results_dir.mkdir(parents=True, exist_ok=True)
    with (cfg.results_dir / "smoke_test_verification.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    shutil.rmtree(PROJECT_ROOT / "warehouse_local", ignore_errors=True)
    print(
        f"[OK] Smoke test PASSED across {len(smoke_rates)} rates: "
        f"{total_table_rows} total valid events persisted to Apache Iceberg, "
        f"5 malformed events isolated per run, 0 duplicates, 0 missing."
    )
    return summary


if __name__ == "__main__":
    run_smoke_test()
