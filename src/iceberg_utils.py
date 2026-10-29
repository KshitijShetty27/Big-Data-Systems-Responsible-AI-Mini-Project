"""Apache Iceberg catalog connection, table management, and query utilities via PyIceberg."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

from src.config import PipelineConfig, get_config
from src.schema import ScoredTransactionRecord


def get_scored_arrow_schema() -> pa.Schema:
    """Return the PyArrow schema matching the Apache Iceberg scored_transactions table."""
    return pa.schema(
        [
            pa.field("transaction_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("time_sec", pa.float64(), nullable=False),
            pa.field("amount", pa.float64(), nullable=False),
            pa.field("amount_segment", pa.string(), nullable=False),
            pa.field("v1", pa.float64(), nullable=False),
            pa.field("v2", pa.float64(), nullable=False),
            pa.field("v3", pa.float64(), nullable=False),
            pa.field("v4", pa.float64(), nullable=False),
            pa.field("v10", pa.float64(), nullable=False),
            pa.field("v12", pa.float64(), nullable=False),
            pa.field("v14", pa.float64(), nullable=False),
            pa.field("v17", pa.float64(), nullable=False),
            pa.field("actual_label", pa.int32(), nullable=False),
            pa.field("predicted_label", pa.int32(), nullable=False),
            pa.field("fraud_probability", pa.float64(), nullable=False),
            pa.field("threshold_used", pa.float64(), nullable=False),
            pa.field("producer_ts_ms", pa.int64(), nullable=False),
            pa.field("scoring_ts_ms", pa.int64(), nullable=False),
            pa.field("latency_ms", pa.int64(), nullable=False),
        ]
    )


def load_rest_iceberg_catalog(config: PipelineConfig | None = None):
    """Connect to the Apache Iceberg REST catalog backed by MinIO S3."""
    from pyiceberg.catalog import load_catalog

    cfg = config or get_config()
    return load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": cfg.iceberg_rest_uri,
            "warehouse": cfg.iceberg_warehouse,
            "s3.endpoint": cfg.s3_endpoint,
            "s3.access-key-id": cfg.s3_access_key,
            "s3.secret-access-key": cfg.s3_secret_key,
            "s3.region": cfg.s3_region,
            "s3.path-style-access": "true",
        },
    )


def load_local_sql_iceberg_catalog(warehouse_dir: Path):
    """
    Create or load a local SQLite-backed Apache Iceberg catalog on the local filesystem.
    Used for smoke tests when the Docker REST + MinIO cluster is not running.
    """
    from pyiceberg.catalog import load_catalog

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    db_path = (warehouse_dir / "iceberg_catalog.db").resolve().as_posix()
    wh_uri = warehouse_dir.resolve().as_uri()
    return load_catalog(
        "local_sql",
        **{
            "type": "sql",
            "uri": f"sqlite:///{db_path}",
            "warehouse": wh_uri,
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        },
    )


def ensure_iceberg_table(catalog, namespace: str, table_name: str):
    """Ensure namespace and scored_transactions Iceberg table exist in the given catalog."""
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass

    identifier = f"{namespace}.{table_name}"
    try:
        return catalog.load_table(identifier)
    except Exception:
        return catalog.create_table(identifier=identifier, schema=get_scored_arrow_schema())


def append_records_to_iceberg(
    table,
    records: list[ScoredTransactionRecord],
) -> int:
    """Append a batch of valid ScoredTransactionRecord objects to an Apache Iceberg table."""
    valid_records = [r for r in records if r.is_valid]
    if not valid_records:
        return 0

    rows = [
        {
            "transaction_id": r.transaction_id,
            "run_id": r.run_id,
            "time_sec": float(r.time_sec),
            "amount": float(r.amount),
            "amount_segment": r.amount_segment,
            "v1": float(r.v1),
            "v2": float(r.v2),
            "v3": float(r.v3),
            "v4": float(r.v4),
            "v10": float(r.v10),
            "v12": float(r.v12),
            "v14": float(r.v14),
            "v17": float(r.v17),
            "actual_label": int(r.actual_label),
            "predicted_label": int(r.predicted_label),
            "fraud_probability": float(r.fraud_probability),
            "threshold_used": float(r.threshold_used),
            "producer_ts_ms": int(r.producer_ts_ms),
            "scoring_ts_ms": int(r.scoring_ts_ms),
            "latency_ms": int(r.latency_ms),
        }
        for r in valid_records
    ]
    arrow_table = pa.Table.from_pylist(rows, schema=get_scored_arrow_schema())
    table.append(arrow_table)
    return len(rows)


def query_run_metrics_from_iceberg(
    table,
    run_id: str,
    expected_valid_events: int | None = None,
) -> dict[str, Any]:
    """
    Query persisted records for a specific `run_id` from an Apache Iceberg table
    and calculate throughput, latency percentiles, and data-integrity counts.
    """
    from pyiceberg.expressions import EqualTo

    scan = table.scan(row_filter=EqualTo("run_id", run_id))
    df: pd.DataFrame = scan.to_pandas()

    persisted_count = int(len(df))
    if persisted_count == 0:
        return {
            "run_id": run_id,
            "persisted_count": 0,
            "unique_transaction_ids": 0,
            "duplicate_records": 0,
            "missing_records": expected_valid_events if expected_valid_events is not None else 0,
            "e2e_throughput_rps": 0.0,
            "latency_min_ms": None,
            "latency_median_p50_ms": None,
            "latency_mean_ms": None,
            "latency_p95_ms": None,
            "latency_p99_ms": None,
            "latency_max_ms": None,
            "predicted_frauds": 0,
            "actual_frauds": 0,
        }

    unique_tx = int(df["transaction_id"].nunique())
    duplicates = persisted_count - unique_tx
    missing = (
        max(0, int(expected_valid_events) - unique_tx)
        if expected_valid_events is not None
        else 0
    )

    latencies = df["latency_ms"].to_numpy(dtype=np.float64)
    min_prod_ts = int(df["producer_ts_ms"].min())
    max_score_ts = int(df["scoring_ts_ms"].max())
    window_sec = max(1e-3, (max_score_ts - min_prod_ts) / 1000.0)
    e2e_throughput = float(unique_tx / window_sec)

    return {
        "run_id": run_id,
        "persisted_count": persisted_count,
        "unique_transaction_ids": unique_tx,
        "duplicate_records": duplicates,
        "missing_records": missing,
        "window_duration_sec": window_sec,
        "e2e_throughput_rps": e2e_throughput,
        "latency_min_ms": float(np.min(latencies)),
        "latency_median_p50_ms": float(np.percentile(latencies, 50)),
        "latency_mean_ms": float(np.mean(latencies)),
        "latency_p95_ms": float(np.percentile(latencies, 95)),
        "latency_p99_ms": float(np.percentile(latencies, 99)),
        "latency_max_ms": float(np.max(latencies)),
        "predicted_frauds": int((df["predicted_label"] == 1).sum()),
        "actual_frauds": int((df["actual_label"] == 1).sum()),
    }
