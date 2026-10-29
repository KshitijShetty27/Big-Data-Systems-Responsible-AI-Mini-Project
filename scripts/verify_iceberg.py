"""Inspect and verify scored records persisted in the Apache Iceberg table."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config
from src.iceberg_utils import (
    load_local_sql_iceberg_catalog,
    load_rest_iceberg_catalog,
    query_run_metrics_from_iceberg,
)


def main() -> None:
    cfg = get_config()
    parser = argparse.ArgumentParser(description="Query and verify Apache Iceberg scored table.")
    parser.add_argument(
        "--local-warehouse",
        type=Path,
        default=None,
        help="Optional path to local SQLite Iceberg warehouse (for smoke test inspection)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Optional specific run_id to filter by",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of sample records to display (default: 10)",
    )
    args = parser.parse_args()

    try:
        if args.local_warehouse is not None:
            catalog = load_local_sql_iceberg_catalog(args.local_warehouse)
        else:
            catalog = load_rest_iceberg_catalog(cfg)
        table_id = f"{cfg.iceberg_namespace}.{cfg.iceberg_table}"
        table = catalog.load_table(table_id)
    except Exception as exc:
        print(
            f"[ERROR] Failed to connect to Apache Iceberg table '{cfg.iceberg_namespace}.{cfg.iceberg_table}': {exc}",
            file=sys.stderr,
        )
        sys.exit(1)

    df: pd.DataFrame = table.scan().to_pandas()
    print(f"=== Apache Iceberg Table: {cfg.iceberg_namespace}.{cfg.iceberg_table} ===")
    print(f"Total Persisted Rows: {len(df):,}")

    if len(df) == 0:
        return

    run_ids = sorted(df["run_id"].unique().tolist())
    print(f"Distinct Run IDs ({len(run_ids)}): {run_ids}")

    target_runs = [args.run_id] if args.run_id else run_ids
    summaries = [query_run_metrics_from_iceberg(table, rid) for rid in target_runs]
    summary_df = pd.DataFrame(summaries)
    print("\n--- Per-Run Persistence & Latency Summary ---")
    print(summary_df.to_string(index=False))

    print(f"\n--- Sample Records (first {args.limit}) ---")
    cols = [
        "transaction_id",
        "run_id",
        "amount",
        "amount_segment",
        "actual_label",
        "predicted_label",
        "fraud_probability",
        "latency_ms",
    ]
    print(df[cols].head(args.limit).to_string(index=False))


if __name__ == "__main__":
    main()
