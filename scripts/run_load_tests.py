"""
Execute multi-rate streaming load tests (Kafka -> Flink -> Iceberg) and measure
offered rate, achieved producer rate, persisted record count, end-to-end throughput,
and p50/p95/p99 latency per run.
"""

from __future__ import annotations

import argparse
import json
import platform
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config
from src.iceberg_utils import (
    load_rest_iceberg_catalog,
    query_run_metrics_from_iceberg,
)
from src.producer import replay_transactions_to_kafka


def check_tcp_port(host: str, port: int, timeout: float = 2.0) -> tuple[bool, str]:
    """Check whether a TCP endpoint is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "reachable"
    except Exception as exc:
        return False, str(exc)


def parse_host_port(endpoint: str, default_port: int) -> tuple[str, int]:
    """Extract (host, port) from a bootstrap server or HTTP URL."""
    cleaned = endpoint.split(",")[0].strip()
    if "://" in cleaned:
        cleaned = cleaned.split("://", 1)[1]
    cleaned = cleaned.split("/", 1)[0]
    if ":" in cleaned:
        h, p = cleaned.rsplit(":", 1)
        return h, int(p)
    return cleaned, default_port


def wait_for_iceberg_persistence(
    table,
    run_id: str,
    expected_count: int,
    timeout_sec: float = 60.0,
    poll_interval_sec: float = 2.0,
) -> dict[str, Any]:
    """
    Poll the Apache Iceberg table until all expected valid records for `run_id`
    are committed by Flink checkpoints, or until `timeout_sec` expires.
    """
    deadline = time.perf_counter() + timeout_sec
    latest_metrics: dict[str, Any] = {}
    while time.perf_counter() < deadline:
        table.refresh()
        latest_metrics = query_run_metrics_from_iceberg(
            table=table,
            run_id=run_id,
            expected_valid_events=expected_count,
        )
        if latest_metrics.get("unique_transaction_ids", 0) >= expected_count:
            return latest_metrics
        time.sleep(poll_interval_sec)

    table.refresh()
    return query_run_metrics_from_iceberg(
        table=table,
        run_id=run_id,
        expected_valid_events=expected_count,
    )


def run_docker_load_tests(
    rates: list[float],
    events_per_run: int,
    duration_sec: float | None,
    settle_timeout_sec: float,
    inject_malformed: int,
) -> int:
    """Execute load tests against the running Docker Kafka -> Flink -> Iceberg stack."""
    cfg = get_config()
    cfg.results_dir.mkdir(parents=True, exist_ok=True)

    kafka_host, kafka_port = parse_host_port(cfg.kafka_bootstrap_servers, 29092)
    rest_host, rest_port = parse_host_port(cfg.iceberg_rest_uri, 8181)

    kafka_ok, kafka_msg = check_tcp_port(kafka_host, kafka_port)
    rest_ok, rest_msg = check_tcp_port(rest_host, rest_port)

    env_meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "os": platform.platform(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "kafka_bootstrap_servers": cfg.kafka_bootstrap_servers,
        "kafka_reachable": kafka_ok,
        "kafka_status": kafka_msg,
        "iceberg_rest_uri": cfg.iceberg_rest_uri,
        "iceberg_rest_reachable": rest_ok,
        "iceberg_rest_status": rest_msg,
        "configured_rates_eps": rates,
        "configured_events_per_run": events_per_run,
    }

    if not (kafka_ok and rest_ok):
        blocker_detail = (
            f"Docker streaming services are not reachable on host "
            f"(Kafka {kafka_host}:{kafka_port} -> {kafka_msg}; "
            f"Iceberg REST {rest_host}:{rest_port} -> {rest_msg}). "
            f"Ensure Docker Desktop / Docker Engine is installed and running, then start the cluster "
            f"with `docker compose up -d --build` and submit the Flink job before running load tests."
        )
        env_meta["status"] = "BLOCKED_DOCKER_UNAVAILABLE"
        env_meta["blocker"] = blocker_detail
        with (cfg.results_dir / "run_metadata.json").open("w", encoding="utf-8") as f:
            json.dump(env_meta, f, indent=2)

        blocked_payload = {
            "status": "BLOCKED_DOCKER_UNAVAILABLE",
            "blocker": blocker_detail,
            "environment": env_meta,
            "runs": [],
        }
        with (cfg.results_dir / "load_test_runs.json").open("w", encoding="utf-8") as f:
            json.dump(blocked_payload, f, indent=2)

        empty_df = pd.DataFrame(
            columns=[
                "run_id",
                "status",
                "offered_rate_eps",
                "achieved_producer_rate_eps",
                "sent_events",
                "delivered_events",
                "failed_deliveries",
                "malformed_injected",
                "persisted_count",
                "unique_transaction_ids",
                "duplicate_records",
                "missing_records",
                "e2e_throughput_rps",
                "latency_median_p50_ms",
                "latency_p95_ms",
                "latency_p99_ms",
                "latency_mean_ms",
                "latency_max_ms",
            ]
        )
        empty_df.to_csv(cfg.results_dir / "load_test_runs.csv", index=False)
        print(f"[BLOCKED] {blocker_detail}", file=sys.stderr)
        return 2

    if not cfg.heldout_csv_path.exists():
        print(
            f"[ERROR] Held-out dataset not found at {cfg.heldout_csv_path}. "
            f"Run `python scripts/train_model.py` first.",
            file=sys.stderr,
        )
        return 1

    heldout_df = pd.read_csv(cfg.heldout_csv_path)
    catalog = load_rest_iceberg_catalog(cfg)
    table_id = f"{cfg.iceberg_namespace}.{cfg.iceberg_table}"

    try:
        iceberg_table = catalog.load_table(table_id)
    except Exception as exc:
        print(
            f"[ERROR] Could not load Iceberg table '{table_id}' from {cfg.iceberg_rest_uri}: {exc}\n"
            f"Ensure the Flink streaming job (`src/flink_job.py`) is running so the table is registered.",
            file=sys.stderr,
        )
        return 1

    run_records: list[dict[str, Any]] = []
    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for idx, rate in enumerate(rates, start=1):
        run_id = f"loadrun_{idx}_{int(rate)}eps_{batch_ts}"
        print(
            f"\n[INFO] Starting Run {idx}/{len(rates)}: run_id={run_id}, "
            f"offered_rate={rate:.1f} eps, max_events={events_per_run} ..."
        )
        prod_metrics = replay_transactions_to_kafka(
            heldout_df=heldout_df,
            run_id=run_id,
            offered_rate_eps=rate,
            max_events=events_per_run,
            duration_sec=duration_sec,
            bootstrap_servers=cfg.kafka_bootstrap_servers,
            topic=cfg.kafka_topic,
            inject_malformed_count=inject_malformed,
        )
        print(
            f"[INFO] Producer completed: sent={prod_metrics.sent_events}, "
            f"achieved_rate={prod_metrics.achieved_rate_eps:.1f} eps. "
            f"Awaiting Flink checkpoint & Iceberg persistence..."
        )
        iceberg_metrics = wait_for_iceberg_persistence(
            table=iceberg_table,
            run_id=run_id,
            expected_count=prod_metrics.sent_events,
            timeout_sec=settle_timeout_sec,
        )

        combined = {
            "run_id": run_id,
            "status": (
                "COMPLETED"
                if iceberg_metrics["unique_transaction_ids"] == prod_metrics.sent_events
                else "INCOMPLETE_PERSISTENCE"
            ),
            "offered_rate_eps": prod_metrics.offered_rate_eps,
            "achieved_producer_rate_eps": round(prod_metrics.achieved_rate_eps, 2),
            "producer_elapsed_sec": round(prod_metrics.elapsed_sec, 4),
            "sent_events": prod_metrics.sent_events,
            "delivered_events": prod_metrics.delivered_events,
            "failed_deliveries": prod_metrics.failed_deliveries,
            "malformed_injected": prod_metrics.malformed_injected,
            "persisted_count": iceberg_metrics["persisted_count"],
            "unique_transaction_ids": iceberg_metrics["unique_transaction_ids"],
            "duplicate_records": iceberg_metrics["duplicate_records"],
            "missing_records": iceberg_metrics["missing_records"],
            "e2e_throughput_rps": round(iceberg_metrics["e2e_throughput_rps"], 2),
            "latency_min_ms": iceberg_metrics["latency_min_ms"],
            "latency_median_p50_ms": iceberg_metrics["latency_median_p50_ms"],
            "latency_mean_ms": (
                round(iceberg_metrics["latency_mean_ms"], 2)
                if iceberg_metrics["latency_mean_ms"] is not None
                else None
            ),
            "latency_p95_ms": iceberg_metrics["latency_p95_ms"],
            "latency_p99_ms": iceberg_metrics["latency_p99_ms"],
            "latency_max_ms": iceberg_metrics["latency_max_ms"],
            "predicted_frauds": iceberg_metrics["predicted_frauds"],
            "actual_frauds": iceberg_metrics["actual_frauds"],
        }
        run_records.append(combined)
        print(
            f"[OK] Run {run_id}: persisted={combined['persisted_count']}/{combined['sent_events']}, "
            f"e2e_throughput={combined['e2e_throughput_rps']:.1f} rps, "
            f"p50={combined['latency_median_p50_ms']} ms, "
            f"p95={combined['latency_p95_ms']} ms, p99={combined['latency_p99_ms']} ms"
        )

    env_meta["status"] = "COMPLETED"
    with (cfg.results_dir / "run_metadata.json").open("w", encoding="utf-8") as f:
        json.dump(env_meta, f, indent=2)

    out_payload = {
        "status": "COMPLETED",
        "environment": env_meta,
        "metric_definitions": {
            "offered_rate_eps": "Target configured producer event rate in events per second.",
            "achieved_producer_rate_eps": "sent_events / producer_elapsed_sec measured at producer flush completion.",
            "persisted_count": "Total valid records committed to Apache Iceberg table for this run_id.",
            "e2e_throughput_rps": "unique_transaction_ids / ((max(scoring_ts_ms) - min(producer_ts_ms)) / 1000.0).",
            "latency_ms": "scoring_ts_ms - producer_ts_ms recorded per event inside Flink scoring UDF.",
        },
        "runs": run_records,
    }
    with (cfg.results_dir / "load_test_runs.json").open("w", encoding="utf-8") as f:
        json.dump(out_payload, f, indent=2)

    pd.DataFrame(run_records).to_csv(cfg.results_dir / "load_test_runs.csv", index=False)
    print(f"\n[OK] Saved load test results to {cfg.results_dir / 'load_test_runs.csv'}")
    return 0


def main() -> None:
    cfg = get_config()
    parser = argparse.ArgumentParser(description="Run multi-rate streaming load tests.")
    parser.add_argument(
        "--rates",
        type=str,
        default="200,500,1000",
        help="Comma-separated offered event rates in events/sec (default: 200,500,1000)",
    )
    parser.add_argument(
        "--events-per-run",
        type=int,
        default=3000,
        help="Number of held-out events to replay per rate run (default: 3000)",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=None,
        help="Optional max duration in seconds per run",
    )
    parser.add_argument(
        "--settle-timeout-sec",
        type=float,
        default=60.0,
        help="Max seconds to wait for Flink Iceberg checkpoint commit (default: 60)",
    )
    parser.add_argument(
        "--inject-malformed",
        type=int,
        default=5,
        help="Number of malformed events to inject per run to verify fault isolation (default: 5)",
    )
    args = parser.parse_args()
    rates = [float(r.strip()) for r in args.rates.split(",") if r.strip()]
    if len(rates) < 3:
        print("[ERROR] At least three increasing event rates are required.", file=sys.stderr)
        sys.exit(1)

    rc = run_docker_load_tests(
        rates=rates,
        events_per_run=args.events_per_run,
        duration_sec=args.duration_sec,
        settle_timeout_sec=args.settle_timeout_sec,
        inject_malformed=args.inject_malformed,
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
