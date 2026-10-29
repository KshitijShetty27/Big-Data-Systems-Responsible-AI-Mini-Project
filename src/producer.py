"""Kafka transaction replay producer with configurable token-bucket rate control and run tracking."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import FEATURE_COLUMNS, TARGET_COLUMN, PipelineConfig, get_config
from src.schema import TransactionEvent


@dataclass
class ProducerRunMetrics:
    """Measured producer telemetry for a single load-test replay run."""

    run_id: str
    topic: str
    offered_rate_eps: float
    configured_max_events: int
    configured_duration_sec: float | None
    sent_events: int
    delivered_events: int
    failed_deliveries: int
    malformed_injected: int
    elapsed_sec: float
    achieved_rate_eps: float
    start_ts_ms: int
    end_ts_ms: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_event_from_row(
    row: pd.Series,
    row_idx: int,
    run_id: str,
    producer_ts_ms: int | None = None,
) -> TransactionEvent:
    """Convert a DataFrame row from held-out transactions into a TransactionEvent."""
    tx_id_raw = row.get("transaction_id", f"tx_{row_idx:06d}")
    tx_id = f"{run_id}_{tx_id_raw}"
    ts_ms = int(producer_ts_ms) if producer_ts_ms is not None else int(time.time() * 1000)

    # Strictly exclude target label from model input features dictionary
    features = {col: float(row[col]) for col in FEATURE_COLUMNS}
    actual_label = int(row[TARGET_COLUMN]) if TARGET_COLUMN in row and pd.notna(row[TARGET_COLUMN]) else None

    return TransactionEvent(
        transaction_id=tx_id,
        run_id=run_id,
        producer_ts_ms=ts_ms,
        features=features,
        actual_label=actual_label,
    )


def create_kafka_producer(bootstrap_servers: str):
    """Create a Kafka producer using confluent_kafka or kafka-python."""
    try:
        from confluent_kafka import Producer  # type: ignore

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "fraud-replay-producer",
            "acks": "all",
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "enable.idempotence": True,
        }
        return ("confluent", Producer(conf))
    except ImportError:
        from kafka import KafkaProducer  # type: ignore

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            acks="all",
            linger_ms=5,
            retries=5,
        )
        return ("kafka_python", producer)


def replay_transactions_to_kafka(
    heldout_df: pd.DataFrame,
    run_id: str,
    offered_rate_eps: float,
    max_events: int = 5000,
    duration_sec: float | None = None,
    bootstrap_servers: str | None = None,
    topic: str | None = None,
    inject_malformed_count: int = 0,
    sink_callback=None,
) -> ProducerRunMetrics:
    """
    Replay held-out transactions into Kafka (or an injected sink callback for testing)
    at a controlled offered event rate (events per second).
    """
    cfg = get_config()
    servers = bootstrap_servers or cfg.kafka_bootstrap_servers
    target_topic = topic or cfg.kafka_topic

    if offered_rate_eps <= 0:
        raise ValueError(f"offered_rate_eps must be positive, got {offered_rate_eps}")

    backend_type = "callback"
    producer = None
    if sink_callback is None:
        backend_type, producer = create_kafka_producer(servers)

    delivered = 0
    failed = 0

    def _on_delivery(err, _msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
        else:
            delivered += 1

    total_rows = len(heldout_df)
    if total_rows == 0:
        raise ValueError("heldout_df is empty; cannot replay transactions.")

    start_wall = time.perf_counter()
    start_ts_ms = int(time.time() * 1000)
    sent = 0
    malformed_sent = 0

    for idx in range(max_events):
        now_wall = time.perf_counter()
        if duration_sec is not None and (now_wall - start_wall) >= duration_sec:
            break

        row = heldout_df.iloc[idx % total_rows]
        now_ms = int(time.time() * 1000)
        event = build_event_from_row(row=row, row_idx=idx, run_id=run_id, producer_ts_ms=now_ms)
        payload_bytes = event.to_json().encode("utf-8")
        key_bytes = event.transaction_id.encode("utf-8")

        if sink_callback is not None:
            sink_callback(key_bytes, payload_bytes)
            delivered += 1
        elif backend_type == "confluent":
            producer.produce(
                target_topic,
                key=key_bytes,
                value=payload_bytes,
                on_delivery=_on_delivery,
            )
            if idx % 200 == 0:
                producer.poll(0)
        else:
            future = producer.send(target_topic, key=key_bytes, value=payload_bytes)
            future.add_callback(lambda _m: _on_delivery(None, _m))
            future.add_errback(lambda exc: _on_delivery(exc, None))

        sent += 1

        # High-precision pacing against target schedule
        expected_elapsed = sent / offered_rate_eps
        actual_elapsed = time.perf_counter() - start_wall
        sleep_needed = expected_elapsed - actual_elapsed
        if sleep_needed > 0.001:
            time.sleep(sleep_needed)

    # Optionally inject malformed events to test Flink dead-letter / validation handling
    for m_idx in range(inject_malformed_count):
        bad_payload = json.dumps(
            {
                "transaction_id": f"{run_id}_malformed_{m_idx}",
                "run_id": run_id,
                "producer_ts_ms": int(time.time() * 1000),
                "features": {"Time": 0.0, "Amount": -999.0},  # Missing V1..V28 and negative amount
            }
        ).encode("utf-8")
        bad_key = f"{run_id}_malformed_{m_idx}".encode("utf-8")
        if sink_callback is not None:
            sink_callback(bad_key, bad_payload)
        elif backend_type == "confluent":
            producer.produce(target_topic, key=bad_key, value=bad_payload)
        else:
            producer.send(target_topic, key=bad_key, value=bad_payload)
        malformed_sent += 1

    if producer is not None:
        producer.flush(30)

    end_wall = time.perf_counter()
    end_ts_ms = int(time.time() * 1000)
    elapsed_sec = max(1e-6, end_wall - start_wall)
    achieved_rate = sent / elapsed_sec

    return ProducerRunMetrics(
        run_id=run_id,
        topic=target_topic,
        offered_rate_eps=offered_rate_eps,
        configured_max_events=max_events,
        configured_duration_sec=duration_sec,
        sent_events=sent,
        delivered_events=delivered,
        failed_deliveries=failed,
        malformed_injected=malformed_sent,
        elapsed_sec=elapsed_sec,
        achieved_rate_eps=achieved_rate,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
    )
