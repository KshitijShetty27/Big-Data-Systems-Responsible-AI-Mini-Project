"""
Genuine Apache Flink (PyFlink) streaming job:
Kafka (raw JSON events) -> Flink UDF Fraud Scoring -> Apache Iceberg REST Catalog (MinIO S3).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config
from src.scoring import LinearFraudModelArtifact


def build_kafka_source_ddl(
    bootstrap_servers: str,
    topic: str,
    consumer_group: str,
    startup_mode: str = "earliest-offset",
) -> str:
    """Return Flink SQL DDL for consuming raw UTF-8 strings from Apache Kafka."""
    return f"""
    CREATE TEMPORARY TABLE kafka_raw_transactions (
        `raw_payload` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = '{consumer_group}',
        'scan.startup.mode' = '{startup_mode}',
        'format' = 'raw'
    )
    """


def build_iceberg_catalog_ddl(
    catalog_name: str,
    rest_uri: str,
    warehouse: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_region: str,
) -> str:
    """Return Flink SQL DDL for registering the Apache Iceberg REST Catalog backed by MinIO S3."""
    return f"""
    CREATE CATALOG {catalog_name} WITH (
        'type' = 'iceberg',
        'catalog-type' = 'rest',
        'uri' = '{rest_uri}',
        'warehouse' = '{warehouse}',
        'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
        's3.endpoint' = '{s3_endpoint}',
        's3.path-style-access' = 'true',
        'client.region' = '{s3_region}',
        's3.access-key-id' = '{s3_access_key}',
        's3.secret-access-key' = '{s3_secret_key}'
    )
    """


def build_scored_table_ddl(catalog_name: str, namespace: str, table_name: str) -> str:
    """Return Flink SQL DDL for the scored_transactions Apache Iceberg table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{namespace}.{table_name} (
        `transaction_id` STRING,
        `run_id` STRING,
        `time_sec` DOUBLE,
        `amount` DOUBLE,
        `amount_segment` STRING,
        `v1` DOUBLE,
        `v2` DOUBLE,
        `v3` DOUBLE,
        `v4` DOUBLE,
        `v10` DOUBLE,
        `v12` DOUBLE,
        `v14` DOUBLE,
        `v17` DOUBLE,
        `actual_label` INT,
        `predicted_label` INT,
        `fraud_probability` DOUBLE,
        `threshold_used` DOUBLE,
        `producer_ts_ms` BIGINT,
        `scoring_ts_ms` BIGINT,
        `latency_ms` BIGINT
    ) PARTITIONED BY (`run_id`) WITH (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    """


def build_malformed_table_ddl(catalog_name: str, namespace: str) -> str:
    """Return Flink SQL DDL for capturing malformed events in a dead-letter Iceberg table."""
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{namespace}.malformed_events (
        `raw_payload` STRING,
        `error_reason` STRING,
        `scoring_ts_ms` BIGINT
    ) WITH (
        'format-version' = '2',
        'write.format.default' = 'parquet'
    )
    """


def run_flink_streaming_job(
    model_artifact_path: Path,
    threshold: float,
    kafka_bootstrap_servers: str,
    kafka_topic: str,
    kafka_consumer_group: str,
    iceberg_rest_uri: str,
    iceberg_warehouse: str,
    iceberg_namespace: str,
    iceberg_table: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
    s3_region: str,
    checkpoint_interval_ms: int = 5000,
) -> None:
    """Submit and execute the PyFlink streaming job."""
    from pyflink.common import Row
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import DataTypes, EnvironmentSettings, StreamTableEnvironment
    from pyflink.table.udf import udf

    artifact = LinearFraudModelArtifact.from_json_file(model_artifact_path)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "2")))
    env.enable_checkpointing(checkpoint_interval_ms)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().set("execution.checkpointing.interval", f"{checkpoint_interval_ms} ms")
    t_env.get_config().set("table.exec.sink.not-null-enforcer", "drop")

    # Bundle source files so Flink Python worker processes can import src.*
    t_env.add_python_file(str(PROJECT_ROOT / "src"))

    result_row_type = DataTypes.ROW(
        [
            DataTypes.FIELD("transaction_id", DataTypes.STRING()),
            DataTypes.FIELD("run_id", DataTypes.STRING()),
            DataTypes.FIELD("time_sec", DataTypes.DOUBLE()),
            DataTypes.FIELD("amount", DataTypes.DOUBLE()),
            DataTypes.FIELD("amount_segment", DataTypes.STRING()),
            DataTypes.FIELD("v1", DataTypes.DOUBLE()),
            DataTypes.FIELD("v2", DataTypes.DOUBLE()),
            DataTypes.FIELD("v3", DataTypes.DOUBLE()),
            DataTypes.FIELD("v4", DataTypes.DOUBLE()),
            DataTypes.FIELD("v10", DataTypes.DOUBLE()),
            DataTypes.FIELD("v12", DataTypes.DOUBLE()),
            DataTypes.FIELD("v14", DataTypes.DOUBLE()),
            DataTypes.FIELD("v17", DataTypes.DOUBLE()),
            DataTypes.FIELD("actual_label", DataTypes.INT()),
            DataTypes.FIELD("predicted_label", DataTypes.INT()),
            DataTypes.FIELD("fraud_probability", DataTypes.DOUBLE()),
            DataTypes.FIELD("threshold_used", DataTypes.DOUBLE()),
            DataTypes.FIELD("producer_ts_ms", DataTypes.BIGINT()),
            DataTypes.FIELD("scoring_ts_ms", DataTypes.BIGINT()),
            DataTypes.FIELD("latency_ms", DataTypes.BIGINT()),
            DataTypes.FIELD("is_valid", DataTypes.BOOLEAN()),
            DataTypes.FIELD("error_reason", DataTypes.STRING()),
        ]
    )

    @udf(result_type=result_row_type)
    def score_transaction_udf(raw_payload: str) -> Row:
        rec = artifact.score_raw_payload(raw_payload, threshold_override=threshold)
        return Row(
            rec.transaction_id,
            rec.run_id,
            rec.time_sec,
            rec.amount,
            rec.amount_segment,
            rec.v1,
            rec.v2,
            rec.v3,
            rec.v4,
            rec.v10,
            rec.v12,
            rec.v14,
            rec.v17,
            rec.actual_label,
            rec.predicted_label,
            rec.fraud_probability,
            rec.threshold_used,
            rec.producer_ts_ms,
            rec.scoring_ts_ms,
            rec.latency_ms,
            rec.is_valid,
            rec.error_reason,
        )

    t_env.create_temporary_system_function("score_transaction_udf", score_transaction_udf)

    # 1. Register Kafka source table
    t_env.execute_sql(
        build_kafka_source_ddl(
            bootstrap_servers=kafka_bootstrap_servers,
            topic=kafka_topic,
            consumer_group=kafka_consumer_group,
        )
    )

    # 2. Register Iceberg REST catalog & tables
    catalog_name = "iceberg_catalog"
    t_env.execute_sql(
        build_iceberg_catalog_ddl(
            catalog_name=catalog_name,
            rest_uri=iceberg_rest_uri,
            warehouse=iceberg_warehouse,
            s3_endpoint=s3_endpoint,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_region=s3_region,
        )
    )
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{iceberg_namespace}")
    t_env.execute_sql(build_scored_table_ddl(catalog_name, iceberg_namespace, iceberg_table))
    t_env.execute_sql(build_malformed_table_ddl(catalog_name, iceberg_namespace))

    # 3. Create intermediate view applying the Flink scoring UDF
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW scored_stream AS
        SELECT
            raw_payload,
            score_transaction_udf(raw_payload) AS scored
        FROM kafka_raw_transactions
        """
    )

    # 4. Route valid transactions to scored_transactions and invalid to malformed_events
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(
        f"""
        INSERT INTO {catalog_name}.{iceberg_namespace}.{iceberg_table}
        SELECT
            scored.transaction_id,
            scored.run_id,
            scored.time_sec,
            scored.amount,
            scored.amount_segment,
            scored.v1,
            scored.v2,
            scored.v3,
            scored.v4,
            scored.v10,
            scored.v12,
            scored.v14,
            scored.v17,
            scored.actual_label,
            scored.predicted_label,
            scored.fraud_probability,
            scored.threshold_used,
            scored.producer_ts_ms,
            scored.scoring_ts_ms,
            scored.latency_ms
        FROM scored_stream
        WHERE scored.is_valid = TRUE
        """
    )
    stmt_set.add_insert_sql(
        f"""
        INSERT INTO {catalog_name}.{iceberg_namespace}.malformed_events
        SELECT
            raw_payload,
            scored.error_reason,
            scored.scoring_ts_ms
        FROM scored_stream
        WHERE scored.is_valid = FALSE
        """
    )

    table_result = stmt_set.execute()
    print("[OK] Submitted Flink streaming job to cluster; awaiting termination...")
    table_result.wait()


def main() -> None:
    cfg = get_config()
    parser = argparse.ArgumentParser(description="Run Flink real-time fraud scoring job.")
    parser.add_argument("--model-artifact", type=Path, default=cfg.model_artifact_path)
    parser.add_argument("--threshold", type=float, default=cfg.fraud_threshold)
    parser.add_argument("--bootstrap-servers", type=str, default=cfg.kafka_bootstrap_servers)
    parser.add_argument("--topic", type=str, default=cfg.kafka_topic)
    parser.add_argument("--consumer-group", type=str, default=cfg.kafka_consumer_group)
    parser.add_argument("--iceberg-rest-uri", type=str, default=cfg.iceberg_rest_uri)
    parser.add_argument("--iceberg-warehouse", type=str, default=cfg.iceberg_warehouse)
    parser.add_argument("--iceberg-namespace", type=str, default=cfg.iceberg_namespace)
    parser.add_argument("--iceberg-table", type=str, default=cfg.iceberg_table)
    parser.add_argument("--s3-endpoint", type=str, default=cfg.s3_endpoint)
    args = parser.parse_args()

    run_flink_streaming_job(
        model_artifact_path=args.model_artifact,
        threshold=args.threshold,
        kafka_bootstrap_servers=args.bootstrap_servers,
        kafka_topic=args.topic,
        kafka_consumer_group=args.consumer_group,
        iceberg_rest_uri=args.iceberg_rest_uri,
        iceberg_warehouse=args.iceberg_warehouse,
        iceberg_namespace=args.iceberg_namespace,
        iceberg_table=args.iceberg_table,
        s3_endpoint=args.s3_endpoint,
        s3_access_key=cfg.s3_access_key,
        s3_secret_key=cfg.s3_secret_key,
        s3_region=cfg.s3_region,
    )


if __name__ == "__main__":
    main()
