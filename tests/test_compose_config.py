"""Unit tests validating Docker Compose v2 configuration, service names, and Flink DDLs."""

from __future__ import annotations

import yaml

from src.config import PROJECT_ROOT
from src.flink_job import (
    build_iceberg_catalog_ddl,
    build_kafka_source_ddl,
    build_scored_table_ddl,
)


def test_docker_compose_structure_and_pinned_images() -> None:
    compose_path = PROJECT_ROOT / "docker-compose.yml"
    assert compose_path.exists()

    with compose_path.open("r", encoding="utf-8") as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    required_services = {
        "kafka",
        "minio",
        "minio-init",
        "iceberg-rest",
        "jobmanager",
        "taskmanager",
        "pipeline-runner",
    }
    assert required_services.issubset(set(services.keys()))

    # Verify infrastructure images are pinned (not :latest)
    for svc_name in ["kafka", "minio", "minio-init", "iceberg-rest", "taskmanager"]:
        img = services[svc_name].get("image", "")
        assert ":" in img and not img.endswith(":latest"), f"Service {svc_name} image must be pinned"

    # Verify container-to-container communication uses Docker service names, not localhost
    jm_env = services["jobmanager"]["environment"]
    assert jm_env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:9092"
    assert jm_env["ICEBERG_REST_URI"] == "http://iceberg-rest:8181"
    assert jm_env["S3_ENDPOINT"] == "http://minio:9000"


def test_flink_sql_ddl_generation() -> None:
    kafka_ddl = build_kafka_source_ddl("kafka:9092", "transactions.raw.v1", "flink-fraud-scorer")
    assert "'connector' = 'kafka'" in kafka_ddl
    assert "'properties.bootstrap.servers' = 'kafka:9092'" in kafka_ddl

    iceberg_ddl = build_iceberg_catalog_ddl(
        "iceberg_catalog",
        "http://iceberg-rest:8181",
        "s3://warehouse/",
        "http://minio:9000",
        "admin",
        "password123",
        "us-east-1",
    )
    assert "'type' = 'iceberg'" in iceberg_ddl
    assert "'catalog-type' = 'rest'" in iceberg_ddl
    assert "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'" in iceberg_ddl

    table_ddl = build_scored_table_ddl("iceberg_catalog", "fraud_detection", "scored_transactions")
    assert "PARTITIONED BY (`run_id`)" in table_ddl
    assert "`latency_ms` BIGINT" in table_ddl
