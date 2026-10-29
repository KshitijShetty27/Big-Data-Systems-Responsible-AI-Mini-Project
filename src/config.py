"""Centralized, cross-platform configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=False)

FEATURE_COLUMNS: list[str] = [
    "Time",
    *[f"V{i}" for i in range(1, 29)],
    "Amount",
]
TARGET_COLUMN: str = "Class"


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for dataset paths, model parameters, Kafka, Flink, and Iceberg."""

    project_root: Path = PROJECT_ROOT
    raw_csv_path: Path = PROJECT_ROOT / os.getenv("RAW_CSV_PATH", "data/raw/creditcard.csv")
    heldout_csv_path: Path = PROJECT_ROOT / os.getenv(
        "HELDOUT_CSV_PATH", "data/processed/heldout_transactions.csv"
    )
    model_joblib_path: Path = PROJECT_ROOT / os.getenv(
        "MODEL_JOBLIB_PATH", "models/fraud_model.joblib"
    )
    model_artifact_path: Path = PROJECT_ROOT / os.getenv(
        "MODEL_ARTIFACT_PATH", "models/fraud_model_artifact.json"
    )
    results_dir: Path = PROJECT_ROOT / os.getenv("RESULTS_DIR", "results")

    random_seed: int = int(os.getenv("RANDOM_SEED", "42"))
    test_size: float = float(os.getenv("TEST_SIZE", "0.20"))
    fraud_threshold: float = float(os.getenv("FRAUD_THRESHOLD", "0.50"))

    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "transactions.raw.v1")
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "transactions.dlq.v1")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "flink-fraud-scorer")

    iceberg_rest_uri: str = os.getenv("ICEBERG_REST_URI", "http://localhost:8181")
    iceberg_warehouse: str = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
    iceberg_namespace: str = os.getenv("ICEBERG_NAMESPACE", "fraud_detection")
    iceberg_table: str = os.getenv("ICEBERG_TABLE", "scored_transactions")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    s3_access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
    s3_region: str = os.getenv("AWS_REGION", "us-east-1")


def get_config() -> PipelineConfig:
    """Return an initialized PipelineConfig instance."""
    return PipelineConfig()
