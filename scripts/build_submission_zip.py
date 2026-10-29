"""Build and verify the final submission archive PES1UG24AM145.zip."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ZIP_NAME = "PES1UG24AM145.zip"

INCLUDE_PATHS = [
    ".env.example",
    ".gitignore",
    "README.md",
    "MODEL_CARD.md",
    "RUN_COMMANDS.md",
    "RUN_COMMANDS.pdf",
    "PRESENTATION_SCRIPT.md",
    "PRESENTATION_SCRIPT.pdf",
    "DEMO_GUIDE.pdf",
    "docker-compose.yml",
    "requirements.txt",
    "data/README.md",
    "data/fixtures/heldout_sample_300.csv",
    "docker/flink/Dockerfile",
    "docker/app/Dockerfile",
    "src/__init__.py",
    "src/config.py",
    "src/schema.py",
    "src/scoring.py",
    "src/model_training.py",
    "src/producer.py",
    "src/flink_job.py",
    "src/iceberg_utils.py",
    "src/evaluation.py",
    "scripts/download_dataset.py",
    "scripts/train_model.py",
    "scripts/run_evaluation.py",
    "scripts/run_load_tests.py",
    "scripts/verify_iceberg.py",
    "scripts/smoke_test.py",
    "scripts/presentation_ui.py",
    "scripts/build_notebook.py",
    "scripts/generate_report.py",
    "scripts/build_submission_zip.py",
    "tests/__init__.py",
    "tests/test_schema.py",
    "tests/test_scoring.py",
    "tests/test_evaluation.py",
    "tests/test_compose_config.py",
    "tests/test_iceberg_smoke.py",
    "tests/test_deliverables.py",
    "models/fraud_model.joblib",
    "models/fraud_model_artifact.json",
    "notebooks/results.ipynb",
    "report/technical_report.md",
    "report/technical_report.pdf",
    "results/evaluation_metrics.json",
    "results/segment_error_analysis.csv",
    "results/segment_error_analysis.json",
    "results/load_test_runs.csv",
    "results/load_test_runs.json",
    "results/run_metadata.json",
    "results/smoke_test_verification.json",
    "results/figures/confusion_matrix_and_threshold.png",
    "results/figures/segment_error_analysis.png",
    "results/figures/throughput_and_latency.png",
]

FORBIDDEN_PATTERNS = [
    "creditcard.csv",
    "heldout_transactions.csv",
    "__pycache__",
    ".pytest_cache",
    ".venv",
    "venv",
    "warehouse_local",
]


def build_submission_zip() -> Path:
    zip_path = PROJECT_ROOT / ZIP_NAME
    missing = [rel for rel in INCLUDE_PATHS if not (PROJECT_ROOT / rel).exists()]
    if missing:
        raise FileNotFoundError(f"Missing required deliverable files before zipping: {missing}")

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for rel in INCLUDE_PATHS:
            abs_file = PROJECT_ROOT / rel
            # Store cleanly at top-level relative path without unnecessary nested directory levels
            zf.write(abs_file, arcname=rel)

    # Inspect and validate the generated ZIP archive
    with zipfile.ZipFile(zip_path, mode="r") as zf:
        bad_file = zf.testzip()
        if bad_file is not None:
            raise RuntimeError(f"Corrupted entry inside {ZIP_NAME}: {bad_file}")
        names = zf.namelist()
        for pat in FORBIDDEN_PATTERNS:
            for name in names:
                if pat in name:
                    raise RuntimeError(f"Forbidden path '{name}' found inside {ZIP_NAME}")

        print(f"[OK] Built and verified {zip_path} ({zip_path.stat().st_size:,} bytes, {len(names)} files):")
        for info in zf.infolist():
            print(f"     {info.filename:<52} {info.file_size:>10,} bytes")

    return zip_path


if __name__ == "__main__":
    try:
        build_submission_zip()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
