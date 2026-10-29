"""Verification tests for required deliverables, PDF report page bounds, and notebook execution."""

from __future__ import annotations

import json
import re
import zipfile
from pathlib import Path

from src.config import PROJECT_ROOT


def count_pdf_pages(pdf_path: Path) -> int:
    """Count pages in a PDF file using standard PDF /Type /Page object markers (zero external deps)."""
    raw_bytes = pdf_path.read_bytes()
    return len(re.findall(rb"/Type\s*/Page\b", raw_bytes))


def test_required_deliverables_and_pdf_page_count() -> None:
    pdf_path = PROJECT_ROOT / "report" / "technical_report.pdf"
    assert pdf_path.exists(), "report/technical_report.pdf must exist"
    num_pages = count_pdf_pages(pdf_path)
    assert 3 <= num_pages <= 5, f"PDF report must be 3-5 pages, got {num_pages}"

    nb_path = PROJECT_ROOT / "notebooks" / "results.ipynb"
    assert nb_path.exists(), "notebooks/results.ipynb must exist"
    with nb_path.open("r", encoding="utf-8") as f:
        nb_json = json.load(f)
    code_cells = [c for c in nb_json.get("cells", []) if c.get("cell_type") == "code"]
    assert len(code_cells) >= 4
    assert all(c.get("execution_count") is not None for c in code_cells), (
        "All code cells in notebooks/results.ipynb must be executed"
    )

    assert (PROJECT_ROOT / "README.md").exists()
    assert (PROJECT_ROOT / "MODEL_CARD.md").exists()
    assert (PROJECT_ROOT / ".env.example").exists()


def test_submission_zip_contents_if_built() -> None:
    zip_path = PROJECT_ROOT / "PES1UG24AM145.zip"
    if not zip_path.exists():
        return
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())
        assert "README.md" in names
        assert "MODEL_CARD.md" in names
        assert "docker-compose.yml" in names
        assert "models/fraud_model_artifact.json" in names
        assert "notebooks/results.ipynb" in names
        assert "report/technical_report.pdf" in names
        assert "results/evaluation_metrics.json" in names
        assert "results/segment_error_analysis.csv" in names
        assert not any("creditcard.csv" in n for n in names), (
            "Large raw dataset must be excluded from PES1UG24AM145.zip"
        )
