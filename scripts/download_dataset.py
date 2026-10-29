"""Download the public ULB Credit Card Fraud dataset from Zenodo with checksum verification."""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import requests
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config

ZENODO_RECORD_API = "https://zenodo.org/api/records/7395559"
ZENODO_DIRECT_URL = "https://zenodo.org/api/records/7395559/files/creditcard.csv/content"
EXPECTED_MIN_BYTES = 140_000_000  # ~150.8 MB


def compute_md5(file_path: Path) -> str:
    """Compute MD5 hash of a file in chunks."""
    md5 = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            md5.update(chunk)
    return md5.hexdigest()


def download_dataset(target_path: Path, force: bool = False) -> Path:
    """Download creditcard.csv from Zenodo if not already present."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and not force and target_path.stat().st_size >= EXPECTED_MIN_BYTES:
        print(f"[OK] Dataset already exists at {target_path} ({target_path.stat().st_size:,} bytes)")
        return target_path

    expected_md5 = None
    try:
        meta_resp = requests.get(ZENODO_RECORD_API, timeout=15)
        if meta_resp.ok:
            for file_info in meta_resp.json().get("files", []):
                if file_info.get("key") == "creditcard.csv":
                    checksum = file_info.get("checksum", "")
                    if checksum.startswith("md5:"):
                        expected_md5 = checksum.split("md5:", 1)[1]
    except Exception as exc:
        print(f"[WARN] Could not fetch Zenodo metadata checksum: {exc}")

    temp_path = target_path.with_suffix(".csv.part")
    print(f"[INFO] Downloading ULB Credit Card Fraud dataset from {ZENODO_DIRECT_URL} ...")
    with requests.get(ZENODO_DIRECT_URL, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total_bytes = int(resp.headers.get("content-length", 0))
        with temp_path.open("wb") as out_file, tqdm(
            total=total_bytes, unit="B", unit_scale=True, desc="creditcard.csv"
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out_file.write(chunk)
                    pbar.update(len(chunk))

    if expected_md5:
        actual_md5 = compute_md5(temp_path)
        if actual_md5 != expected_md5:
            temp_path.unlink(missing_ok=True)
            raise RuntimeError(
                f"MD5 mismatch for downloaded creditcard.csv: expected {expected_md5}, got {actual_md5}"
            )
        print(f"[OK] Verified Zenodo MD5 checksum: {actual_md5}")

    temp_path.replace(target_path)
    print(f"[OK] Saved dataset to {target_path} ({target_path.stat().st_size:,} bytes)")
    return target_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download ULB credit card fraud dataset.")
    parser.add_argument(
        "--output",
        type=Path,
        default=get_config().raw_csv_path,
        help="Destination path for creditcard.csv (default: data/raw/creditcard.csv)",
    )
    parser.add_argument("--force", action="store_true", help="Re-download even if file exists")
    args = parser.parse_args()
    try:
        download_dataset(args.output, force=args.force)
    except Exception as exc:
        print(
            f"[ERROR] Automatic download failed: {exc}\n"
            f"Please download creditcard.csv manually from:\n"
            f"  1) https://zenodo.org/records/7395559\n"
            f"  2) https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud\n"
            f"and place it at: {args.output}",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
