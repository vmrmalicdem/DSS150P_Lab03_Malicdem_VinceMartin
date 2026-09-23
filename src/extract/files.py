import re
from pathlib import Path
import shutil

from src.config import path_for

SOURCE_FILES = ("customers.csv", "products.json", "orders.csv")


def safe_dirname(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "-", run_id)


def extract_sources(run_id: str) -> Path:
    source_dir = path_for("source_dir")
    raw_dir = path_for("raw_dir")
    safe_name = safe_dirname(run_id)
    raw_run_dir = raw_dir / f"run_id={safe_name}"
    raw_run_dir.mkdir(parents=True, exist_ok=True)

    for filename in SOURCE_FILES:
        shutil.copy2(source_dir / filename, raw_run_dir / filename)

    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "_latest_run_id.txt").write_text(safe_name, encoding="utf-8")

    return raw_run_dir
