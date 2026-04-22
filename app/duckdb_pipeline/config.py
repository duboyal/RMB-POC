from __future__ import annotations

import os
from pathlib import Path


DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(DATA_DIR / "staging.duckdb"))).resolve()

POSTGRES_URL = os.getenv("DATABASE_URL", "")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
