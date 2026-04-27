from __future__ import annotations

import os
from pathlib import Path


DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", str(DATA_DIR / "staging.duckdb"))).resolve()

# TEST OR REAL - TOGGLE
# MAKE SURE TO REBOOT DOCKER COMPOSE AND DOCKER DOWN -V

POSTGRES_URL = os.getenv("DATABASE_URL", "")
# POSTGRES_URL = os.getenv("TEST_DATABASE_URL", "")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
