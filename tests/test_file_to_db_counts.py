from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from app.duckdb_pipeline.config import POSTGRES_URL

load_dotenv()


TABLE_FILE_PAIRS = [
    ("cust1", "data/processed/cust1.txt"),
    ("detal1", "data/processed/detal1.txt"),
]


def count_file_rows(file_path: str) -> int:
    path = Path(file_path)

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        return max(sum(1 for _ in f) - 1, 0)


def count_db_rows(table_name: str) -> int:
    conn = psycopg2.connect(POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            return cur.fetchone()[0]
    finally:
        conn.close()


def test_file_row_counts_match_database():
    for table_name, file_path in TABLE_FILE_PAIRS:
        file_count = count_file_rows(file_path)
        db_count = count_db_rows(table_name)

        assert db_count == file_count, (
            f"{table_name}: database has {db_count} rows, "
            f"but file has {file_count} rows"
        )