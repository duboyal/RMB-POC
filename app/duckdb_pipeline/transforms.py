from __future__ import annotations

from app.duckdb_pipeline.config import DUCKDB_PATH
from app.duckdb_pipeline.staging import get_connection


def trim_all_text_columns(
    source_table: str,
    target_table: str,
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    """
    Create a cleaned copy of a table where every column is trimmed as text.
    This is a safe first-pass cleanup for raw legacy imports.
    """
    con = get_connection(duckdb_path)
    try:
        columns = con.execute(f'DESCRIBE "{source_table}"').fetchall()
        column_names = [row[0] for row in columns]

        select_parts = [
            f'TRIM(CAST("{col}" AS VARCHAR)) AS "{col}"' for col in column_names
        ]
        select_sql = ",\n                ".join(select_parts)

        con.execute(
            f"""
            CREATE OR REPLACE TABLE "{target_table}" AS
            SELECT
                {select_sql}
            FROM "{source_table}"
            """
        )
    finally:
        con.close()


def split_valid_and_rejects(
    source_table: str,
    valid_table: str,
    reject_table: str,
    key_column: str,
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    """
    Split rows into:
    - valid_table: rows with a non-empty business key
    - reject_table: rows missing that key
    """
    con = get_connection(duckdb_path)
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE "{valid_table}" AS
            SELECT *
            FROM "{source_table}"
            WHERE "{key_column}" IS NOT NULL
              AND TRIM(CAST("{key_column}" AS VARCHAR)) <> ''
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE TABLE "{reject_table}" AS
            SELECT *
            FROM "{source_table}"
            WHERE "{key_column}" IS NULL
               OR TRIM(CAST("{key_column}" AS VARCHAR)) = ''
            """
        )
    finally:
        con.close()


def deduplicate_by_key(
    source_table: str,
    target_table: str,
    key_columns: list[str],
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    """
    Keep one row per business key.
    For now, if duplicates exist, the first encountered row is kept.
    """
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    partition_by = ", ".join([f'"{col}"' for col in key_columns])

    con = get_connection(duckdb_path)
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE "{target_table}" AS
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY {partition_by}
                           ORDER BY {partition_by}
                       ) AS rn
                FROM "{source_table}"
            )
            WHERE rn = 1
            """
        )
    finally:
        con.close()


def assert_required_columns(
    table_name: str,
    required_columns: list[str],
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    """
    Raise an error if any required columns are missing from a DuckDB table.
    """
    con = get_connection(duckdb_path)
    try:
        rows = con.execute(f'DESCRIBE "{table_name}"').fetchall()
        existing_columns = {row[0] for row in rows}
    finally:
        con.close()

    missing = [col for col in required_columns if col not in existing_columns]
    if missing:
        raise ValueError(f'Table "{table_name}" is missing required columns: {missing}')


def cust1_transform(
    raw_table: str = "cust1_raw",
    trimmed_table: str = "cust1_trimmed",
    valid_table: str = "cust1_valid",
    reject_table: str = "cust1_rejects",
    final_table: str = "cust1_final",
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    """
    First-pass CUST1 transform.

    Flow:
    1. confirm required columns exist
    2. trim all columns as text
    3. split into valid / rejects using CUSTOMER NUMBER
    4. deduplicate valid rows by CUSTOMER NUMBER
    """
    assert_required_columns(
        table_name=raw_table,
        required_columns=["CUSTOMER NUMBER", "CUSTOMER NAME"],
        duckdb_path=duckdb_path,
    )

    trim_all_text_columns(
        source_table=raw_table,
        target_table=trimmed_table,
        duckdb_path=duckdb_path,
    )

    split_valid_and_rejects(
        source_table=trimmed_table,
        valid_table=valid_table,
        reject_table=reject_table,
        key_column="CUSTOMER NUMBER",
        duckdb_path=duckdb_path,
    )

    deduplicate_by_key(
        source_table=valid_table,
        target_table=final_table,
        key_columns=["CUSTOMER NUMBER"],
        duckdb_path=duckdb_path,
    )
