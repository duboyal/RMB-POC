from __future__ import annotations

from pathlib import Path

from app.duckdb_pipeline.config import POSTGRES_URL
from app.duckdb_pipeline.export import export_duckdb_table_to_postgres
from app.duckdb_pipeline.staging import (
    drop_extra_trailing_column_if_present,
    get_table_columns,
    rename_columns,
    stage_delimited_file,
)
from app.duckdb_pipeline.transforms import trim_all_text_columns


RAW_SHIPT1_HEADER = [
    'CUSTOMER #',
    'SHIP-TO #',
    'SHIP-TO NAME',
    'SHIP-TO ADDRESS 1',
    'SHIP-TO ADDRESS 2',
    'CITY',
    'STATE',
    'ZIP CODE',
    'TAX CODE',
    'STORE NUMBER',
]

SHIPT1_COLUMNS = [
    'CUSTOMER #',
    'SHIP-TO #',
    'SHIP-TO NAME',
    'SHIP-TO ADDRESS 1',
    'SHIP-TO ADDRESS 2',
    'CITY',
    'STATE',
    'ZIP CODE',
    'TAX CODE',
    'STORE NUMBER',
]

REQUIRED_SHIPT1_COLUMNS = {'CUSTOMER #', 'SHIP-TO #'}


def validate_header_row(file_path: str, delim: str = "|") -> None:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File is empty: {file_path}")

    actual_header = [part.strip() for part in first_line.split(delim)]

    if actual_header != RAW_SHIPT1_HEADER:
        raise ValueError(
            "SHIPT1 header does not match expected layout. "
            f"Expected {len(RAW_SHIPT1_HEADER)} columns, got {len(actual_header)}."
        )


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(SHIPT1_COLUMNS):
        raise ValueError(
            f"SHIPT1 column count mismatch: expected {len(SHIPT1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_SHIPT1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"SHIPT1 missing required columns: {sorted(missing_required)}")


def run_shipt1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "\t",
    postgres_table: str = "shipt1",
    if_exists: str = "replace",
) -> None:
    print("[shipt1] Validating header row...", flush=True)
    validate_header_row(file_path=file_path, delim=delim)

    print("[shipt1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="shipt1_raw",
        delim=delim,
        header=False,
        skip_rows=1,
        replace=True,
    )

    print("[shipt1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="shipt1_raw",
        expected_column_count=len(SHIPT1_COLUMNS),
    )

    print("[shipt1] Renaming columns...", flush=True)
    rename_columns(
        table_name="shipt1_raw",
        new_column_names=SHIPT1_COLUMNS,
    )

    print("[shipt1] Validating columns...", flush=True)
    validate_expected_columns("shipt1_raw")

    print("[shipt1] Transforming in DuckDB...", flush=True)
    trim_all_text_columns(
        source_table="shipt1_raw",
        target_table="shipt1_final",
    )

    print("[shipt1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="shipt1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[shipt1] Finished successfully.", flush=True)
