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


RAW_PCODE1_HEADER = [
    'PRODUCT CATEGORY CODE',
    'DESCRIPTION',
    'Unassigned',
    'Unassigned',
    'SALES $ MOVEMENT 1',
    'SALES $ MOVEMENT 2',
    'SALES $ MOVEMENT 3',
    'SALES $ MOVEMENT 4',
    'SALES $ MOVEMENT 5',
    'SALES $ MOVEMENT 6',
    'SALES $ MOVEMENT 7',
    'SALES $ MOVEMENT 8',
    'SALES $ MOVEMENT 9',
    'SALES $ MOVEMENT 10',
    'SALES $ MOVEMENT 11',
    'SALES $ MOVEMENT 12',
    'CURRENT MTD SALES $',
    'LAST YEAR YTD SALES $',
    'LBS. MOVEMENT 1',
    'LBS. MOVEMENT 2',
    'LBS. MOVEMENT 3',
    'LBS. MOVEMENT 4',
    'LBS. MOVEMENT 5',
    'LBS. MOVEMENT 6',
    'LBS. MOVEMENT 7',
    'LBS. MOVEMENT 8',
    'LBS. MOVEMENT 9',
    'LBS. MOVEMENT 10',
    'LBS. MOVEMENT 11',
    'LBS. MOVEMENT 12',
    'CURRENT MTD LBS.',
    'LAST YEAR YTD LBS.',
    'COST MOVEMENT 1',
    'COST MOVEMENT 2',
    'COST MOVEMENT 3',
    'COST MOVEMENT 4',
    'COST MOVEMENT 5',
    'COST MOVEMENT 6',
    'COST MOVEMENT 7',
    'COST MOVEMENT 8',
    'COST MOVEMENT 9',
    'COST MOVEMENT 10',
    'COST MOVEMENT 11',
    'COST MOVEMENT 12',
    'CURRENT MTD COST',
    'LAST YEAR YTD COST',
    'PIECES MOVEMENT 1',
    'PIECES MOVEMENT 2',
    'PIECES MOVEMENT 3',
    'PIECES MOVEMENT 4',
    'PIECES MOVEMENT 5',
    'PIECES MOVEMENT 6',
    'PIECES MOVEMENT 7',
    'PIECES MOVEMENT 8',
    'PIECES MOVEMENT 9',
    'PIECES MOVEMENT 10',
    'PIECES MOVEMENT 11',
    'PIECES MOVEMENT 12',
    'CURRENT MTD PIECES',
    'LAST YEAR YTD PIECES',
]

PCODE1_COLUMNS = [
    'PRODUCT CATEGORY CODE',
    'DESCRIPTION',
    'UNASSIGNED_3',
    'UNASSIGNED_4',
    'SALES $ MOVEMENT 1',
    'SALES $ MOVEMENT 2',
    'SALES $ MOVEMENT 3',
    'SALES $ MOVEMENT 4',
    'SALES $ MOVEMENT 5',
    'SALES $ MOVEMENT 6',
    'SALES $ MOVEMENT 7',
    'SALES $ MOVEMENT 8',
    'SALES $ MOVEMENT 9',
    'SALES $ MOVEMENT 10',
    'SALES $ MOVEMENT 11',
    'SALES $ MOVEMENT 12',
    'CURRENT MTD SALES $',
    'LAST YEAR YTD SALES $',
    'LBS. MOVEMENT 1',
    'LBS. MOVEMENT 2',
    'LBS. MOVEMENT 3',
    'LBS. MOVEMENT 4',
    'LBS. MOVEMENT 5',
    'LBS. MOVEMENT 6',
    'LBS. MOVEMENT 7',
    'LBS. MOVEMENT 8',
    'LBS. MOVEMENT 9',
    'LBS. MOVEMENT 10',
    'LBS. MOVEMENT 11',
    'LBS. MOVEMENT 12',
    'CURRENT MTD LBS.',
    'LAST YEAR YTD LBS.',
    'COST MOVEMENT 1',
    'COST MOVEMENT 2',
    'COST MOVEMENT 3',
    'COST MOVEMENT 4',
    'COST MOVEMENT 5',
    'COST MOVEMENT 6',
    'COST MOVEMENT 7',
    'COST MOVEMENT 8',
    'COST MOVEMENT 9',
    'COST MOVEMENT 10',
    'COST MOVEMENT 11',
    'COST MOVEMENT 12',
    'CURRENT MTD COST',
    'LAST YEAR YTD COST',
    'PIECES MOVEMENT 1',
    'PIECES MOVEMENT 2',
    'PIECES MOVEMENT 3',
    'PIECES MOVEMENT 4',
    'PIECES MOVEMENT 5',
    'PIECES MOVEMENT 6',
    'PIECES MOVEMENT 7',
    'PIECES MOVEMENT 8',
    'PIECES MOVEMENT 9',
    'PIECES MOVEMENT 10',
    'PIECES MOVEMENT 11',
    'PIECES MOVEMENT 12',
    'CURRENT MTD PIECES',
    'LAST YEAR YTD PIECES',
]

REQUIRED_PCODE1_COLUMNS = {'PRODUCT CATEGORY CODE'}


def validate_header_row(file_path: str, delim: str = "\t") -> None:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File is empty: {file_path}")

    actual_header = [part.strip() for part in first_line.split(delim)]

    if actual_header != RAW_PCODE1_HEADER:
        raise ValueError(
            "PCODE1 header does not match expected layout. "
            f"Expected {len(RAW_PCODE1_HEADER)} columns, got {len(actual_header)}."
        )


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(PCODE1_COLUMNS):
        raise ValueError(
            f"PCODE1 column count mismatch: expected {len(PCODE1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_PCODE1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"PCODE1 missing required columns: {sorted(missing_required)}")


def run_pcode1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "\t",
    postgres_table: str = "pcode1",
    if_exists: str = "replace",
) -> None:
    print("[pcode1] Validating header row...", flush=True)
    validate_header_row(file_path=file_path, delim=delim)

    print("[pcode1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="pcode1_raw",
        delim=delim,
        header=False,
        skip_rows=1,
        replace=True,
    )

    print("[pcode1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="pcode1_raw",
        expected_column_count=len(PCODE1_COLUMNS),
    )

    print("[pcode1] Renaming columns...", flush=True)
    rename_columns(
        table_name="pcode1_raw",
        new_column_names=PCODE1_COLUMNS,
    )

    print("[pcode1] Validating columns...", flush=True)
    validate_expected_columns("pcode1_raw")

    print("[pcode1] Transforming in DuckDB...", flush=True)
    trim_all_text_columns(
        source_table="pcode1_raw",
        target_table="pcode1_final",
    )

    print("[pcode1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="pcode1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[pcode1] Finished successfully.", flush=True)
