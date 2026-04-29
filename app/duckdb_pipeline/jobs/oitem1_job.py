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


RAW_OITEM1_HEADER = [
    'CUSTOMER #',
    'INVOICE #',
    'JULIAN DATE',
    'INVOICE DATE',
    'ORIGINAL A/R AMOUNT',
    'DATE + REFERENCE #1',
    'APPLIED AMOUNT #1',
    'DATE + REFERENCE #2',
    'APPLIED AMOUNT #2',
    'DATE + REFERENCE #3',
    'APPLIED AMOUNT #3',
    'A/R BALANCE',
    'ORDER #',
    'STORE NUMBER',
    'LINE NUMBER',
    'LAST PAYMENT AMOUNT',
    'REVERSE A/R BALANCE',
    'PURCHASE ORDER NUMBER',
    'TERMS DESCRIPTION',
    'BANK CODE',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'AMEX FEE',
    'Unassigned (numeric)',
    'Unassigned (numeric)',
]

OITEM1_COLUMNS = [
    'CUSTOMER #',
    'INVOICE #',
    'JULIAN DATE',
    'INVOICE DATE',
    'ORIGINAL A/R AMOUNT',
    'DATE + REFERENCE #1',
    'APPLIED AMOUNT #1',
    'DATE + REFERENCE #2',
    'APPLIED AMOUNT #2',
    'DATE + REFERENCE #3',
    'APPLIED AMOUNT #3',
    'A/R BALANCE',
    'ORDER #',
    'STORE NUMBER',
    'LINE NUMBER',
    'LAST PAYMENT AMOUNT',
    'REVERSE A/R BALANCE',
    'PURCHASE ORDER NUMBER',
    'TERMS DESCRIPTION',
    'BANK CODE',
    'UNASSIGNED_21',
    'UNASSIGNED_22',
    'UNASSIGNED_23',
    'AMEX FEE',
    'UNASSIGNED_25',
    'UNASSIGNED_26',
]

REQUIRED_OITEM1_COLUMNS = {'CUSTOMER #', 'INVOICE #', 'LINE NUMBER'}


def validate_header_row(file_path: str, delim: str = "\t") -> None:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File is empty: {file_path}")

    actual_header = [part.strip() for part in first_line.split(delim)]

    if actual_header != RAW_OITEM1_HEADER:
        raise ValueError(
            "OITEM1 header does not match expected layout. "
            f"Expected {len(RAW_OITEM1_HEADER)} columns, got {len(actual_header)}."
        )


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(OITEM1_COLUMNS):
        raise ValueError(
            f"OITEM1 column count mismatch: expected {len(OITEM1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_OITEM1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"OITEM1 missing required columns: {sorted(missing_required)}")


def run_oitem1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "|",
    postgres_table: str = "oitem1",
    if_exists: str = "replace",
) -> None:
    print("[oitem1] Validating header row...", flush=True)
    validate_header_row(file_path=file_path, delim=delim)

    print("[oitem1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="oitem1_raw",
        delim=delim,
        header=False,
        skip_rows=1,
        replace=True,
    )

    print("[oitem1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="oitem1_raw",
        expected_column_count=len(OITEM1_COLUMNS),
    )

    print("[oitem1] Renaming columns...", flush=True)
    rename_columns(
        table_name="oitem1_raw",
        new_column_names=OITEM1_COLUMNS,
    )

    print("[oitem1] Validating columns...", flush=True)
    validate_expected_columns("oitem1_raw")

    print("[oitem1] Transforming in DuckDB...", flush=True)
    trim_all_text_columns(
        source_table="oitem1_raw",
        target_table="oitem1_final",
    )

    print("[oitem1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="oitem1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[oitem1] Finished successfully.", flush=True)
