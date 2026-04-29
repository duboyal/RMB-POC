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


RAW_HEDER1_HEADER = [
    'ORDER #',
    'BILL TO CUSTOMER #',
    'BILL TO CUSTOMER NAME',
    'BILL TO ADDRESS 1',
    'BILL TO ADDRESS 2',
    'BILL TO CITY',
    'BILL TO STATE',
    'BILL TO ZIP CODE',
    'SHIP-TO #',
    'ORDER DATE (ENTERED)',
    'SHIP-ON DATE',
    'INVOICE DATE',
    'CHECKER #',
    'INVOICE #',
    'DRIVER #',
    'P.O. #',
    'TERMS DESCRIPTION',
    'SALESPERSON #',
    'TRUCK #',
    'SHIP-VIA #',
    'SHIP-VIA DESCRIPTION',
    'REMARKS 1',
    'REMARKS 2',
    'MERCHANDISE AMOUNT $',
    'PICK-UP ALLOWANCE',
    'TOTAL TAX',
    'MISC. CHARGE',
    'MISC. DESCRIPTION',
    'CUSTOMER CLASS #',
    'DISCOUNT AMOUNT',
    'ORDER CLERK',
    'Unassigned',
    'CREDIT STATUS',
    '# OF PIECES',
    'TOTAL GROSS WEIGHT',
    'REMARKS 3',
    'COMPANY CODE',
    'COMPANY NAME',
    'WAREHOUSE #',
    'PRICE CODE',
    'TOTAL # OF CASES',
    'OPERATOR #',
    'PRINT FLAG',
    'REMARKS 5 (CREDIT)',
    'REMARKS 4',
    'PICKER ID #',
    'TERMS CODE',
    'TAX CODE',
    'STOP #',
    'ROUTE #',
    'STORE NUMBER',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'SRP ZONE',
    'TOTAL NET LBS.',
    'TOTAL LINES ON ORDER',
]

HEDER1_COLUMNS = [
    'ORDER #',
    'BILL TO CUSTOMER #',
    'BILL TO CUSTOMER NAME',
    'BILL TO ADDRESS 1',
    'BILL TO ADDRESS 2',
    'BILL TO CITY',
    'BILL TO STATE',
    'BILL TO ZIP CODE',
    'SHIP-TO #',
    'ORDER DATE (ENTERED)',
    'SHIP-ON DATE',
    'INVOICE DATE',
    'CHECKER #',
    'INVOICE #',
    'DRIVER #',
    'P.O. #',
    'TERMS DESCRIPTION',
    'SALESPERSON #',
    'TRUCK #',
    'SHIP-VIA #',
    'SHIP-VIA DESCRIPTION',
    'REMARKS 1',
    'REMARKS 2',
    'MERCHANDISE AMOUNT $',
    'PICK-UP ALLOWANCE',
    'TOTAL TAX',
    'MISC. CHARGE',
    'MISC. DESCRIPTION',
    'CUSTOMER CLASS #',
    'DISCOUNT AMOUNT',
    'ORDER CLERK',
    'UNASSIGNED_32',
    'CREDIT STATUS',
    '# OF PIECES',
    'TOTAL GROSS WEIGHT',
    'REMARKS 3',
    'COMPANY CODE',
    'COMPANY NAME',
    'WAREHOUSE #',
    'PRICE CODE',
    'TOTAL # OF CASES',
    'OPERATOR #',
    'PRINT FLAG',
    'REMARKS 5 (CREDIT)',
    'REMARKS 4',
    'PICKER ID #',
    'TERMS CODE',
    'TAX CODE',
    'STOP #',
    'ROUTE #',
    'STORE NUMBER',
    'UNASSIGNED_52',
    'UNASSIGNED_53',
    'UNASSIGNED_54',
    'SRP ZONE',
    'TOTAL NET LBS.',
    'TOTAL LINES ON ORDER',
]

REQUIRED_HEDER1_COLUMNS = {'ORDER #'}


def validate_header_row(file_path: str, delim: str = "\t") -> None:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File is empty: {file_path}")

    actual_header = [part.strip() for part in first_line.split(delim)]

    if actual_header != RAW_HEDER1_HEADER:
        raise ValueError(
            "HEDER1 header does not match expected layout. "
            f"Expected {len(RAW_HEDER1_HEADER)} columns, got {len(actual_header)}."
        )


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(HEDER1_COLUMNS):
        raise ValueError(
            f"HEDER1 column count mismatch: expected {len(HEDER1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_HEDER1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"HEDER1 missing required columns: {sorted(missing_required)}")


def run_heder1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "\t",
    postgres_table: str = "heder1",
    if_exists: str = "replace",
) -> None:
    print("[heder1] Validating header row...", flush=True)
    validate_header_row(file_path=file_path, delim=delim)

    print("[heder1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="heder1_raw",
        delim=delim,
        header=False,
        skip_rows=1,
        replace=True,
    )

    print("[heder1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="heder1_raw",
        expected_column_count=len(HEDER1_COLUMNS),
    )

    print("[heder1] Renaming columns...", flush=True)
    rename_columns(
        table_name="heder1_raw",
        new_column_names=HEDER1_COLUMNS,
    )

    print("[heder1] Validating columns...", flush=True)
    validate_expected_columns("heder1_raw")

    print("[heder1] Transforming in DuckDB...", flush=True)
    trim_all_text_columns(
        source_table="heder1_raw",
        target_table="heder1_final",
    )

    print("[heder1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="heder1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[heder1] Finished successfully.", flush=True)
