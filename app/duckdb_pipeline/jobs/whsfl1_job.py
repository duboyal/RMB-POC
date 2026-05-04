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


RAW_WHSFL1_HEADER = [
    'WAREHOUSE #',
    'ITEM #',
    'DESCRIPTION',
    'BIN LOCATION',
    'QUANTITY ON-HAND',
    'QUANTITY COMMITTED',
    'QUANTITY ON-ORDER',
    'Unassigned',
    'MTD RECEIPTS, UNITS',
    'YTD RECEIPTS, UNITS',
    'MTD ADJUSTMENTS, UNITS',
    'YTD ADJUSTMENTS, UNITS',
    'MTD RETURNS $$ (SALEABLE)',
    'YTD RETURNS $$ (SALEABLE)',
    'MTD SALES $$',
    'YTD SALES $$',
    'MTD SALES CASES',
    'YTD SALES CASES',
    'MTD SALES LBS.',
    'YTD SALES LBS.',
    'MONTHLY BEGINNING INV.',
    'LAST COST',
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
    'CASES MOVEMENT 1',
    'CASES MOVEMENT 2',
    'CASES MOVEMENT 3',
    'CASES MOVEMENT 4',
    'CASES MOVEMENT 5',
    'CASES MOVEMENT 6',
    'CASES MOVEMENT 7',
    'CASES MOVEMENT 8',
    'CASES MOVEMENT 9',
    'CASES MOVEMENT 10',
    'CASES MOVEMENT 11',
    'CASES MOVEMENT 12',
    'CURRENT MTD PIECES',
    'LAST YEAR YTD PIECES',
    'MTD COST $',
    'YTD COST $',
    'LBS. ON-HAND',
    'LBS. COMMITTED',
    'DAILY COMMITTED PIECES',
    'MTD NON-SALEABLE RETURN $',
    'YTD NON-SALEABLE RETURN $',
    'MTD LOST SALES $',
    'YTD LOST SALES $',
    'MTD LOST SALE PIECES',
    'YTD LOST SALE PIECES',
    'MTD NON-SALE RTRN PIECES',
    'COST MOVEMENT #1',
    'COST MOVEMENT #2',
    'COST MOVEMENT #3',
    'COST MOVEMENT #4',
    'COST MOVEMENT #5',
    'COST MOVEMENT #6',
    'COST MOVEMENT #7',
    'COST MOVEMENT #8',
    'COST MOVEMENT #9',
    'COST MOVEMENT #10',
    'COST MOVEMENT #11',
    'COST MOVEMENT #12',
    'CURRENT MTD COST MOVE',
    'LAST YEAR YTD COST MOVE',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'YTD NON-SALE RTRN PIECES',
    'MTD PURCHASE $$',
    'YTD PURCHASE $$',
    'Unassigned',
    'Unassigned',
    'Unassigned',
    'Unassigned',
]

WHSFL1_COLUMNS = [
    'WAREHOUSE #',
    'ITEM #',
    'DESCRIPTION',
    'BIN LOCATION',
    'QUANTITY ON-HAND',
    'QUANTITY COMMITTED',
    'QUANTITY ON-ORDER',
    'UNASSIGNED_8',
    'MTD RECEIPTS, UNITS',
    'YTD RECEIPTS, UNITS',
    'MTD ADJUSTMENTS, UNITS',
    'YTD ADJUSTMENTS, UNITS',
    'MTD RETURNS $$ (SALEABLE)',
    'YTD RETURNS $$ (SALEABLE)',
    'MTD SALES $$',
    'YTD SALES $$',
    'MTD SALES CASES',
    'YTD SALES CASES',
    'MTD SALES LBS.',
    'YTD SALES LBS.',
    'MONTHLY BEGINNING INV.',
    'LAST COST',
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
    'CASES MOVEMENT 1',
    'CASES MOVEMENT 2',
    'CASES MOVEMENT 3',
    'CASES MOVEMENT 4',
    'CASES MOVEMENT 5',
    'CASES MOVEMENT 6',
    'CASES MOVEMENT 7',
    'CASES MOVEMENT 8',
    'CASES MOVEMENT 9',
    'CASES MOVEMENT 10',
    'CASES MOVEMENT 11',
    'CASES MOVEMENT 12',
    'CURRENT MTD PIECES',
    'LAST YEAR YTD PIECES',
    'MTD COST $',
    'YTD COST $',
    'LBS. ON-HAND',
    'LBS. COMMITTED',
    'DAILY COMMITTED PIECES',
    'MTD NON-SALEABLE RETURN $',
    'YTD NON-SALEABLE RETURN $',
    'MTD LOST SALES $',
    'YTD LOST SALES $',
    'MTD LOST SALE PIECES',
    'YTD LOST SALE PIECES',
    'MTD NON-SALE RTRN PIECES',
    'COST MOVEMENT #1',
    'COST MOVEMENT #2',
    'COST MOVEMENT #3',
    'COST MOVEMENT #4',
    'COST MOVEMENT #5',
    'COST MOVEMENT #6',
    'COST MOVEMENT #7',
    'COST MOVEMENT #8',
    'COST MOVEMENT #9',
    'COST MOVEMENT #10',
    'COST MOVEMENT #11',
    'COST MOVEMENT #12',
    'CURRENT MTD COST MOVE',
    'LAST YEAR YTD COST MOVE',
    'UNASSIGNED_91',
    'UNASSIGNED_92',
    'UNASSIGNED_93',
    'UNASSIGNED_94',
    'UNASSIGNED_95',
    'UNASSIGNED_96',
    'UNASSIGNED_97',
    'UNASSIGNED_98',
    'UNASSIGNED_99',
    'UNASSIGNED_100',
    'UNASSIGNED_101',
    'UNASSIGNED_102',
    'UNASSIGNED_103',
    'UNASSIGNED_104',
    'YTD NON-SALE RTRN PIECES',
    'MTD PURCHASE $$',
    'YTD PURCHASE $$',
    'UNASSIGNED_108',
    'UNASSIGNED_109',
    'UNASSIGNED_110',
    'UNASSIGNED_111',
]

REQUIRED_WHSFL1_COLUMNS = {'ITEM #', 'WAREHOUSE #'}


def validate_header_row(file_path: str, delim: str = "\t") -> None:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File is empty: {file_path}")

    actual_header = [part.strip() for part in first_line.split(delim)]

    if actual_header != RAW_WHSFL1_HEADER:
        raise ValueError(
            "WHSFL1 header does not match expected layout. "
            f"Expected {len(RAW_WHSFL1_HEADER)} columns, got {len(actual_header)}."
        )


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(WHSFL1_COLUMNS):
        raise ValueError(
            f"WHSFL1 column count mismatch: expected {len(WHSFL1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_WHSFL1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"WHSFL1 missing required columns: {sorted(missing_required)}")


def run_whsfl1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "|",
    postgres_table: str = "whsfl1",
    if_exists: str = "replace",
) -> None:
    print("[whsfl1] Validating header row...", flush=True)
    validate_header_row(file_path=file_path, delim=delim)

    print("[whsfl1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="whsfl1_raw",
        delim=delim,
        header=False,
        skip_rows=1,
        replace=True,
    )

    print("[whsfl1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="whsfl1_raw",
        expected_column_count=len(WHSFL1_COLUMNS),
    )

    print("[whsfl1] Renaming columns...", flush=True)
    rename_columns(
        table_name="whsfl1_raw",
        new_column_names=WHSFL1_COLUMNS,
    )

    print("[whsfl1] Validating columns...", flush=True)
    validate_expected_columns("whsfl1_raw")

    print("[whsfl1] Transforming in DuckDB...", flush=True)
    trim_all_text_columns(
        source_table="whsfl1_raw",
        target_table="whsfl1_final",
    )

    print("[whsfl1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="whsfl1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[whsfl1] Finished successfully.", flush=True)