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


RAW_INVEN1_HEADER = [
    'ITEM #',
    'DESCRIPTION',
    'OLD ITEM NUMBER',
    'QUANTITY PER PACK',
    'UNIT OF MEASURE',
    'GROSS WEIGHT',
    'NET WEIGHT',
    'PIECES PER CASE',
    'MAJOR VENDOR',
    'SECONDARY VENDOR',
    'LOCK-OUT CODE',
    'BILLING TYPE',
    'REFRIGERATION CODE',
    'KEY STOCK',
    'PRIOR SELLING COST',
    'LAST COST',
    'AVERAGE COST',
    'CASE PRICING UNIT',
    'PIECE PRICING UNIT',
    'MINIMUM WEIGHT/CASE',
    'CASE CUBED DIMENSIONS',
    'CASES PER PALLET',
    'CATEGORY/GROUP',
    'PRICE LIST',
    'UPC CODE',
    'TAXABLE',
    'Currency Code',
    'SELLING COST',
    'BROKEN CASE SURCHARGE %',
    'Unassigned',
    'LAST INVOICE DATE',
    'MAXIMUM WEIGHT/CASE',
    'LAST PRICE CHANGE DATE',
    'LAST COST DATE',
    'ITEM ALPHA NAME',
    'MINIMUM ON-HAND',
    'MAXIMUM ON-HAND',
    'SUBSTITUTION ITEM 1',
    'SUBSTITUTION ITEM 2',
    'DATE ITEM # BEGAN',
    'BRAND NAME',
    'P.O. ALERT %',
    'SRP UNIT',
    'SRP FACTOR',
    'BUYER CODE',
    'CYCLE COUNT MONTHS',
    'Unassigned',
    'Unassigned',
    'Unassigned',
]

INVEN1_COLUMNS = [
    'ITEM #',
    'DESCRIPTION',
    'OLD ITEM NUMBER',
    'QUANTITY PER PACK',
    'UNIT OF MEASURE',
    'GROSS WEIGHT',
    'NET WEIGHT',
    'PIECES PER CASE',
    'MAJOR VENDOR',
    'SECONDARY VENDOR',
    'LOCK-OUT CODE',
    'BILLING TYPE',
    'REFRIGERATION CODE',
    'KEY STOCK',
    'PRIOR SELLING COST',
    'LAST COST',
    'AVERAGE COST',
    'CASE PRICING UNIT',
    'PIECE PRICING UNIT',
    'MINIMUM WEIGHT/CASE',
    'CASE CUBED DIMENSIONS',
    'CASES PER PALLET',
    'CATEGORY/GROUP',
    'PRICE LIST',
    'UPC CODE',
    'TAXABLE',
    'Currency Code',
    'SELLING COST',
    'BROKEN CASE SURCHARGE %',
    'UNASSIGNED_30',
    'LAST INVOICE DATE',
    'MAXIMUM WEIGHT/CASE',
    'LAST PRICE CHANGE DATE',
    'LAST COST DATE',
    'ITEM ALPHA NAME',
    'MINIMUM ON-HAND',
    'MAXIMUM ON-HAND',
    'SUBSTITUTION ITEM 1',
    'SUBSTITUTION ITEM 2',
    'DATE ITEM # BEGAN',
    'BRAND NAME',
    'P.O. ALERT %',
    'SRP UNIT',
    'SRP FACTOR',
    'BUYER CODE',
    'CYCLE COUNT MONTHS',
    'UNASSIGNED_47',
    'UNASSIGNED_48',
    'UNASSIGNED_49',
]

REQUIRED_INVEN1_COLUMNS = {'ITEM #'}


def validate_header_row(file_path: str, delim: str = "\t") -> None:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File is empty: {file_path}")

    actual_header = [part.strip() for part in first_line.split(delim)]

    if actual_header != RAW_INVEN1_HEADER:
        raise ValueError(
            "INVEN1 header does not match expected layout. "
            f"Expected {len(RAW_INVEN1_HEADER)} columns, got {len(actual_header)}."
        )


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(INVEN1_COLUMNS):
        raise ValueError(
            f"INVEN1 column count mismatch: expected {len(INVEN1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_INVEN1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"INVEN1 missing required columns: {sorted(missing_required)}")


def run_inven1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "\t",
    postgres_table: str = "inven1",
    if_exists: str = "replace",
) -> None:
    print("[inven1] Validating header row...", flush=True)
    validate_header_row(file_path=file_path, delim=delim)

    print("[inven1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="inven1_raw",
        delim=delim,
        header=False,
        skip_rows=1,
        replace=True,
    )

    print("[inven1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="inven1_raw",
        expected_column_count=len(INVEN1_COLUMNS),
    )

    print("[inven1] Renaming columns...", flush=True)
    rename_columns(
        table_name="inven1_raw",
        new_column_names=INVEN1_COLUMNS,
    )

    print("[inven1] Validating columns...", flush=True)
    validate_expected_columns("inven1_raw")

    print("[inven1] Transforming in DuckDB...", flush=True)
    trim_all_text_columns(
        source_table="inven1_raw",
        target_table="inven1_final",
    )

    print("[inven1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="inven1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[inven1] Finished successfully.", flush=True)
