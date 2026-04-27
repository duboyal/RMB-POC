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


RAW_DETAL1_HEADER = [
    "KEY",
    "ITEM #",
    "DESCRIPTION",
    "UNIT SIZE",
    "PACK",
    "UNIT GROSS WEIGHT",
    "COST PER UNIT",
    "PRICE PER UNIT",
    "TOTAL NET WEIGHT",
    "Unassigned",
    "QUANTITY ORDERED",
    "QUANTITY SHIPPED",
    "PRICING UNIT",
    "ITEM CATEGORY/GROUP",
    "UPC NUMBER",
    "TAXABLE",
    "Unassigned",
    "Unassigned",
    "BIN LOCATION",
    "UNIT AVERAGE NET WEIGHT",
    "PIECES PER CASE",
    "PRODUCT TYPE",
    "MINIMUM WEIGHT/CASE",
    "CASE CUBED DIMENSIONS",
    "Unassigned",
    "WAREHOUSE #",
    "ORDER UNIT",
    "OLD QUANTITY (LOST SALES)",
    "SRP AMOUNT",
    "BRAND",
    "MAXIMUM WEIGHT/CASE",
    "RETURN TO INVENTORY (C/M)",
    "LINE REMARK",
]

DETAL1_COLUMNS = [
    "KEY",
    "ITEM #",
    "DESCRIPTION",
    "UNIT SIZE",
    "PACK",
    "UNIT GROSS WEIGHT",
    "COST PER UNIT",
    "PRICE PER UNIT",
    "TOTAL NET WEIGHT",
    "UNASSIGNED_10",
    "QUANTITY ORDERED",
    "QUANTITY SHIPPED",
    "PRICING UNIT",
    "ITEM CATEGORY/GROUP",
    "UPC NUMBER",
    "TAXABLE",
    "UNASSIGNED_17",
    "UNASSIGNED_18",
    "BIN LOCATION",
    "UNIT AVERAGE NET WEIGHT",
    "PIECES PER CASE",
    "PRODUCT TYPE",
    "MINIMUM WEIGHT/CASE",
    "CASE CUBED DIMENSIONS",
    "UNASSIGNED_25",
    "WAREHOUSE #",
    "ORDER UNIT",
    "OLD QUANTITY (LOST SALES)",
    "SRP AMOUNT",
    "BRAND",
    "MAXIMUM WEIGHT/CASE",
    "RETURN TO INVENTORY (C/M)",
    "LINE REMARK",
]

REQUIRED_DETAL1_COLUMNS = {"KEY", "ITEM #"}


def validate_header_row(file_path: str, delim: str = "|") -> None:
    path = Path(file_path)

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    actual_header = [part.strip() for part in first_line.split(delim)]

    if actual_header != RAW_DETAL1_HEADER:
        raise ValueError(
            "DETAL1 header does not match expected layout. "
            f"Expected {len(RAW_DETAL1_HEADER)} columns, got {len(actual_header)}."
        )


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(DETAL1_COLUMNS):
        raise ValueError(
            f"DETAL1 column count mismatch: expected {len(DETAL1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_DETAL1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"DETAL1 missing required columns: {sorted(missing_required)}")


def run_detal1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "|",
    postgres_table: str = "detal1",
    if_exists: str = "replace",
) -> None:
    print("[detal1] Validating header row...", flush=True)
    validate_header_row(file_path=file_path, delim=delim)

    print("[detal1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="detal1_raw",
        delim=delim,
        header=False,
        skip_rows=1,
        replace=True,
    )

    print("[detal1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="detal1_raw",
        expected_column_count=len(DETAL1_COLUMNS),
    )

    print("[detal1] Renaming columns...", flush=True)
    rename_columns(
        table_name="detal1_raw",
        new_column_names=DETAL1_COLUMNS,
    )

    print("[detal1] Validating columns...", flush=True)
    validate_expected_columns("detal1_raw")

    print("[detal1] Transforming in DuckDB...", flush=True)
    trim_all_text_columns(
        source_table="detal1_raw",
        target_table="detal1_final",
    )

    print("[detal1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="detal1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[detal1] Finished successfully.", flush=True)
