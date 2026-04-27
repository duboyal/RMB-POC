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
from app.duckdb_pipeline.transforms import cust1_transform


CUST1_COLUMNS = [
    "CUSTOMER NUMBER",
    "CUSTOMER NAME",
    "ADDRESS 1",
    "ADDRESS 2",
    "CITY",
    "STATE",
    "ZIP CODE",
    "TERMS CODE",
    "SALESPERSON #",
    "TELEPHONE #",
    "MASTER A/R TAG #",
    "OLDEST INV. JULIAN DATE",
    "CREDIT LIMIT",
    "# A/R TRANSACTIONS",
    "# OF PAYMENTS",
    "TOTAL # OF DAYS",
    "# OF TIMES PAST DUE",
    "LAST INVOICE DATE",
    "LAST PAYMENT DATE",
    "A/R BALANCE",
    "CUSTOMER CLASS #",
    "DISCOUNT % AMOUNT",
    "DISCOUNT $ AMOUNT",
    "MTD SALES $",
    "YTD SALES $",
    "UNASSIGNED_26",
    "YTD COST $",
    "MTD COST $",
    "YTD RETURN $",
    "PRINT STATEMENT?",
    "MTD RETURN $",
    "MTD COMMISSION $",
    "YTD COMMISSION $",
    "MTD # OF INVOICES",
    "YTD # OF INVOICES",
    "OPEN ORDER $",
    "# OF OPEN ORDERS",
    "ROUTE #",
    "SHIP-TO CODE",
    "MINIMUM ORDER $ AMOUNT",
    "DATE ACCOUNT OPENED",
    "KEYOFF FLAG",
    "SHIP VIA",
    "INTERNAL REMARKS 1",
    "INTERNAL REMARKS 2",
    "ACCOUNTING REMARKS",
    "YTD # OF CREDIT MEMOS",
    "MTD # OF CREDIT MEMOS",
    "YTD # OF RETURNED CHECKS",
    "SALES CONTACT NAME",
    "CUSTOMER ALPHA NAME",
    "SPECIAL INSTRUCTIONS 1",
    "SPECIAL INSTRUCTIONS 2",
    "PRICE CODE",
    "CREDIT DEPARTMENT CONTACT",
    "BANK NAME",
    "BANK ACCOUNT #",
    "SALES $ MOVEMENT #1",
    "SALES $ MOVEMENT #2",
    "SALES $ MOVEMENT #3",
    "SALES $ MOVEMENT #4",
    "SALES $ MOVEMENT #5",
    "SALES $ MOVEMENT #6",
    "SALES $ MOVEMENT #7",
    "SALES $ MOVEMENT #8",
    "SALES $ MOVEMENT #9",
    "SALES $ MOVEMENT #10",
    "SALES $ MOVEMENT #11",
    "SALES $ MOVEMENT #12",
    "CURRENT MTD SALES $",
    "LAST YEAR YTD SALES $",
    "COST MOVEMENT #1",
    "COST MOVEMENT #2",
    "COST MOVEMENT #3",
    "COST MOVEMENT #4",
    "COST MOVEMENT #5",
    "COST MOVEMENT #6",
    "COST MOVEMENT #7",
    "COST MOVEMENT #8",
    "COST MOVEMENT #9",
    "COST MOVEMENT #10",
    "COST MOVEMENT #11",
    "COST MOVEMENT #12",
    "CURRENT MTD COST",
    "LAST YEAR YTD COST",
    "A/R MOVEMENT #1",
    "A/R MOVEMENT #2",
    "A/R MOVEMENT #3",
    "A/R MOVEMENT #4",
    "A/R MOVEMENT #5",
    "A/R MOVEMENT #6",
    "A/R MOVEMENT #7",
    "A/R MOVEMENT #8",
    "A/R MOVEMENT #9",
    "A/R MOVEMENT #10",
    "A/R MOVEMENT #11",
    "A/R MOVEMENT #12",
    "CURRENT MTD A/R",
    "LAST YEAR YTD A/R",
    "TAX CODE",
    "LAST PAY AMOUNT",
    "CALL DAY",
    "DELIVERY DAY",
    "CALL TIME",
    "LOCKOUT FLAG",
    "CALL SHEETS?",
    "PICK-UP ALLOWANCE?",
    "MTD LOST SALES $",
    "YTD LOST SALES $",
    "MTD LOST SALES PIECES",
    "YTD LOST SALES PIECES",
    "UNASSIGNED_112",
    "CUSTOMER REF/OLD #",
    "FAX TELEPHONE #",
    "STORE #",
    "FINANCE CHARGE",
    "SRP ZONE",
]

REQUIRED_CUST1_COLUMNS = {
    "CUSTOMER NUMBER",
    "CUSTOMER NAME",
}


def detect_header(file_path: str, delim: str = "|") -> bool:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        first_line = f.readline().strip()

    if not first_line:
        raise ValueError(f"File is empty: {file_path}")

    first_row = [part.strip() for part in first_line.split(delim)]
    expected = set(CUST1_COLUMNS)

    overlap = sum(1 for col in first_row if col in expected)
    return overlap >= 3


def validate_expected_columns(table_name: str) -> None:
    columns = get_table_columns(table_name)

    if len(columns) != len(CUST1_COLUMNS):
        raise ValueError(
            f"CUST1 column count mismatch: expected {len(CUST1_COLUMNS)}, got {len(columns)}"
        )

    missing_required = REQUIRED_CUST1_COLUMNS - set(columns)
    if missing_required:
        raise ValueError(f"CUST1 missing required columns: {sorted(missing_required)}")


def run_cust1_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "|",
    postgres_table: str = "cust1",
    if_exists: str = "replace",
) -> None:
    print("[cust1] Detecting header...", flush=True)
    has_header = detect_header(file_path=file_path, delim=delim)
    print(f"[cust1] Header detected: {has_header}", flush=True)

    print("[cust1] Staging file into DuckDB...", flush=True)
    stage_delimited_file(
        file_path=file_path,
        table_name="cust1_raw",
        delim=delim,
        header=has_header,
        replace=True,
    )

    print("[cust1] Checking for trailing empty column...", flush=True)
    drop_extra_trailing_column_if_present(
        table_name="cust1_raw",
        expected_column_count=len(CUST1_COLUMNS),
    )

    print("[cust1] Validating columns...", flush=True)
    if has_header:
        validate_expected_columns("cust1_raw")
    else:
        current_columns = get_table_columns("cust1_raw")
        if len(current_columns) != len(CUST1_COLUMNS):
            raise ValueError(
                f"CUST1 raw file column count mismatch: expected {len(CUST1_COLUMNS)}, got {len(current_columns)}"
            )

        rename_columns(
            table_name="cust1_raw",
            new_column_names=CUST1_COLUMNS,
        )

        validate_expected_columns("cust1_raw")

    print("[cust1] Transforming in DuckDB...", flush=True)
    cust1_transform(
        raw_table="cust1_raw",
        trimmed_table="cust1_trimmed",
        valid_table="cust1_valid",
        reject_table="cust1_rejects",
        final_table="cust1_final",
    )

    print("[cust1] Exporting to Postgres...", flush=True)
    export_duckdb_table_to_postgres(
        duckdb_table="cust1_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[cust1] Finished successfully.", flush=True)
