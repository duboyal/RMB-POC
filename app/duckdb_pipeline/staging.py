from __future__ import annotations

from pathlib import Path

import duckdb

from app.duckdb_pipeline.config import DUCKDB_PATH, ensure_data_dir


def get_connection(duckdb_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    ensure_data_dir()
    db_path = Path(duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def stage_delimited_file(
    file_path: str,
    table_name: str,
    delim: str = "|",
    header: bool = True,
    replace: bool = True,
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    create_clause = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE"
    escaped_path = path.as_posix().replace("'", "''")
    escaped_delim = delim.replace("'", "''")

    con = get_connection(duckdb_path)
    try:
        con.execute(
            f"""
            {create_clause} "{table_name}" AS
            SELECT *
            FROM read_csv_auto(
                '{escaped_path}',
                delim='{escaped_delim}',
                header={str(header).lower()},
                sample_size=-1,
                strict_mode=false,
                null_padding=true,
                ignore_errors=false
            )
            """
        )
    finally:
        con.close()


def get_table_columns(
    table_name: str,
    duckdb_path: str = DUCKDB_PATH,
) -> list[str]:
    con = get_connection(duckdb_path)
    try:
        rows = con.execute(f'DESCRIBE "{table_name}"').fetchall()
        return [row[0] for row in rows]
    finally:
        con.close()


def rename_columns(
    table_name: str,
    new_column_names: list[str],
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    existing_names = get_table_columns(table_name, duckdb_path=duckdb_path)

    if len(existing_names) != len(new_column_names):
        raise ValueError(
            f"Column count mismatch: existing={len(existing_names)} new={len(new_column_names)}"
        )

    con = get_connection(duckdb_path)
    try:
        for old_name, new_name in zip(existing_names, new_column_names):
            if old_name != new_name:
                con.execute(
                    f'ALTER TABLE "{table_name}" RENAME COLUMN "{old_name}" TO "{new_name}"'
                )
    finally:
        con.close()


def drop_extra_trailing_column_if_present(
    table_name: str,
    expected_column_count: int,
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    """
    Drop one extra final column if it exists and is completely empty.

    This handles files where rows end with a trailing delimiter:
        a|b|c|
    which creates one extra blank column.
    """
    columns = get_table_columns(table_name, duckdb_path=duckdb_path)

    if len(columns) == expected_column_count:
        return

    if len(columns) != expected_column_count + 1:
        raise ValueError(
            f'Unexpected column count for "{table_name}": '
            f"expected {expected_column_count} or {expected_column_count + 1}, got {len(columns)}"
        )

    extra_col = columns[-1]

    con = get_connection(duckdb_path)
    try:
        non_empty_count = con.execute(
            f"""
            SELECT COUNT(*)
            FROM "{table_name}"
            WHERE "{extra_col}" IS NOT NULL
              AND TRIM(CAST("{extra_col}" AS VARCHAR)) <> ''
            """
        ).fetchone()[0]

        if non_empty_count > 0:
            raise ValueError(
                f'Extra trailing column "{extra_col}" is not empty; refusing to drop it.'
            )

        con.execute(f'ALTER TABLE "{table_name}" DROP COLUMN "{extra_col}"')
        print(
            f'Dropped empty trailing column "{extra_col}" from "{table_name}"',
            flush=True,
        )
    finally:
        con.close()


def row_count(
    table_name: str,
    duckdb_path: str = DUCKDB_PATH,
) -> int:
    con = get_connection(duckdb_path)
    try:
        return con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    finally:
        con.close()
