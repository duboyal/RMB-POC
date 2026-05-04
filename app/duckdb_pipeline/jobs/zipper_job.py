from __future__ import annotations

from app.duckdb_pipeline.config import POSTGRES_URL
from app.duckdb_pipeline.export import export_duckdb_table_to_postgres
from app.duckdb_pipeline.staging import stage_delimited_file
from app.duckdb_pipeline.transforms import trim_all_text_columns


def run_zipper_job(
    file_path: str,
    postgres_url: str | None = None,
    delim: str = "|",
    postgres_table: str = "zipper",
    if_exists: str = "replace",
) -> None:
    print("[zipper] Staging file into DuckDB...", flush=True)

    # 1. Stage raw file
    stage_delimited_file(
        file_path=file_path,
        table_name="zipper_raw",
        delim=delim,
        header=True,
        replace=True,
    )

    print("[zipper] Transforming in DuckDB...", flush=True)

    # 2. Basic transform (same pattern as whsfl1)
    trim_all_text_columns(
        source_table="zipper_raw",
        target_table="zipper_final",
    )

    print("[zipper] Exporting to Postgres...", flush=True)

    # 3. Export to Postgres
    export_duckdb_table_to_postgres(
        duckdb_table="zipper_final",
        postgres_table=postgres_table,
        postgres_url=postgres_url or POSTGRES_URL,
        if_exists=if_exists,
    )

    print("[zipper] Finished successfully.", flush=True)