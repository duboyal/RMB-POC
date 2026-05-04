from app.duckdb_pipeline.staging import stage_delimited_file
from app.duckdb_pipeline.export import export_duckdb_table_to_postgres


def run_zipper_job(file_path: str) -> None:
    print(f"[zipper] Processing file: {file_path}", flush=True)

    table_name = "zipper"

    # 1. Stage into DuckDB
    stage_delimited_file(
        file_path=file_path,
        table_name=table_name,
        delim="|",          
        replace=True,
    )

    print(f"[zipper] Staged data into DuckDB", flush=True)

    # 2. Export to Postgres
    export_duckdb_table_to_postgres(table_name)

    print(f"[zipper] Exported table '{table_name}' to Postgres", flush=True)
    print("[zipper] Done.", flush=True)