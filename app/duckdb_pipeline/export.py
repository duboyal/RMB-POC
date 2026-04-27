from __future__ import annotations

from sqlalchemy import create_engine, text

from app.duckdb_pipeline.config import DUCKDB_PATH
from app.duckdb_pipeline.staging import get_connection


def export_duckdb_table_to_postgres(
    duckdb_table: str,
    postgres_table: str,
    postgres_url: str,
    if_exists: str = "append",
    schema: str | None = None,
    duckdb_path: str = DUCKDB_PATH,
) -> None:
    if not postgres_url:
        raise ValueError("postgres_url is required")

    print(f"[export] Reading DuckDB table: {duckdb_table}", flush=True)
    con = get_connection(duckdb_path)
    try:
        df = con.execute(f'SELECT * FROM "{duckdb_table}"').df()
    finally:
        con.close()

    print(f"[export] Rows to export: {len(df)}", flush=True)
    print(f"[export] Writing to Postgres table: {postgres_table}", flush=True)
    print(f"[config] Writing to DB: {POSTGRES_URL}", flush=True)
    engine = create_engine(postgres_url, pool_pre_ping=True)

    with engine.begin() as pg_conn:
        df.to_sql(
            name=postgres_table,
            con=pg_conn,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=500,
        )

    print("[export] Postgres export finished.", flush=True)


def truncate_postgres_table(
    postgres_table: str,
    postgres_url: str,
    schema: str | None = None,
) -> None:
    if not postgres_url:
        raise ValueError("postgres_url is required")

    engine = create_engine(postgres_url, pool_pre_ping=True)
    qualified_name = f"{schema}.{postgres_table}" if schema else postgres_table

    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{qualified_name}"'))
