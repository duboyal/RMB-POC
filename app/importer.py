import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.dialects.postgresql import insert

from db import engine


PRIMARY_KEYS = {
    "cust1": ["CUSTOMER NUMBER"],
    "detal1": ["CUSTOMER #", "INVOICE #", "LINE NUMBER"],
    "heder1": ["ORDER #"],
    "inven1": ["ITEM #"],
    "oitem1": ["KEY"],
    "pcode1": ["PRODUCT CATEGORY CODE"],
    "scode1": ["PRODUCT GROUP CODE"],
    "shipt1": ["CUSTOMER #", "SHIP-TO #"],
    "whsfl1": ["WAREHOUSE #", "ITEM #"],
}

APPEND_ONLY_TABLES = {
    "heder1",
    "detal1",
    "oitem1",
}

UPSERT_TABLES = {
    "cust1",
    "inven1",
    "shipt1",
    "pcode1",
    "scode1",
    "whsfl1",
}


def sanitize_table_name(path: Path) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", path.stem).lower()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_dataframe(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        sep="|",
        dtype=str,
        skipinitialspace=True,
    ).fillna("")


def add_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    now = utc_now()
    df = df.copy()
    df["created_at"] = now
    df["updated_at"] = now
    return df


def table_exists(table_name: str) -> bool:
    return inspect(engine).has_table(table_name)


def ensure_key_columns_present(
    df: pd.DataFrame, key_columns: list[str], table_name: str
) -> None:
    missing_keys = [col for col in key_columns if col not in df.columns]
    if missing_keys:
        raise ValueError(
            f"Incoming data for table '{table_name}' is missing key column(s): {missing_keys}"
        )


def ensure_no_duplicate_keys(
    df: pd.DataFrame, key_columns: list[str], table_name: str
) -> None:
    duplicated = df[df.duplicated(subset=key_columns, keep=False)]
    if not duplicated.empty:
        sample = duplicated[key_columns].head(10).to_dict(orient="records")
        raise ValueError(
            f"Cannot create primary key for table '{table_name}' because incoming file contains duplicate key values. "
            f"Sample duplicates: {sample}"
        )


def append_dataframe(df: pd.DataFrame, table_name: str) -> None:
    df.to_sql(table_name, con=engine, if_exists="append", index=False)


def add_primary_key_constraint(table_name: str, key_columns: list[str]) -> None:
    constraint_name = f"{table_name}_pk"
    quoted_columns = ", ".join(f'"{col}"' for col in key_columns)

    sql = text(
        f"""
        ALTER TABLE "{table_name}"
        ADD CONSTRAINT "{constraint_name}" PRIMARY KEY ({quoted_columns})
        """
    )

    with engine.begin() as conn:
        conn.execute(sql)


def create_table_and_seed(
    df: pd.DataFrame, table_name: str, key_columns: list[str] | None = None
) -> None:
    if key_columns:
        ensure_key_columns_present(df, key_columns, table_name)
        ensure_no_duplicate_keys(df, key_columns, table_name)

    append_dataframe(df, table_name)

    if key_columns:
        add_primary_key_constraint(table_name, key_columns)
        print(f"Added primary key on {table_name}: {key_columns}", flush=True)


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    key_columns: list[str],
    chunk_size: int = 1000,
) -> None:
    if df.empty:
        print(f"No rows to upsert for table: {table_name}", flush=True)
        return

    ensure_key_columns_present(df, key_columns, table_name)

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    db_columns = {col.name for col in table.columns}

    unknown_input_columns = [col for col in df.columns if col not in db_columns]
    if unknown_input_columns:
        raise ValueError(
            f"Incoming data has column(s) not present in DB table '{table_name}': {unknown_input_columns}"
        )

    missing_db_keys = [key for key in key_columns if key not in db_columns]
    if missing_db_keys:
        raise ValueError(
            f"Key column(s) missing from DB table '{table_name}': {missing_db_keys}"
        )

    writable_columns = [col for col in df.columns if col in db_columns]

    records = (
        df[writable_columns]
        .where(pd.notnull(df[writable_columns]), None)
        .to_dict(orient="records")
    )

    if not records:
        print(f"No valid records to upsert for table: {table_name}", flush=True)
        return

    update_columns = [
        col
        for col in writable_columns
        if col not in key_columns and col != "created_at"
    ]

    with engine.begin() as conn:
        for start in range(0, len(records), chunk_size):
            batch = records[start : start + chunk_size]

            stmt = insert(table).values(batch)
            set_clause = {col: stmt.excluded[col] for col in update_columns}

            if "updated_at" in db_columns:
                set_clause["updated_at"] = utc_now()

            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=key_columns,
                set_=set_clause,
            )

            conn.execute(upsert_stmt)


def import_file(path: str | Path) -> int:
    path = Path(path)
    df = load_dataframe(path)

    print("COLUMNS:", df.columns.tolist(), flush=True)
    print(df.head(5).to_string(), flush=True)

    table_name = sanitize_table_name(path)
    df = add_timestamps(df)

    exists = table_exists(table_name)
    key_columns = PRIMARY_KEYS.get(table_name)

    if table_name in APPEND_ONLY_TABLES:
        if not exists:
            print(
                f"Creating new append-only table and inserting rows: {table_name}",
                flush=True,
            )
            append_dataframe(df, table_name)
        else:
            print(
                f"Appending rows into existing append-only table: {table_name}",
                flush=True,
            )
            append_dataframe(df, table_name)
        return len(df)

    if table_name in UPSERT_TABLES:
        if not key_columns:
            raise ValueError(
                f"Table '{table_name}' is marked as UPSERT but has no PRIMARY_KEYS entry."
            )

        if not exists:
            print(
                f"Creating new upsert table and inserting rows: {table_name}",
                flush=True,
            )
            create_table_and_seed(df, table_name, key_columns)
        else:
            print(f"Upserting rows into existing table: {table_name}", flush=True)
            upsert_dataframe(df, table_name, key_columns)
        return len(df)

    print(
        f"Table '{table_name}' is not classified. Defaulting to append-only.",
        flush=True,
    )
    append_dataframe(df, table_name)
    return len(df)
