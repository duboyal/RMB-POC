import re
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import inspect, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert

from db import engine


PRIMARY_KEYS = {
    "cust1": ["CUSTOMER NUMBER"],
    "heder1": ["ORDER #"],
    "inven1": ["ITEM #"],
    "shipt1": ["CUSTOMER #", "SHIP-TO #"],
}


def sanitize_table_name(path) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", path.stem).lower()


def add_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    df = df.copy()
    df["created_at"] = now
    df["updated_at"] = now
    return df


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


def table_exists(table_name: str) -> bool:
    inspector = inspect(engine)
    return inspector.has_table(table_name)


def append_dataframe(df: pd.DataFrame, table_name: str) -> None:
    df.to_sql(table_name, con=engine, if_exists="append", index=False)


def upsert_dataframe(
    df: pd.DataFrame, table_name: str, key_columns: list[str], chunk_size: int = 1000
) -> None:
    if df.empty:
        print(f"No rows to upsert for table: {table_name}")
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

    for key in key_columns:
        if key not in db_columns:
            raise ValueError(
                f"Key column '{key}' does not exist in DB table '{table_name}'"
            )

    writable_columns = [col for col in df.columns if col in db_columns]

    records = (
        df[writable_columns]
        .where(pd.notnull(df[writable_columns]), None)
        .to_dict(orient="records")
    )

    if not records:
        print(f"No valid records to upsert for table: {table_name}")
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
                set_clause["updated_at"] = datetime.now(timezone.utc)

            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=key_columns,
                set_=set_clause,
            )

            conn.execute(upsert_stmt)


def create_table_and_seed(
    df: pd.DataFrame, table_name: str, key_columns: list[str] | None = None
) -> None:
    if key_columns:
        ensure_key_columns_present(df, key_columns, table_name)
        ensure_no_duplicate_keys(df, key_columns, table_name)

    append_dataframe(df, table_name)

    if key_columns:
        add_primary_key_constraint(table_name, key_columns)
        print(f"Added primary key on {table_name}: {key_columns}")


def import_file(path):
    df = pd.read_csv(path, sep="\t")
    table_name = sanitize_table_name(path)
    df = add_timestamps(df)

    key_columns = PRIMARY_KEYS.get(table_name)
    exists = table_exists(table_name)

    if not exists:
        print(f"Creating new table and inserting rows: {table_name}")
        create_table_and_seed(df, table_name, key_columns)
        return len(df)

    if not key_columns:
        print(f"Table {table_name} exists, but no key is defined. Appending only.")
        append_dataframe(df, table_name)
        return len(df)

    print(f"Table {table_name} exists. Upserting rows with keys: {key_columns}")
    upsert_dataframe(df, table_name, key_columns)
    return len(df)


## ----------OLD CODE BELOW HERE--------------
# import pandas as pd
# import re
# from datetime import datetime


# def import_file(path):
#     df = pd.read_csv(path, sep="\t")  #  IMPORTANT (your files are tab-separated)

#     # clean table name
#     table_name = re.sub(r"[^a-zA-Z0-9_]", "_", path.stem).lower()

#     print(f"Importing into table: {table_name}")

#     # timestamps
#     now = datetime.utcnow()
#     df["created_at"] = now
#     df["updated_at"] = now

#     df.to_sql(
#         table_name, con=engine, if_exists="append", index=False  # keep append for now
#     )

#     return len(df)

## ----------OLD CODE BELOW HERE--------------
# def import_file(path):
#     df = pd.read_csv(path)
#     df.to_sql("order_header", con=engine, if_exists="append", index=False)
#     return len(df)
