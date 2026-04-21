import io
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect

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


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_ident_list(names: list[str]) -> str:
    return ", ".join(quote_ident(name) for name in names)


def load_dataframe(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep="|",
        dtype=str,
        skipinitialspace=True,
        index_col=False,
    ).fillna("")

    df.columns = df.columns.str.strip()
    return df


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


def create_table_from_dataframe(df: pd.DataFrame, table_name: str) -> None:
    df.head(0).to_sql(table_name, con=engine, if_exists="fail", index=False)


def add_primary_key_constraint(table_name: str, key_columns: list[str]) -> None:
    constraint_name = f"{table_name}_pk"
    quoted_columns = quote_ident_list(key_columns)

    sql = f"""
    ALTER TABLE {quote_ident(table_name)}
    ADD CONSTRAINT {quote_ident(constraint_name)} PRIMARY KEY ({quoted_columns})
    """

    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def dataframe_to_copy_buffer(df: pd.DataFrame) -> io.StringIO:
    copy_df = df.copy()

    for col in copy_df.columns:
        copy_df[col] = copy_df[col].apply(
            lambda x: (
                ""
                if pd.isna(x)
                else x.isoformat() if hasattr(x, "isoformat") else str(x)
            )
        )

    buffer = io.StringIO()
    copy_df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="")
    buffer.seek(0)
    return buffer


def copy_dataframe_to_table(df: pd.DataFrame, table_name: str) -> None:
    buffer = dataframe_to_copy_buffer(df)
    quoted_table = quote_ident(table_name)
    quoted_columns = quote_ident_list(df.columns.tolist())

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            copy_sql = (
                f"COPY {quoted_table} ({quoted_columns}) "
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '', HEADER FALSE)"
            )
            cur.copy_expert(copy_sql, buffer)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def append_dataframe(df: pd.DataFrame, table_name: str) -> None:
    if not table_exists(table_name):
        create_table_from_dataframe(df, table_name)
    copy_dataframe_to_table(df, table_name)


def create_table_and_seed(
    df: pd.DataFrame, table_name: str, key_columns: list[str] | None = None
) -> None:
    if key_columns:
        ensure_key_columns_present(df, key_columns, table_name)
        ensure_no_duplicate_keys(df, key_columns, table_name)

    create_table_from_dataframe(df, table_name)

    if key_columns:
        add_primary_key_constraint(table_name, key_columns)
        print(f"Added primary key on {table_name}: {key_columns}", flush=True)

    copy_dataframe_to_table(df, table_name)


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    key_columns: list[str],
) -> None:
    if df.empty:
        print(f"No rows to upsert for table: {table_name}", flush=True)
        return

    ensure_key_columns_present(df, key_columns, table_name)

    inspector = inspect(engine)
    db_columns = {col["name"] for col in inspector.get_columns(table_name)}

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
    df = df[writable_columns].copy()

    temp_table = f"{table_name}_staging_temp"

    quoted_target = quote_ident(table_name)
    quoted_temp = quote_ident(temp_table)
    quoted_columns = quote_ident_list(writable_columns)

    join_condition = " AND ".join(
        f"t.{quote_ident(col)} = s.{quote_ident(col)}" for col in key_columns
    )

    insert_not_exists_condition = " AND ".join(
        f"t.{quote_ident(col)} IS NULL" for col in key_columns
    )

    update_columns = [
        col
        for col in writable_columns
        if col not in key_columns and col != "created_at"
    ]

    set_assignments = []
    for col in update_columns:
        if col == "updated_at":
            set_assignments.append(f"{quote_ident(col)} = NOW()")
        else:
            set_assignments.append(f"{quote_ident(col)} = s.{quote_ident(col)}")

    change_conditions = [
        f"t.{quote_ident(col)} IS DISTINCT FROM s.{quote_ident(col)}"
        for col in update_columns
        if col != "updated_at"
    ]

    update_sql = f"""
    UPDATE {quoted_target} AS t
    SET {", ".join(set_assignments)}
    FROM {quoted_temp} AS s
    WHERE {join_condition}
      AND ({' OR '.join(change_conditions)})
    """

    insert_sql = f"""
    INSERT INTO {quoted_target} ({quoted_columns})
    SELECT {quoted_columns}
    FROM {quoted_temp} AS s
    LEFT JOIN {quoted_target} AS t
      ON {join_condition}
    WHERE {insert_not_exists_condition}
    """

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            print(f"Creating temp staging table for {table_name}", flush=True)
            cur.execute(
                f"CREATE TEMP TABLE {quoted_temp} (LIKE {quoted_target} INCLUDING DEFAULTS) ON COMMIT DROP"
            )

            print(
                f"Copying incoming rows into temp staging table for {table_name}",
                flush=True,
            )
            buffer = dataframe_to_copy_buffer(df)
            copy_sql = (
                f"COPY {quoted_temp} ({quoted_columns}) "
                "FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '', HEADER FALSE)"
            )
            cur.copy_expert(copy_sql, buffer)

            print(f"Updating changed rows for {table_name}", flush=True)
            cur.execute(update_sql)
            updated_count = cur.rowcount

            print(f"Inserting new rows for {table_name}", flush=True)
            cur.execute(insert_sql)
            inserted_count = cur.rowcount

        raw_conn.commit()
        print(
            f"Finished upsert for {table_name}: inserted={inserted_count}, updated={updated_count}",
            flush=True,
        )
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()


def import_file(path: str | Path) -> int:
    path = Path(path)
    df = load_dataframe(path)

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

        print(f"IMPORT COMPLETE: {table_name} ({len(df)} rows)", flush=True)
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

        print(f"IMPORT COMPLETE: {table_name} ({len(df)} rows)", flush=True)
        return len(df)

    print(
        f"Table '{table_name}' is not classified. Defaulting to append-only.",
        flush=True,
    )
    append_dataframe(df, table_name)
    print(f"IMPORT COMPLETE: {table_name} ({len(df)} rows)", flush=True)
    return len(df)
