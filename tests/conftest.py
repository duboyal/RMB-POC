import os
from collections.abc import Generator

import pytest
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine


TEST_DATABASE_URL = os.environ["TEST_DATABASE_URL"]


TABLES_TO_CLEAN = [
    "cust1",
    "inven1",
    "shipt1",
    "heder1",
    "detal1",
]


@pytest.fixture(scope="session")
def engine() -> Generator[Engine, None, None]:
    engine = create_engine(TEST_DATABASE_URL, future=True)
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(engine: Engine) -> Generator[None, None, None]:
    with engine.begin() as conn:
        for table in reversed(TABLES_TO_CLEAN):
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
    yield
    with engine.begin() as conn:
        for table in reversed(TABLES_TO_CLEAN):
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))


def _insert_row(engine: Engine, table: str, row: dict) -> None:
    cols = ", ".join(row.keys())
    vals = ", ".join(f":{k}" for k in row.keys())
    sql = text(f"INSERT INTO {table} ({cols}) VALUES ({vals})")
    with engine.begin() as conn:
        conn.execute(sql, row)


def _fetch_one(engine: Engine, sql: str, params: dict | None = None) -> dict | None:
    with engine.begin() as conn:
        result = conn.execute(text(sql), params or {})
        row = result.mappings().first()
        return dict(row) if row else None


def _fetch_all(engine: Engine, sql: str, params: dict | None = None) -> list[dict]:
    with engine.begin() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(r) for r in result.mappings().all()]


def _fetch_val(engine: Engine, sql: str, params: dict | None = None):
    with engine.begin() as conn:
        result = conn.execute(text(sql), params or {})
        return result.scalar()


@pytest.fixture
def insert_row(engine: Engine):
    return lambda table, row: _insert_row(engine, table, row)


@pytest.fixture
def fetch_one(engine: Engine):
    return lambda sql, params=None: _fetch_one(engine, sql, params)


@pytest.fixture
def fetch_all(engine: Engine):
    return lambda sql, params=None: _fetch_all(engine, sql, params)


@pytest.fixture
def fetch_val(engine: Engine):
    return lambda sql, params=None: _fetch_val(engine, sql, params)


@pytest.fixture
def seed_cust1(insert_row):
    def _seed(**overrides):
        row = {
            "customer_number": "000123",
            "customer_name": "Default Name",
            "city": "Buffalo",
            "state": "NY",
        }
        row.update(overrides)
        insert_row("cust1", row)

    return _seed


@pytest.fixture
def seed_inven1(insert_row):
    def _seed(**overrides):
        row = {
            "item_number": "ABC123",
            "description": "Default Item",
            "brand_name": "Default Brand",
        }
        row.update(overrides)
        insert_row("inven1", row)

    return _seed


@pytest.fixture
def seed_shipt1(insert_row):
    def _seed(**overrides):
        row = {
            "customer_number": "000123",
            "ship_to_number": "01",
            "ship_to_name": "Default Ship To",
            "city": "Buffalo",
        }
        row.update(overrides)
        insert_row("shipt1", row)

    return _seed


@pytest.fixture
def assert_row_count(fetch_val):
    def _assert(table: str, where_sql: str, params: dict, expected: int):
        count = fetch_val(
            f"SELECT COUNT(*) FROM {table} WHERE {where_sql}",
            params,
        )
        assert count == expected

    return _assert


@pytest.fixture(autouse=True)
def clean_tables(engine: Engine) -> Generator[None, None, None]:
    existing_tables = set(inspect(engine).get_table_names())

    with engine.begin() as conn:
        for table in reversed(TABLES_TO_CLEAN):
            if table in existing_tables:
                conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))

    yield

    existing_tables = set(inspect(engine).get_table_names())

    with engine.begin() as conn:
        for table in reversed(TABLES_TO_CLEAN):
            if table in existing_tables:
                conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
