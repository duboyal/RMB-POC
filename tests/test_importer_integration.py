import pandas as pd
from pathlib import Path

from importer import import_file


def test_cust1_insert(tmp_path, engine, fetch_one):
    file_path = tmp_path / "cust1.csv"

    df = pd.DataFrame(
        [
            {
                "CUSTOMER NUMBER": "000123",
                "CUSTOMER NAME": "Acme Foods",
                "CITY": "Buffalo",
                "STATE": "NY",
            }
        ]
    )
    df.to_csv(file_path, sep="|", index=False)

    rows_imported = import_file(file_path, engine=engine)

    assert rows_imported == 1

    row = fetch_one(
        """
        SELECT "CUSTOMER NUMBER", "CUSTOMER NAME", "CITY", "STATE"
        FROM cust1
        WHERE "CUSTOMER NUMBER" = :customer_number
        """,
        {"customer_number": "000123"},
    )

    assert row is not None
    assert row["CUSTOMER NUMBER"] == "000123"
    assert row["CUSTOMER NAME"] == "Acme Foods"
    assert row["CITY"] == "Buffalo"
    assert row["STATE"] == "NY"
