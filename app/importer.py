import pandas as pd
from db import engine
import re


def import_file(path):

    df = pd.read_csv(path)

    # sanitize table name
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", path.stem).lower()

    print(f"Importing into table: {table_name}")

    df.to_sql(table_name, con=engine, if_exists="append", index=False)

    return len(df)


# def import_file(path):
#     df = pd.read_csv(path)
#     df.to_sql("order_header", con=engine, if_exists="append", index=False)
#     return len(df)
