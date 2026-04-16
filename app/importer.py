import pandas as pd
import re
from datetime import datetime


def import_file(path):
    df = pd.read_csv(path, sep="\t")  #  IMPORTANT (your files are tab-separated)

    # clean table name
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", path.stem).lower()

    print(f"Importing into table: {table_name}")

    # timestamps
    now = datetime.utcnow()
    df["created_at"] = now
    df["updated_at"] = now

    df.to_sql(
        table_name, con=engine, if_exists="append", index=False  # keep append for now
    )

    return len(df)


# def import_file(path):
#     df = pd.read_csv(path)
#     df.to_sql("order_header", con=engine, if_exists="append", index=False)
#     return len(df)
