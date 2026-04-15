import pandas as pd
from db import engine


def import_file(path):
    df = pd.read_csv(path)

    df.to_sql("order_header_222", con=engine, if_exists="append", index=False)

    return len(df)
