# from sqlalchemy import create_engine
# engine = create_engine("postgresql+psycopg2://user:pass@postgres:5432/rmb")

import os
from sqlalchemy import create_engine

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)
