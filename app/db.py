from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://user:pass@postgres:5432/rmb")
