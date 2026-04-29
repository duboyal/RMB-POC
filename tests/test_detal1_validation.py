import psycopg2
from dotenv import load_dotenv

from app.duckdb_pipeline.config import POSTGRES_URL

load_dotenv()


def test_detal1_table_exists_and_has_rows():
    conn = psycopg2.connect(POSTGRES_URL)
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "detal1"')
            count = cur.fetchone()[0]

        assert count > 0
    finally:
        conn.close()