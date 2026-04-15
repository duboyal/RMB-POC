import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

cur = conn.cursor()

cur.execute(
    """
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    message TEXT
)
"""
)

cur.execute("INSERT INTO test_table (message) VALUES (%s)", ("hello from local code",))

conn.commit()
print("Insert worked")

cur.close()
conn.close()


#### old code basically the same

# import os
# from dotenv import load_dotenv
# import psycopg2

# load_dotenv()

# conn = psycopg2.connect(
#     host=os.getenv("DB_HOST"),
#     port=os.getenv("DB_PORT"),
#     dbname=os.getenv("DB_NAME"),
#     user=os.getenv("DB_USER"),
#     password=os.getenv("DB_PASSWORD"),
# )

# cur = conn.cursor()

# cur.execute("SELECT current_database(), current_user;")
# print("Connected:", cur.fetchall())

# cur.close()
# conn.close()
