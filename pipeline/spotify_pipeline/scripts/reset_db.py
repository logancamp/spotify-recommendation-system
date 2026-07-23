import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

url = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(url, pool_pre_ping=True)

with open("sql/reset.sql", "r") as f:
    ddl = f.read()

with engine.begin() as conn:
    conn.execute(text(ddl))

print("✅ Database cleared (tables truncated).")
