import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

if not all([db_host, db_port, db_name, db_user, db_password]):
    raise SystemExit("Missing one or more DB_* values in .env")

url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(url, pool_pre_ping=True)

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1 AS ok;")).fetchone()
    print("✅ Postgres connection OK. SELECT returned:", dict(result._mapping))
