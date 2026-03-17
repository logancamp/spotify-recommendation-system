import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd

# Loads .env from the current working directory (repo root) by default
load_dotenv()

def get_engine():
    """
    Creates a SQLAlchemy engine using DB_* values from .env

    Required .env fields:
      DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PASSWORD")

    missing = [k for k, v in {
        "DB_HOST": host,
        "DB_PORT": port,
        "DB_NAME": name,
        "DB_USER": user,
        "DB_PASSWORD": pwd
    }.items() if not v]

    if missing:
        raise RuntimeError(f"Missing DB values in .env: {missing}")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
    return create_engine(url, pool_pre_ping=True)

def read_df(sql: str):
    """
    Run SQL query -> return pandas DataFrame.

    We intentionally DO NOT use pandas.read_sql() here because in some
    environments pandas attempts to call DBAPI cursor() methods on
    SQLAlchemy objects. This manual approach is stable.
    """
    eng = get_engine()
    with eng.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)
