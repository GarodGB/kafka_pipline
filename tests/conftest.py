"""Shared helpers for the tests: one database connection + the config."""
import json
import os
from dotenv import load_dotenv
load_dotenv()

import psycopg2
import pytest

DB = dict(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5433"),
          dbname=os.getenv("PGDATABASE", "transactions"),
          user=os.getenv("PGUSER", "admin"), password=os.environ["PGPASSWORD"])


@pytest.fixture(scope="session")
def cfg():
    with open("config/pipeline_config.json") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def conn():
    c = psycopg2.connect(**DB)
    yield c
    c.close()


@pytest.fixture
def q(conn):
    """q("SELECT ...") -> first value of the first row."""
    def run(sql, params=None):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    yield run
    conn.rollback()
