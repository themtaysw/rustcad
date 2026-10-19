import os

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql

from receiver.app import create_app

ADMIN_URL = os.environ.get(
    "TEST_DATABASE_ADMIN_URL", "postgresql://telemetry:telemetry@127.0.0.1:55432/telemetry"
)
TEST_DB = "telemetry_test"
TOKEN = "test-token"


@pytest.fixture(scope="session")
def database_url() -> str:
    """A separate database on the same server, so tests never touch dev data.

    Recreated every session: schema.sql only creates missing tables, so an old test
    database would keep an outdated schema.
    """
    db = sql.Identifier(TEST_DB)
    try:
        with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=3) as conn:
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(db))
            conn.execute(sql.SQL("CREATE DATABASE {}").format(db))
    except psycopg.OperationalError as e:
        pytest.fail(
            f"cannot reach Postgres at {ADMIN_URL} ({e}). "
            "Start it with: docker compose -f cloud/docker-compose.yml up -d postgres"
        )
    return psycopg.conninfo.make_conninfo(ADMIN_URL, dbname=TEST_DB)


@pytest.fixture
def client(database_url):
    with TestClient(create_app(database_url=database_url, token=TOKEN)) as client:
        with psycopg.connect(database_url) as conn:
            conn.execute("TRUNCATE telemetry, batches")
        client.headers["Authorization"] = f"Bearer {TOKEN}"
        yield client


@pytest.fixture
def db(database_url):
    with psycopg.connect(database_url, autocommit=True) as conn:
        yield conn
