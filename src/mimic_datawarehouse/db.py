"""RDS connection helpers."""

from __future__ import annotations

from dataclasses import replace

import boto3
import psycopg2
from sqlalchemy import Engine, create_engine, text

from .config import Settings


def make_engine(settings: Settings) -> Engine:
    if settings.auth_mode == "iam":
        return _make_iam_engine(settings)
    return create_engine(settings.sqlalchemy_url)


def _make_iam_engine(settings: Settings) -> Engine:
    """Build an engine that authenticates with a freshly-generated IAM auth token on
    every new physical connection (tokens expire after 15 minutes, so this can't be
    generated once and reused for the engine's lifetime)."""
    rds_client = boto3.client("rds", region_name=settings.region)

    def _creator():
        token = rds_client.generate_db_auth_token(
            DBHostname=settings.rds_host,
            Port=settings.rds_port,
            DBUsername=settings.rds_user,
        )
        return psycopg2.connect(
            host=settings.rds_host,
            port=settings.rds_port,
            dbname=settings.rds_database,
            user=settings.rds_user,
            password=token,
            sslmode="require",
        )

    return create_engine("postgresql+psycopg2://", creator=_creator)


def make_engine_from_params(host: str, port: int, dbname: str, user: str, password: str) -> Engine:
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")


def ensure_database(settings: Settings) -> bool:
    """Create `settings.rds_database` if it doesn't already exist. Returns True if it
    was created. Connects to the `postgres` maintenance database to do so, since
    CREATE DATABASE can't run against the database being created, or inside a
    transaction."""
    bootstrap_settings = replace(settings, rds_database="postgres")
    engine = make_engine(bootstrap_settings)
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": settings.rds_database},
            ).first()
            if exists:
                return False
            conn.execute(text(f'CREATE DATABASE "{settings.rds_database}"'))
            return True
    finally:
        engine.dispose()


def ensure_schemas(engine: Engine, schemas: list[str]) -> None:
    with engine.begin() as conn:
        for schema in schemas:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
