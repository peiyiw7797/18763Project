from __future__ import annotations

import json
from typing import Any, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SCHEMA_NAME = "fifa"
PLAYERS_TABLE = "players"
INGESTION_LOG_TABLE = "ingestion_log"


def create_db_engine(url: str) -> Engine:
    """Create a SQLAlchemy engine using the provided connection URL."""

    return create_engine(url, future=True)


def ensure_schema(engine: Engine) -> None:
    """Create the target schema if it does not exist."""

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))


def write_players_table(engine: Engine, dataframe: pd.DataFrame) -> None:
    """Replace the `fifa.players` table with the supplied dataframe."""

    ensure_schema(engine)
    dataframe.to_sql(
        PLAYERS_TABLE,
        engine,
        schema=SCHEMA_NAME,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )


def ensure_ingestion_log(engine: Engine) -> None:
    """Ensure the ingestion log table exists."""

    ensure_schema(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}"."{INGESTION_LOG_TABLE}" (
                    ingestion_id SERIAL PRIMARY KEY,
                    ingested_at TIMESTAMPTZ DEFAULT NOW(),
                    row_count INTEGER NOT NULL,
                    years INTEGER[] NOT NULL,
                    source_files JSONB NOT NULL,
                    checksum TEXT NOT NULL
                )
                """
            )
        )


def log_ingestion(
    engine: Engine,
    *,
    row_count: int,
    years: list[int],
    source_files: Dict[str, Dict[str, Any]],
    checksum: str,
) -> None:
    """Insert a new ingestion record."""

    ensure_ingestion_log(engine)
    payload = {
        "row_count": row_count,
        "years": years,
        "source_files": json.dumps(source_files),
        "checksum": checksum,
    }
    with engine.begin() as conn:
        conn.execute(
            text(
                f'INSERT INTO "{SCHEMA_NAME}"."{INGESTION_LOG_TABLE}" '
                "(row_count, years, source_files, checksum) "
                "VALUES (:row_count, :years, :source_files::jsonb, :checksum)"
            ),
            payload,
        )


__all__ = [
    "SCHEMA_NAME",
    "PLAYERS_TABLE",
    "INGESTION_LOG_TABLE",
    "create_db_engine",
    "ensure_schema",
    "write_players_table",
    "log_ingestion",
]
