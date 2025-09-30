from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Iterator, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SCHEMA_NAME = "fifa"
PLAYERS_TABLE = "players"
INGESTION_LOG_TABLE = "ingestion_log"

PLAYER_COLUMNS = [
    "player_uid",
    "year",
    "gender",
    "sofifa_id",
    "short_name",
    "long_name",
    "club_name",
    "league_name",
    "contract_valid_until",
    "age",
    "nationality_name",
    "player_positions",
    "overall",
    "potential",
    "value_eur",
    "wage_eur",
    "release_clause_eur",
    "height_cm",
    "weight_kg",
    "extra_attributes",
]


def create_db_engine(url: str) -> Engine:
    """Create a SQLAlchemy engine using the provided connection URL."""

    return create_engine(url, future=True)


def ensure_schema(engine: Engine) -> None:
    """Create the target schema if it does not exist."""

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))


def ensure_players_table(engine: Engine) -> None:
    """Create the unified players table and supporting indexes if needed."""

    ensure_schema(engine)
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}"."{PLAYERS_TABLE}" (
            player_uid TEXT PRIMARY KEY,
            year INTEGER NOT NULL,
            gender TEXT NOT NULL,
            sofifa_id BIGINT,
            short_name TEXT,
            long_name TEXT,
            club_name TEXT,
            league_name TEXT,
            contract_valid_until INTEGER,
            age INTEGER,
            nationality_name TEXT,
            player_positions TEXT,
            overall INTEGER,
            potential INTEGER,
            value_eur BIGINT,
            wage_eur BIGINT,
            release_clause_eur BIGINT,
            height_cm INTEGER,
            weight_kg INTEGER,
            extra_attributes JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS idx_players_year ON "{SCHEMA_NAME}"."{PLAYERS_TABLE}" (year)'
            )
        )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS idx_players_club ON "{SCHEMA_NAME}"."{PLAYERS_TABLE}" (club_name)'
            )
        )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS idx_players_nationality ON "{SCHEMA_NAME}"."{PLAYERS_TABLE}" (nationality_name)'
            )
        )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS idx_players_sofifa ON "{SCHEMA_NAME}"."{PLAYERS_TABLE}" (sofifa_id)'
            )
        )


def _chunked(iterable: Iterable[Dict[str, Any]], size: int) -> Iterator[List[Dict[str, Any]]]:
    chunk: List[Dict[str, Any]] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def write_players_table(engine: Engine, records: Iterable[Dict[str, Any]]) -> None:
    """Replace the `fifa.players` table content with the supplied records."""

    ensure_players_table(engine)
    insert_sql = text(
        f"""
        INSERT INTO "{SCHEMA_NAME}"."{PLAYERS_TABLE}" (
            {', '.join('"' + col + '"' for col in PLAYER_COLUMNS)}
        ) VALUES (
            {', '.join(':' + col for col in PLAYER_COLUMNS[:-1])},
            :extra_attributes::jsonb
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            text(f'TRUNCATE TABLE "{SCHEMA_NAME}"."{PLAYERS_TABLE}" RESTART IDENTITY')
        )
        formatted_records = (
            {
                **{key: record.get(key) for key in PLAYER_COLUMNS if key != "extra_attributes"},
                "extra_attributes": json.dumps(
                    record.get("extra_attributes", {}),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            }
            for record in records
        )
        for batch in _chunked(formatted_records, 1000):
            conn.execute(insert_sql, batch)


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
    years: List[int],
    source_files: Dict[str, Dict[str, Any]],
    checksum: str,
) -> None:
    """Insert a new ingestion record."""

    ensure_ingestion_log(engine)
    payload = {
        "row_count": row_count,
        "years": years,
        "source_files": json.dumps(source_files, sort_keys=True),
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
    "PLAYER_COLUMNS",
    "create_db_engine",
    "ensure_schema",
    "ensure_players_table",
    "write_players_table",
    "ensure_ingestion_log",
    "log_ingestion",
]
