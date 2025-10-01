from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, Dict

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SCHEMA_NAME = "fifa"
PLAYERS_TABLE = "players"
INGESTION_LOG_TABLE = "ingestion_log"


def create_db_engine(url: str) -> Engine:
    """Create a SQLAlchemy engine using the provided connection URL."""

    return create_engine(url, future=True)


def ensure_schema(engine: Engine) -> None:
    """Create the target schema."""

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))


def _iter_spark_frames(
    dataframe: SparkDataFrame | Iterable[SparkDataFrame],
) -> Iterable[SparkDataFrame]:
    if isinstance(dataframe, SparkDataFrame):
        yield dataframe
        return
    for chunk in dataframe:
        if not isinstance(chunk, SparkDataFrame):
            raise TypeError(
                "write_players_table expected Spark DataFrames within the iterable."
            )
        yield chunk


def _build_jdbc_options(engine: Engine) -> tuple[str, Dict[str, str]]:
    url = engine.url
    backend = url.get_backend_name()
    if backend != "postgresql":
        raise ValueError(
            f"Unsupported database backend '{backend}' for Spark JDBC ingestion."
        )

    host = url.host or "localhost"
    port = url.port or 5432
    if not url.database:
        raise ValueError("Database name must be provided for JDBC ingestion.")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{url.database}"
    query_params = dict(url.query or {})
    query_params.setdefault("stringtype", "unspecified")
    if query_params:
        query = "&".join(f"{key}={value}" for key, value in query_params.items())
        jdbc_url = f"{jdbc_url}?{query}"

    options: Dict[str, str] = {"driver": "org.postgresql.Driver", "currentSchema": SCHEMA_NAME}
    if url.username:
        options["user"] = url.username
    if url.password:
        options["password"] = url.password

    return jdbc_url, options


def write_players_table(
    engine: Engine, dataframe: SparkDataFrame | Iterable[SparkDataFrame]
) -> None:
    """Write player data to the target table using Spark JDBC"""

    ensure_schema(engine)
    jdbc_url, jdbc_options = _build_jdbc_options(engine)
    dbtable = f"{SCHEMA_NAME}.{PLAYERS_TABLE}"

    first = True
    wrote_any = False
    for chunk in _iter_spark_frames(dataframe):
        wrote_any = True
        writer = chunk.write.format("jdbc").option("url", jdbc_url).option(
            "dbtable", dbtable
        )
        for key, value in jdbc_options.items():
            writer = writer.option(key, value)

        if first:
            writer = writer.mode("overwrite").option("truncate", "true")
        else:
            writer = writer.mode("append")
        writer.save()
        first = False

    if not wrote_any:
        raise ValueError("write_players_table received no data frames to persist.")


def ensure_ingestion_log(engine: Engine) -> None:
    """Create the ingestion log table."""

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
    spark: SparkSession,
    *,
    row_count: int,
    years: list[int],
    source_files: Dict[str, Dict[str, Any]],
    checksum: str,
) -> None:
    """Insert a new ingestion record using Spark JDBC."""

    ensure_ingestion_log(engine)
    jdbc_url, jdbc_options = _build_jdbc_options(engine)
    dbtable = f"{SCHEMA_NAME}.{INGESTION_LOG_TABLE}"

    payload = {
        "row_count": int(row_count),
        "years": list(map(int, years)),
        "source_files": json.dumps(source_files),
        "checksum": str(checksum),
    }

    schema = StructType(
        [
            StructField("row_count", IntegerType(), False),
            StructField("years", ArrayType(IntegerType(), False), False),
            StructField("source_files", StringType(), False),
            StructField("checksum", StringType(), False),
        ]
    )
    dataframe = spark.createDataFrame([Row(**payload)], schema=schema)

    writer = dataframe.write.format("jdbc").mode("append").option("url", jdbc_url).option(
        "dbtable", dbtable
    )
    for key, value in jdbc_options.items():
        writer = writer.option(key, value)
    writer.save()


__all__ = [
    "SCHEMA_NAME",
    "PLAYERS_TABLE",
    "INGESTION_LOG_TABLE",
    "create_db_engine",
    "ensure_schema",
    "write_players_table",
    "log_ingestion",
]
