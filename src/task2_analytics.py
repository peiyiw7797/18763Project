from __future__ import annotations

import argparse
import json
from collections import Counter
import os
from typing import Dict, Iterable, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from sqlalchemy.engine import make_url

from .config import ConfigError, get_database_config
from .db_utils import PLAYERS_TABLE, SCHEMA_NAME
from .youtube_pubsub import CommentPublisher, CommentSubscriber

SPARK_APP_NAME = "FifaAnalytics"
POSTGRES_DRIVER = "org.postgresql.Driver"
POSTGRES_JAR = "org.postgresql:postgresql:42.7.3"


def build_spark_session(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """Create (or reuse) a Spark session configured for JDBC access."""

    builder = SparkSession.builder.appName(app_name)
    builder = builder.config("spark.jars.packages", POSTGRES_JAR)
    return builder.getOrCreate()


def load_players_frame(spark: SparkSession) -> DataFrame:
    """Load the unified players table from PostgreSQL via JDBC."""

    db_config = get_database_config()
    url = make_url(db_config.url)
    if url.drivername.split("+")[0] != "postgresql":
        raise ConfigError("Only PostgreSQL connection URLs are supported.")

    host = url.host or "localhost"
    port = url.port or 5432
    database = url.database
    if not database:
        raise ConfigError("Database name missing from FIFA_DB_URL.")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    properties = {
        "user": url.username or "",
        "password": url.password or "",
        "driver": POSTGRES_DRIVER,
        "currentSchema": SCHEMA_NAME,
    }
    return spark.read.format("jdbc").options(
        url=jdbc_url,
        dbtable=f'"{SCHEMA_NAME}"."{PLAYERS_TABLE}"',
    ).options(**properties).load()


def top_clubs_with_expiring_contracts(
    frame: DataFrame, *, year: int, top: int, contract_year: int
) -> DataFrame:
    if top < 1:
        raise ValueError("`top` must be a positive integer")

    filtered = frame.filter(
        (F.col("year") == year) & (F.col("contract_valid_until") >= contract_year)
    )
    grouped = (
        filtered.groupBy("club_name")
        .agg(F.count("*").alias("player_count"))
        .filter(F.col("player_count") > 0)
    )
    window = Window.orderBy(F.col("player_count").desc(), F.col("club_name"))
    ranked = grouped.withColumn("rank", F.dense_rank().over(window))
    return ranked.filter(F.col("rank") <= top).orderBy("rank", F.col("club_name"))


def rank_clubs_by_average_age(
    frame: DataFrame, *, year: int, top: int, order: str = "desc"
) -> DataFrame:
    if top < 1:
        raise ValueError("`top` must be a positive integer")
    direction = order.lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("order must be either 'asc' or 'desc'")

    filtered = frame.filter((F.col("year") == year) & F.col("age").isNotNull())
    grouped = filtered.groupBy("club_name").agg(F.avg("age").alias("average_age"))
    window = Window.orderBy(
        F.col("average_age").asc() if direction == "asc" else F.col("average_age").desc(),
        F.col("club_name"),
    )
    ranked = grouped.withColumn("rank", F.dense_rank().over(window))
    ordering = ["rank", F.col("club_name")]
    return ranked.filter(F.col("rank") <= top).orderBy(*ordering)


def most_popular_nationality_by_year(frame: DataFrame) -> DataFrame:
    aggregated = (
        frame.groupBy("year", "nationality_name")
        .agg(F.count("*").alias("player_count"))
    )
    window = Window.partitionBy("year").orderBy(F.col("player_count").desc())
    ranked = aggregated.withColumn("rank", F.row_number().over(window))
    return ranked.filter(F.col("rank") == 1).orderBy("year")


def nationality_histogram(
    frame: DataFrame, *, output_path: str | None = None
) -> Dict[str, int]:
    unique_players = frame.dropna(subset=["sofifa_id"]).dropDuplicates(["sofifa_id"])
    histogram = (
        unique_players.groupBy("nationality_name")
        .agg(F.count("*").alias("player_count"))
        .orderBy(F.col("player_count").desc())
    )
    pdf = histogram.toPandas()
    plt.figure(figsize=(12, 6))
    plt.bar(pdf["nationality_name"], pdf["player_count"])
    plt.xticks(rotation=90)
    plt.ylabel("Players (deduplicated)")
    plt.title("Unique players per nationality")
    plt.tight_layout()
    if output_path:
        plt.savefig(output_path, dpi=200)
    else:
        plt.show()
    return dict(zip(pdf["nationality_name"], pdf["player_count"]))


def build_alias_map(frame: DataFrame) -> Dict[str, str]:
    """Prepare a mapping of aliases to canonical player names."""

    rows = frame.select("short_name", "long_name").dropna(subset=["short_name"]).collect()
    alias_map: Dict[str, str] = {}
    for row in rows:
        canonical = row["short_name"].strip()
        alias_map[canonical.lower()] = canonical
        long_name = row["long_name"]
        if long_name and long_name.strip():
            alias_map[long_name.lower()] = canonical
    return alias_map


def find_most_discussed_player(
    frame: DataFrame,
    *,
    api_key: str,
    limit: int = 500,
    dump_path: str | None = None,
) -> Tuple[str, Counter]:
    roster_2022 = frame.filter((F.col("year") == 2022) & (F.col("gender") == "male"))
    alias_map = build_alias_map(roster_2022)
    if not alias_map:
        raise RuntimeError("No players found for 2022 male roster.")

    publisher = CommentPublisher(api_key=api_key)
    subscriber = CommentSubscriber(alias_map=alias_map, dump_path=dump_path)
    counts = subscriber.process(publisher.iter_comments(limit=limit))
    if not counts:
        raise RuntimeError("No player mentions detected in the sampled comments.")
    top_player, _ = counts.most_common(1)[0]
    return top_player, counts


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Task II Spark analytics")
    subparsers = parser.add_subparsers(dest="command", required=True)

    contracts = subparsers.add_parser("contracts", help="Top clubs with expiring contracts")
    contracts.add_argument("--year", type=int, required=True)
    contracts.add_argument("--top", type=int, required=True)
    contracts.add_argument("--contract-year", type=int, required=True)

    age = subparsers.add_parser("age", help="Rank clubs by average age")
    age.add_argument("--year", type=int, required=True)
    age.add_argument("--top", type=int, required=True)
    age.add_argument("--order", type=str, default="desc")

    nationality = subparsers.add_parser(
        "nationality", help="Most popular nationality per year"
    )

    histogram = subparsers.add_parser(
        "histogram", help="Generate nationality histogram"
    )
    histogram.add_argument("--output", type=str, default=None)

    youtube = subparsers.add_parser(
        "youtube", help="Find the most discussed player on YouTube"
    )
    youtube.add_argument("--limit", type=int, default=500)
    youtube.add_argument("--dump-path", type=str, default=None)

    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    spark = build_spark_session()
    frame = load_players_frame(spark)

    if args.command == "contracts":
        result = top_clubs_with_expiring_contracts(
            frame, year=args.year, top=args.top, contract_year=args.contract_year
        )
        result.show(truncate=False)
    elif args.command == "age":
        result = rank_clubs_by_average_age(
            frame, year=args.year, top=args.top, order=args.order
        )
        result.show(truncate=False)
    elif args.command == "nationality":
        result = most_popular_nationality_by_year(frame)
        result.show(truncate=False)
    elif args.command == "histogram":
        histogram_data = nationality_histogram(frame, output_path=args.output)
        print(json.dumps(histogram_data, indent=2))
    elif args.command == "youtube":
        from .config import load_environment

        load_environment()
        api_key = os.getenv('YOUTUBE_API_KEY')
        if not api_key:
            raise ConfigError('YOUTUBE_API_KEY is not configured.')
        top_player, counts = find_most_discussed_player(
            frame, api_key=api_key, limit=args.limit, dump_path=args.dump_path
        )
        summary = {
            'player': top_player,
            'mentions': counts[top_player],
            'counts': dict(counts),
        }
        print(json.dumps(summary, indent=2))
    else:
        raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
