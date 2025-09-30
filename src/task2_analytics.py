from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from typing import Dict, Iterable, List, Sequence, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import text

from .config import ConfigError, get_database_config, load_environment
from .db_utils import PLAYERS_TABLE, SCHEMA_NAME, create_db_engine
from .youtube_pubsub import CommentPublisher, CommentSubscriber


def fetch_all(query: str, params: Dict[str, object] | None = None) -> List[Dict[str, object]]:
    """Execute a SQL query against PostgreSQL and return a list of dictionaries."""

    db_config = get_database_config()
    engine = create_db_engine(db_config.url)
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return [dict(row._mapping) for row in result]


def top_clubs_with_expiring_contracts(
    *, year: int, top: int, contract_year: int
) -> List[Dict[str, object]]:
    if top < 1:
        raise ValueError("`top` must be a positive integer")

    query = f"""
        WITH filtered AS (
            SELECT club_name, COUNT(*) AS player_count
            FROM "{SCHEMA_NAME}"."{PLAYERS_TABLE}"
            WHERE year = :year
              AND contract_valid_until IS NOT NULL
              AND contract_valid_until >= :contract_year
              AND club_name IS NOT NULL
            GROUP BY club_name
        ), ranked AS (
            SELECT
                club_name,
                player_count,
                DENSE_RANK() OVER (ORDER BY player_count DESC, club_name) AS rank
            FROM filtered
        )
        SELECT club_name, player_count, rank
        FROM ranked
        WHERE rank <= :top
        ORDER BY rank, club_name
    """
    return fetch_all(query, {"year": year, "contract_year": contract_year, "top": top})


def rank_clubs_by_average_age(
    *, year: int, top: int, order: str = "desc"
) -> List[Dict[str, object]]:
    if top < 1:
        raise ValueError("`top` must be a positive integer")
    direction = order.lower()
    if direction not in {"asc", "desc"}:
        raise ValueError("order must be either 'asc' or 'desc'")

    ordering = "ASC" if direction == "asc" else "DESC"
    query = f"""
        WITH averages AS (
            SELECT club_name, AVG(age)::NUMERIC(10,2) AS average_age
            FROM "{SCHEMA_NAME}"."{PLAYERS_TABLE}"
            WHERE year = :year
              AND age IS NOT NULL
              AND club_name IS NOT NULL
            GROUP BY club_name
        ), ranked AS (
            SELECT
                club_name,
                average_age,
                DENSE_RANK() OVER (ORDER BY average_age {ordering}, club_name) AS rank
            FROM averages
        )
        SELECT club_name, average_age, rank
        FROM ranked
        WHERE rank <= :top
        ORDER BY rank, club_name
    """
    return fetch_all(query, {"year": year, "top": top})


def most_popular_nationality_by_year() -> List[Dict[str, object]]:
    query = f"""
        WITH counts AS (
            SELECT year, nationality_name, COUNT(*) AS player_count
            FROM "{SCHEMA_NAME}"."{PLAYERS_TABLE}"
            WHERE nationality_name IS NOT NULL
            GROUP BY year, nationality_name
        ), ranked AS (
            SELECT
                year,
                nationality_name,
                player_count,
                ROW_NUMBER() OVER (
                    PARTITION BY year
                    ORDER BY player_count DESC, nationality_name
                ) AS position
            FROM counts
        )
        SELECT year, nationality_name, player_count
        FROM ranked
        WHERE position = 1
        ORDER BY year
    """
    return fetch_all(query)


def nationality_histogram(*, output_path: str | None = None) -> Dict[str, int]:
    query = f"""
        WITH distinct_players AS (
            SELECT DISTINCT ON (sofifa_id)
                sofifa_id,
                nationality_name
            FROM "{SCHEMA_NAME}"."{PLAYERS_TABLE}"
            WHERE sofifa_id IS NOT NULL
              AND nationality_name IS NOT NULL
            ORDER BY sofifa_id, year DESC
        )
        SELECT nationality_name, COUNT(*) AS player_count
        FROM distinct_players
        GROUP BY nationality_name
        ORDER BY player_count DESC, nationality_name
    """
    rows = fetch_all(query)
    histogram = {row["nationality_name"]: row["player_count"] for row in rows}

    if histogram:
        plt.figure(figsize=(12, 6))
        plt.bar(list(histogram.keys()), list(histogram.values()))
        plt.xticks(rotation=90)
        plt.ylabel("Players (deduplicated)")
        plt.title("Unique players per nationality")
        plt.tight_layout()
        if output_path:
            plt.savefig(output_path, dpi=200)
        else:
            plt.show()
    return histogram


def build_alias_map(rows: Sequence[Dict[str, object]]) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}
    for row in rows:
        short_name = (row.get("short_name") or "").strip()
        if not short_name:
            continue
        canonical = short_name
        alias_map[canonical.lower()] = canonical
        long_name = (row.get("long_name") or "").strip()
        if long_name:
            alias_map[long_name.lower()] = canonical
    return alias_map


def fetch_alias_rows() -> List[Dict[str, object]]:
    query = f"""
        SELECT short_name, long_name
        FROM "{SCHEMA_NAME}"."{PLAYERS_TABLE}"
        WHERE year = 2022 AND gender = 'male'
    """
    return fetch_all(query)


def find_most_discussed_player(
    *, api_key: str, limit: int = 500, dump_path: str | None = None
) -> Tuple[str, Counter]:
    rows = fetch_alias_rows()
    alias_map = build_alias_map(rows)
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
    parser = argparse.ArgumentParser(description="Task II PostgreSQL analytics")
    subparsers = parser.add_subparsers(dest="command", required=True)

    contracts = subparsers.add_parser("contracts", help="Top clubs with expiring contracts")
    contracts.add_argument("--year", type=int, required=True)
    contracts.add_argument("--top", type=int, required=True)
    contracts.add_argument("--contract-year", type=int, required=True)

    age = subparsers.add_parser("age", help="Rank clubs by average age")
    age.add_argument("--year", type=int, required=True)
    age.add_argument("--top", type=int, required=True)
    age.add_argument("--order", type=str, default="desc")

    subparsers.add_parser(
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


def print_rows(rows: Sequence[Dict[str, object]]) -> None:
    if not rows:
        print("No results found.")
        return
    headers = list(rows[0].keys())
    widths = [max(len(str(row.get(col, ""))) for row in rows + [dict(zip(headers, headers))]) for col in headers]
    header_line = " | ".join(col.ljust(width) for col, width in zip(headers, widths))
    separator = "-+-".join("-" * width for width in widths)
    print(header_line)
    print(separator)
    for row in rows:
        print(" | ".join(str(row.get(col, "")).ljust(width) for col, width in zip(headers, widths)))


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)

    if args.command == "contracts":
        rows = top_clubs_with_expiring_contracts(
            year=args.year, top=args.top, contract_year=args.contract_year
        )
        print_rows(rows)
    elif args.command == "age":
        rows = rank_clubs_by_average_age(year=args.year, top=args.top, order=args.order)
        print_rows(rows)
    elif args.command == "nationality":
        rows = most_popular_nationality_by_year()
        print_rows(rows)
    elif args.command == "histogram":
        histogram_data = nationality_histogram(output_path=args.output)
        print(json.dumps(histogram_data, indent=2))
    elif args.command == "youtube":
        load_environment()
        api_key = os.getenv("YOUTUBE_API_KEY")
        if not api_key:
            raise ConfigError("YOUTUBE_API_KEY is not configured.")
        top_player, counts = find_most_discussed_player(
            api_key=api_key, limit=args.limit, dump_path=args.dump_path
        )
        summary = {
            "player": top_player,
            "mentions": counts[top_player],
            "counts": dict(counts),
        }
        print(json.dumps(summary, indent=2))
    else:
        raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
