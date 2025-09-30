from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, Tuple

import pandas as pd

from .config import ConfigError, get_database_config, load_file_map
from .db_utils import create_db_engine, log_ingestion, write_players_table

EXPECTED_COLUMNS = {
    "sofifa_id",
    "short_name",
    "long_name",
    "player_positions",
    "age",
    "height_cm",
    "weight_kg",
    "nationality_name",
    "club_name",
    "league_name",
    "overall",
    "potential",
    "value_eur",
    "wage_eur",
    "release_clause_eur",
    "contract_valid_until",
}


COLUMN_ALIASES = {
    "nationality": "nationality_name",
    "nationality_name": "nationality_name",
    "team_jersey_number": "club_jersey_number",
    "club": "club_name",
    "club_contract_valid_until": "contract_valid_until",
    "contract": "contract_valid_until",
    "contract_end_year": "contract_valid_until",
}


def normalize_column(name: str) -> str:
    """Convert column headers to snake_case."""

    cleaned = name.strip().lower()
    cleaned = cleaned.replace("%", "_pct").replace("/", "_")
    cleaned = cleaned.replace("(", "_").replace(")", "")
    cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in cleaned)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def hash_file(path: Path, chunk_size: int = 65536) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    """Load a CSV file with pandas, preserving column types when possible."""

    return pd.read_csv(path, low_memory=False)


def standardize_dataframe(
    frame: pd.DataFrame, *, year: int, gender: str
) -> pd.DataFrame:
    """Apply column normalization and add metadata fields."""

    rename_map = {col: normalize_column(col) for col in frame.columns}
    frame = frame.rename(columns=rename_map)
    frame = frame.rename(columns=COLUMN_ALIASES)

    if "nationality_name" not in frame.columns and "nationality" in frame.columns:
        frame = frame.rename(columns={"nationality": "nationality_name"})

    frame["year"] = year
    frame["gender"] = gender

    if "sofifa_id" not in frame.columns:
        if "player_url" in frame.columns:
            frame["sofifa_id"] = (
                frame["player_url"].astype(str).str.extract(r"(\d+)", expand=False)
            )
        else:
            raise ConfigError(
                "Missing `sofifa_id` column and unable to infer it from player_url."
            )
    frame["sofifa_id"] = frame["sofifa_id"].astype("Int64")

    if "short_name" not in frame.columns and "name" in frame.columns:
        frame["short_name"] = frame["name"]

    if "long_name" not in frame.columns and "full_name" in frame.columns:
        frame["long_name"] = frame["full_name"]

    if "contract_valid_until" in frame.columns:
        frame["contract_valid_until"] = frame["contract_valid_until"].astype("Int64")
    else:
        frame["contract_valid_until"] = pd.NA

    if "age" in frame.columns:
        frame["age"] = frame["age"].astype("Int64")

    frame["player_uid"] = (
        frame["gender"].str[0].str.upper()
        + "_"
        + frame["year"].astype(str)
        + "_"
        + frame["sofifa_id"].astype(str)
    )
    frame = frame.drop_duplicates(subset=["player_uid"], keep="last")

    # Ensure all expected columns exist
    for column in EXPECTED_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    ordered_columns = [
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
    ] + sorted(col for col in frame.columns if col not in {
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
    })
    frame = frame[ordered_columns]
    return frame.reset_index(drop=True)


def load_players(data_dir: Path, file_map: Dict[str, Dict[int, str]]) -> Tuple[pd.DataFrame, Dict[str, Dict[str, str]]]:
    """Read all configured CSV files and return a unified dataframe plus metadata."""

    frames: list[pd.DataFrame] = []
    metadata: Dict[str, Dict[str, str]] = {}

    for group, entries in file_map.items():
        gender = "female" if group.lower().startswith("f") else "male"
        for year, filename in sorted(entries.items()):
            csv_path = data_dir / filename
            if not csv_path.exists():
                raise FileNotFoundError(f"Expected dataset file not found: {csv_path}")
            df = read_csv(csv_path)
            frames.append(standardize_dataframe(df, year=year, gender=gender))
            metadata.setdefault(str(year), {})[gender] = {
                'file': filename,
                'sha256': hash_file(csv_path),
            }

    unified = pd.concat(frames, axis=0, ignore_index=True)
    unified = unified.sort_values(["year", "gender", "club_name", "short_name"])
    return unified.reset_index(drop=True), metadata


def compute_checksum(frame: pd.DataFrame) -> str:
    """Compute a deterministic checksum for the dataframe content."""

    payload = frame.sort_values("player_uid").to_json(orient="records", date_unit="s")
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Task I data ingestion pipeline")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing the yearly CSV files",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    file_map = load_file_map(args.data_dir)
    dataframe, metadata = load_players(args.data_dir, file_map)
    checksum = compute_checksum(dataframe)

    db_config = get_database_config()
    engine = create_db_engine(db_config.url)
    write_players_table(engine, dataframe)
    log_ingestion(
        engine,
        row_count=len(dataframe),
        years=sorted(dataframe["year"].unique().tolist()),
        source_files=metadata,
        checksum=checksum,
    )

    print("Ingestion completed successfully.")
    print(json.dumps(
        {
            "rows_loaded": len(dataframe),
            "years": sorted(dataframe["year"].unique().tolist()),
            "checksum": checksum,
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
