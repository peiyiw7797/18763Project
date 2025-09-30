from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

from .config import ConfigError, get_database_config, load_file_map
from .db_utils import create_db_engine, log_ingestion, write_players_table

NUMERIC_COLUMNS = {
    "overall",
    "potential",
    "value_eur",
    "wage_eur",
    "release_clause_eur",
    "height_cm",
    "weight_kg",
    "age",
    "contract_valid_until",
}


BASE_COLUMNS = {
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
    "club": "club_name",
    "club_contract_valid_until": "contract_valid_until",
    "contract": "contract_valid_until",
    "contract_end_year": "contract_valid_until",
    "short_name": "short_name",
    "name": "short_name",
    "long_name": "long_name",
    "full_name": "long_name",
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
    """Return the SHA-256 hash for the given file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def to_int(value: str | None) -> int | None:
    """Convert string values to integers, ignoring blanks and invalid entries."""

    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"na", "nan", "null", "none"}:
        return None
    try:
        if "." in text:
            return int(float(text))
        return int(text)
    except ValueError:
        return None


def extract_sofifa_id(data: Dict[str, str]) -> int | None:
    """Extract the sofifa identifier from available columns."""

    sofifa = to_int(data.get("sofifa_id"))
    if sofifa is not None:
        return sofifa
    url = data.get("player_url") or data.get("playerid_url")
    if url:
        match = re.search(r"(\d+)", str(url))
        if match:
            return to_int(match.group(1))
    return None


def normalize_record(
    row: Dict[str, str], *, year: int, gender: str
) -> Dict[str, object]:
    """Convert a raw CSV row into the canonical record representation."""

    normalized = {normalize_column(key): value for key, value in row.items()}
    renamed: Dict[str, str] = {}
    for key, value in normalized.items():
        target = COLUMN_ALIASES.get(key, key)
        renamed[target] = value

    if "nationality_name" not in renamed:
        renamed["nationality_name"] = normalized.get("nationality")

    sofifa_id = extract_sofifa_id(renamed)
    if sofifa_id is None:
        raise ConfigError("Unable to determine sofifa_id for a player record.")

    short_name = renamed.get("short_name") or renamed.get("name")
    long_name = renamed.get("long_name") or renamed.get("full_name")

    record: Dict[str, object] = {
        "player_uid": f"{gender[0].upper()}_{year}_{sofifa_id}",
        "year": year,
        "gender": gender,
        "sofifa_id": sofifa_id,
        "short_name": (short_name or "").strip() or None,
        "long_name": (long_name or "").strip() or None,
        "club_name": (renamed.get("club_name") or "").strip() or None,
        "league_name": (renamed.get("league_name") or "").strip() or None,
        "player_positions": (renamed.get("player_positions") or "").strip() or None,
        "nationality_name": (renamed.get("nationality_name") or "").strip() or None,
        "extra_attributes": {},
    }

    for column in BASE_COLUMNS:
        if column in {"short_name", "long_name", "club_name", "league_name", "player_positions", "nationality_name"}:
            continue
        value = renamed.get(column)
        record[column] = to_int(value) if column in NUMERIC_COLUMNS else value

    for column in BASE_COLUMNS:
        if column not in record:
            record[column] = None

    record.setdefault("overall", None)
    record.setdefault("potential", None)
    record.setdefault("value_eur", None)
    record.setdefault("wage_eur", None)
    record.setdefault("release_clause_eur", None)
    record.setdefault("height_cm", None)
    record.setdefault("weight_kg", None)
    record.setdefault("age", None)
    record.setdefault("contract_valid_until", None)

    extra_keys = set(renamed) - (BASE_COLUMNS | {"short_name", "long_name", "player_positions", "club_name", "league_name", "nationality_name", "player_url", "playerid_url"})
    record["extra_attributes"] = {
        key: renamed[key]
        for key in sorted(extra_keys)
        if renamed.get(key) not in {None, ""}
    }

    return record


def iter_records(path: Path, *, year: int, gender: str) -> Iterator[Dict[str, object]]:
    """Yield normalized player records for a given CSV file."""

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not any(row.values()):
                continue
            yield normalize_record(row, year=year, gender=gender)


def load_players(
    data_dir: Path, file_map: Dict[str, Dict[int, str]]
) -> Tuple[List[Dict[str, object]], Dict[str, Dict[str, Dict[str, str]]]]:
    """Read all configured CSV files and return records plus metadata."""

    records: List[Dict[str, object]] = []
    metadata: Dict[str, Dict[str, Dict[str, str]]] = {}

    for group, entries in file_map.items():
        gender = "female" if group.lower().startswith("f") else "male"
        for year, filename in sorted(entries.items()):
            csv_path = data_dir / filename
            if not csv_path.exists():
                raise FileNotFoundError(f"Expected dataset file not found: {csv_path}")
            for record in iter_records(csv_path, year=year, gender=gender):
                records.append(record)
            metadata.setdefault(str(year), {})[gender] = {
                "file": filename,
                "sha256": hash_file(csv_path),
            }

    deduplicated = {record["player_uid"]: record for record in records}
    ordered = sorted(
        deduplicated.values(),
        key=lambda item: (
            item["year"],
            item["gender"],
            item.get("club_name") or "",
            item.get("short_name") or "",
        ),
    )
    return ordered, metadata


def compute_checksum(records: List[Dict[str, object]]) -> str:
    """Compute a deterministic checksum for the normalized player records."""

    serializable = []
    for record in sorted(records, key=lambda item: item["player_uid"]):
        copy = {key: record[key] for key in record if key != "extra_attributes"}
        copy["extra_attributes"] = record.get("extra_attributes", {})
        serializable.append(copy)
    payload = json.dumps(serializable, ensure_ascii=False, sort_keys=True)
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
    records, metadata = load_players(args.data_dir, file_map)
    checksum = compute_checksum(records)

    db_config = get_database_config()
    engine = create_db_engine(db_config.url)
    write_players_table(engine, records)
    log_ingestion(
        engine,
        row_count=len(records),
        years=sorted({int(record["year"]) for record in records}),
        source_files=metadata,
        checksum=checksum,
    )

    print("Ingestion completed successfully.")
    print(json.dumps(
        {
            "rows_loaded": len(records),
            "years": sorted({int(record["year"]) for record in records}),
            "checksum": checksum,
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
