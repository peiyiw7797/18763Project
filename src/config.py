from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class DatabaseConfig:
    """Simple container for database connectivity information."""

    url: str


class ConfigError(RuntimeError):
    """Raised when the local configuration is incomplete."""


def load_environment(dotenv_path: str | os.PathLike[str] | None = None) -> None:
    """Load environment variables from a `.env` file if present."""

    load_dotenv(dotenv_path=dotenv_path, override=False)


def get_database_config() -> DatabaseConfig:
    """Return validated database configuration from the environment."""

    load_environment()
    url = os.getenv("FIFA_DB_URL")
    if not url:
        raise ConfigError(
            "FIFA_DB_URL is not set. Export it or configure it in a .env file."
        )
    return DatabaseConfig(url=url)


def load_file_map(data_dir: str | os.PathLike[str]) -> Mapping[str, Dict[int, str]]:
    """Load the CSV filename map for male and female datasets."""

    path = Path(data_dir) / "file_map.yml"
    if not path.exists():
        raise ConfigError(
            f"Dataset mapping file not found at {path}. Please create it based on data/file_map.yml."
        )
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    normalized: Dict[str, Dict[int, str]] = {}
    for group, entries in data.items():
        if not isinstance(entries, dict):
            raise ConfigError(
                f"Invalid mapping for '{group}'. Expected a dictionary of year -> filename."
            )
        normalized[group] = {}
        for year_str, filename in entries.items():
            year = int(year_str)
            normalized[group][year] = filename
    return normalized


__all__ = [
    "ConfigError",
    "DatabaseConfig",
    "get_database_config",
    "load_file_map",
    "load_environment",
]
