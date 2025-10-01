from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable, Mapping

if __package__ is None or __package__ == "":
    import sys

    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

    from spark_compat import ensure_typing_submodules  # Reference: conlsulted copilot for spark_compat.py

    ensure_typing_submodules()

    from config import ConfigError, get_database_config, load_file_map 
    from db_utils import create_db_engine, write_players_table  
else:
    from .spark_compat import ensure_typing_submodules

    ensure_typing_submodules()

    from .config import ConfigError, get_database_config, load_file_map
    from .db_utils import create_db_engine, write_players_table

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_POSTGRES_COORDINATE = "org.postgresql:postgresql:42.7.4"

EXPECTED_DATASETS: dict[str, tuple[int, ...]] = {
    "male": tuple(range(2015, 2023)),
    "female": tuple(range(2016, 2023)),
}

PLAYER_ID_COLUMN = "player_id"
YEAR_COLUMN = "year"
GENDER_COLUMN = "gender"

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

COLUMN_TYPE_MAP: dict[str, str] = {
    "sofifa_id": "long",
    YEAR_COLUMN: "int",
    "contract_valid_until": "int",
    "age": "int",
    "height_cm": "int",
    "weight_kg": "int",
}

COLUMN_TYPE_MAP.update(
    {
        column: "double"
        for column in {
            "attacking_crossing",
            "attacking_finishing",
            "attacking_heading_accuracy",
            "attacking_short_passing",
            "attacking_volleys",
            "cam",
            "cb",
            "cdm",
            "cf",
            "club_jersey_number",
            "club_team_id",
            "cm",
            "defending",
            "defending_marking_awareness",
            "defending_sliding_tackle",
            "defending_standing_tackle",
            "dribbling",
            "gk",
            "goalkeeping_diving",
            "goalkeeping_handling",
            "goalkeeping_kicking",
            "goalkeeping_positioning",
            "goalkeeping_reflexes",
            "goalkeeping_speed",
            "international_reputation",
            "lam",
            "lb",
            "lcb",
            "lcm",
            "ldm",
            "league_level",
            "lf",
            "lm",
            "ls",
            "lw",
            "lwb",
            "mentality_aggression",
            "mentality_composure",
            "mentality_interceptions",
            "mentality_penalties",
            "mentality_positioning",
            "mentality_vision",
            "movement_acceleration",
            "movement_agility",
            "movement_balance",
            "movement_reactions",
            "movement_sprint_speed",
            "nation_jersey_number",
            "nation_team_id",
            "nationality_id",
            "overall",
            "pace",
            "passing",
            "physic",
            "power_jumping",
            "power_long_shots",
            "power_shot_power",
            "power_stamina",
            "power_strength",
            "potential",
            "ram",
            "rb",
            "rcb",
            "rcm",
            "rdm",
            "release_clause_eur",
            "rf",
            "rm",
            "rs",
            "rw",
            "rwb",
            "shooting",
            "skill_ball_control",
            "skill_curve",
            "skill_dribbling",
            "skill_fk_accuracy",
            "skill_long_passing",
            "skill_moves",
            "st",
            "value_eur",
            "wage_eur",
            "weak_foot",
        }
    }
)

for column in {"club_joined", "dob"}:
    COLUMN_TYPE_MAP[column] = "timestamp"

COLUMN_ALIASES = {
    "nationality": "nationality_name",
    "nationality_name": "nationality_name",
    "team_jersey_number": "club_jersey_number",
    "club": "club_name",
    "club_contract_valid_until": "contract_valid_until",
    "contract": "contract_valid_until",
    "contract_end_year": "contract_valid_until",
}

CORE_COLUMNS = [
    PLAYER_ID_COLUMN,
    YEAR_COLUMN,
    GENDER_COLUMN,
    "sofifa_id",
    "short_name",
    "long_name",
    "club_name",
    "league_name",
    "contract_valid_until",
    "age",
    "nationality_name",
]

SORT_COLUMNS = [
    YEAR_COLUMN,
    GENDER_COLUMN,
    PLAYER_ID_COLUMN,
    "club_name",
    "short_name",
]

# Reference: conlsulted copilot for postgres jdbc jar loading logic
def _resolve_postgres_jdbc_jar(project_root: Path) -> Path | None:
    """Return the newest PostgreSQL JDBC jar found locally or via environment."""

    candidates: list[Path] = []
    env_value = os.getenv("FIFA_POSTGRES_JAR")
    if env_value:
        env_path = Path(env_value).expanduser()
        if not env_path.exists():
            raise ConfigError(
                f"The FIFA_POSTGRES_JAR path '{env_value}' does not exist or is inaccessible."
            )
        if env_path.is_dir():
            candidates.extend(sorted(env_path.glob("postgresql-*.jar")))
        else:
            candidates.append(env_path)

    search_roots = [project_root / "artifacts", project_root / "artifacts" / "jars"]
    for root in search_roots:
        if not root.exists():
            continue
        candidates.extend(path for path in sorted(root.glob("postgresql-*.jar")) if path.is_file())

    unique_candidates: dict[Path, float] = {}
    for path in candidates:
        try:
            unique_candidates[path.resolve()] = path.stat().st_mtime
        except FileNotFoundError:
            continue

    if not unique_candidates:
        return None

    return max(unique_candidates, key=unique_candidates.get)

# Reference: conlsulted copilot for postgres jdbc jar loading logic
def _configure_postgres_driver(
    builder: SparkSession.Builder, project_root: Path
) -> SparkSession.Builder:
    """Augment the Spark builder so the PostgreSQL JDBC driver is on the classpath."""

    jar_path = _resolve_postgres_jdbc_jar(project_root)
    if jar_path is not None:
        jar_str = str(jar_path)
        return (
            builder.config("spark.jars", jar_str)
            .config("spark.driver.extraClassPath", jar_str)
            .config("spark.executor.extraClassPath", jar_str)
        )

    coordinate = os.getenv("FIFA_POSTGRES_MAVEN", DEFAULT_POSTGRES_COORDINATE)
    return builder.config("spark.jars.packages", coordinate)


def _ensure_postgres_driver_loaded(spark: SparkSession) -> None:
    """Verify that the PostgreSQL JDBC driver is available to the Spark JVM."""

    try:
        spark._jvm.java.lang.Class.forName("org.postgresql.Driver")  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - JVM interaction not exercised in tests
        raise ConfigError(
            "PostgreSQL JDBC driver not found. Provide it via FIFA_POSTGRES_JAR or set "
            "FIFA_POSTGRES_MAVEN (e.g. 'org.postgresql:postgresql:42.7.4') before running the "
            "ingestion script."
        ) from exc


def _cast_numeric_column(
    frame: SparkDataFrame, column: str, target_type: str
) -> SparkDataFrame:
    """Strip non-numeric characters and cast the column to the requested numeric type."""

    numeric_pattern = r"(-?\d+(?:\.\d+)?)"
    numeric_str = F.regexp_extract(F.col(column).cast("string"), numeric_pattern, 1)
    sanitized = F.when(F.length(F.trim(numeric_str)) == 0, F.lit(None)).otherwise(numeric_str)
    return frame.withColumn(column, sanitized.cast(target_type))


def normalize_column(name: str) -> str:
    """Return a canonical snake_case version of a raw column header."""

    cleaned = name.strip().lower()
    cleaned = cleaned.replace("%", "_pct").replace("/", "_")
    cleaned = cleaned.replace("(", "_").replace(")", "")
    cleaned = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in cleaned)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")


def read_csv(spark: SparkSession, path: Path) -> SparkDataFrame:
    """Load a CSV file into Spark with header and schema inference enabled."""

    return spark.read.csv(str(path), header=True, inferSchema=True)


def rename_columns(frame: SparkDataFrame) -> SparkDataFrame:
    """Apply `normalize_column` to every column in the DataFrame."""

    renamed = frame
    for current in frame.columns:
        normalized = normalize_column(current)
        if normalized != current:
            renamed = renamed.withColumnRenamed(current, normalized)
    return renamed


def apply_aliases(frame: SparkDataFrame) -> SparkDataFrame:
    """Collapse known alias columns into their canonical counterparts."""

    unified = frame
    for source, target in COLUMN_ALIASES.items():
        if source not in unified.columns or source == target:
            continue
        if target in unified.columns:
            unified = unified.withColumn(target, F.coalesce(F.col(target), F.col(source)))
            unified = unified.drop(source)
        else:
            unified = unified.withColumnRenamed(source, target)
    return unified


def standardize_dataframe(
    frame: SparkDataFrame, *, year: int, gender: str
) -> SparkDataFrame:
    """Prepare a single input dataset with normalized columns and core metadata."""

    frame = rename_columns(frame)
    frame = apply_aliases(frame)

    frame = frame.withColumn(YEAR_COLUMN, F.lit(int(year)))
    frame = frame.withColumn(GENDER_COLUMN, F.lit(gender.lower()))

    if "sofifa_id" not in frame.columns:
        if "player_url" in frame.columns:
            frame = frame.withColumn(
                "sofifa_id",
                F.regexp_extract(F.col("player_url").cast("string"), r"(\d+)", 1),
            )
        else:
            raise ConfigError(
                "Missing `sofifa_id` column and unable to infer it from player_url."
            )
    frame = frame.withColumn("sofifa_id", F.col("sofifa_id").cast("long"))

    if "short_name" not in frame.columns and "name" in frame.columns:
        frame = frame.withColumn("short_name", F.col("name"))

    if "long_name" not in frame.columns and "full_name" in frame.columns:
        frame = frame.withColumn("long_name", F.col("full_name"))

    if "contract_valid_until" in frame.columns:
        frame = frame.withColumn(
            "contract_valid_until", F.col("contract_valid_until").cast("int")
        )
    else:
        frame = frame.withColumn("contract_valid_until", F.lit(None).cast("int"))

    if "age" in frame.columns:
        frame = frame.withColumn("age", F.col("age").cast("int"))
    else:
        frame = frame.withColumn("age", F.lit(None).cast("int"))

    frame = frame.withColumn(
        PLAYER_ID_COLUMN,
        F.concat(
            F.upper(F.substring(F.col(GENDER_COLUMN), 1, 1)),
            F.lit("_"),
            F.col(YEAR_COLUMN).cast("string"),
            F.lit("_"),
            F.col("sofifa_id").cast("string"),
        ),
    )

    return frame.dropDuplicates([PLAYER_ID_COLUMN])


def ensure_expected_columns(frame: SparkDataFrame) -> SparkDataFrame:
    """Add any missing required columns and coerce configured column types."""

    current = frame
    for column in EXPECTED_COLUMNS:
        if column in current.columns:
            continue

        target_type = COLUMN_TYPE_MAP.get(column, "string")
        current = current.withColumn(column, F.lit(None).cast(target_type))

    for column, target_type in COLUMN_TYPE_MAP.items():
        if column not in current.columns:
            continue
        if target_type in {"long", "int", "double"}:
            current = _cast_numeric_column(current, column, target_type)
        elif target_type == "timestamp":
            current = current.withColumn(
                column,
                F.when(F.trim(F.col(column).cast("string")) == "", F.lit(None).cast("timestamp"))
                .otherwise(F.to_timestamp(F.col(column))),
            )
    return current


def order_columns(frame: SparkDataFrame) -> SparkDataFrame:
    """Project the DataFrame using a stable column ordering."""

    ordered = list(CORE_COLUMNS)
    ordered += sorted(col for col in frame.columns if col not in CORE_COLUMNS)
    return frame.select(*ordered)


def build_players_dataframe(
    spark: SparkSession,
    data_dir: Path,
    file_map: Mapping[str, Mapping[int, str]],
) -> SparkDataFrame:
    """Load, align, and union all Task 1 FIFA player datasets."""

    materialised_map = {group: dict(entries) for group, entries in file_map.items()}
    frames: list[SparkDataFrame] = []

    for gender, years in EXPECTED_DATASETS.items():
        if gender not in materialised_map:
            raise ConfigError(f"Missing '{gender}' datasets in file_map.yml.")

        year_to_file = materialised_map[gender]
        for year in years:
            if year not in year_to_file:
                raise ConfigError(
                    f"Missing filename for gender='{gender}' year={year} in file_map.yml."
                )

            csv_path = data_dir / year_to_file[year]
            if not csv_path.exists():
                raise FileNotFoundError(f"Expected dataset file not found: {csv_path}")

            raw_frame = read_csv(spark, csv_path)
            standardised = standardize_dataframe(raw_frame, year=year, gender=gender)
            frames.append(standardised)

    if not frames:
        raise ConfigError("No player datasets were loaded; check the configuration.")

    unified = frames[0]
    for frame in frames[1:]:
        unified = unified.unionByName(frame, allowMissingColumns=True)

    unified = ensure_expected_columns(unified)
    unified = order_columns(unified)
    unified = unified.dropDuplicates([PLAYER_ID_COLUMN])
    unified = unified.orderBy(*(F.col(column) for column in SORT_COLUMNS))
    return unified


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for the ingestion pipeline."""

    parser = argparse.ArgumentParser(description="Task 1 data ingestion pipeline")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Directory containing the yearly CSV files",
    )
    return parser.parse_args(argv)


def build_spark_session(project_root: Path) -> SparkSession:
    """Construct a Spark session configured with the PostgreSQL JDBC driver."""

    builder = SparkSession.builder.appName("task1-load")
    builder = _configure_postgres_driver(builder, project_root)
    spark = builder.getOrCreate()
    _ensure_postgres_driver_loaded(spark)
    return spark


def main(argv: Iterable[str] | None = None) -> None:
    """Execute the Task 1 ingestion workflow."""

    args = parse_args(argv)
    data_dir = args.data_dir
    file_map = load_file_map(data_dir)

    project_root = Path(__file__).resolve().parents[1]
    spark = build_spark_session(project_root)
    players_df: SparkDataFrame | None = None

    try:
        players_df = build_players_dataframe(spark, data_dir, file_map).cache()
        row_count = players_df.count()
        print(f"Loaded {row_count} players spanning {len(EXPECTED_DATASETS)} datasets.")

        db_config = get_database_config()
        engine = create_db_engine(db_config.url)
        write_players_table(engine, players_df)
        print("Player data written to PostgreSQL schema 'fifa'.")
    finally:
        if players_df is not None:
            players_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
