# Course Project 1 – Tasks I & II Implementation

This repository provides reference Python implementations for the first two tasks of the Course Project.
It contains repeatable scripts that (1) build and populate the PostgreSQL data warehouse requested in
Task I and (2) expose PostgreSQL analytics required in Task II, including the YouTube comment
publisher/subscriber workflow.

## Repository Structure

```text
.
├── Course_Project_1 (1).pdf      # Original project description
├── README.md                     # This document
├── requirements.txt              # Python dependencies (no pandas / Spark)
├── data/
│   ├── README.md                 # Dataset download instructions
│   └── file_map.yml              # Expected CSV filenames per year
└── src/
    ├── __init__.py
    ├── config.py                 # Configuration helpers
    ├── db_utils.py               # PostgreSQL utilities and schema management
    ├── task1_load.py             # Task I ingestion workflow (pure SQL)
    ├── task2_analytics.py        # PostgreSQL analytics for Task II
    └── youtube_pubsub.py         # Publisher/subscriber utilities for Task II (Q5)
```

## Environment Setup

1.  Install Python 3.10+.
2.  Create and activate a virtual environment.
3.  Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

4.  Download the Kaggle dataset into the `data/` folder (see `data/README.md`).
5.  Configure environment variables (see below) or copy `.env.template` to `.env` and fill values.

### Required Environment Variables

| Variable | Description |
| --- | --- |
| `FIFA_DB_URL` | SQLAlchemy-compatible PostgreSQL connection string, e.g. `postgresql+psycopg2://user:pass@localhost:5432/fifa`. |
| `YOUTUBE_API_KEY` | API key for the YouTube Data API v3 (required for Task II question 5). |

You can supply these variables in one of two ways:

* **Temporary shell exports** (recommended for ad-hoc runs):

  ```bash
  export FIFA_DB_URL="postgresql+psycopg2://user:pass@localhost:5432/fifa"
  export YOUTUBE_API_KEY="your_api_key"
  python -m src.task1_load --data-dir data
  ```

  Replace the placeholder values with your actual connection string and API key. The
  exports last for the lifetime of the terminal session.

* **Persistent `.env` file** (recommended for repeated runs):

  ```bash
  cp .env.template .env
  # edit .env with your credentials
  ```

  The scripts call `load_dotenv` under the hood, so the variables defined in `.env`
  will be picked up automatically whenever you run the modules with `python -m ...`.

## Task I – Build and Populate Tables

Run the ingestion script after the dataset is placed under `data/`:

```bash
python -m src.task1_load --data-dir data
```

The script performs the following steps:

1.  Loads the CSV filename mapping from `data/file_map.yml`.
2.  Creates the `fifa` schema (if missing) and a unified `fifa.players` table with strongly typed columns tailored to the course deliverables.
3.  Normalizes column headers across seasons, adds a `year` column, and inserts a `player_uid` synthetic key to guarantee uniqueness.
4.  Performs all cleansing and type conversions with standard Python modules before issuing batched PostgreSQL `INSERT` commands (no pandas involved).
5.  Records ingestion metadata (row counts, years covered, and file hashes) in the `fifa.ingestion_log` table.

The script is idempotent—it truncates the existing `fifa.players` table before loading so that re-runs start from a clean slate.

### Dataset Features

The unified `fifa.players` table stores the most relevant columns required by the project:

* `player_uid` – synthetic unique identifier composed of gender, season, and `sofifa_id`.
* `year` – season of the roster snapshot.
* `gender` – `male` or `female` to distinguish the original roster.
* `sofifa_id` – persistent player identifier from EA's FIFA database.
* `short_name` / `long_name` – player display names used for reporting and YouTube matching.
* `club_name`, `league_name` – organization attributes used for contract and age analytics.
* `contract_valid_until` – contract expiration year, normalized as an integer for filtering.
* `age`, `nationality_name` – demographic features needed for age ranking and nationality reports.
* Additional skill attributes (`overall`, `potential`, `value_eur`, etc.) are preserved for future machine-learning tasks.

### Why PostgreSQL instead of NoSQL?

PostgreSQL remains the best default choice for this dataset. The yearly FIFA roster files share
consistent tabular schemas and require strong typing, relational joins, and SQL analytics. Features
like `contract_valid_until`, `club_name`, and `nationality_name` are naturally expressed as columns
and benefit from indexing and declarative constraints. While a graph database such as Neo4J could
model player relationships, it would add operational overhead without delivering clear advantages
for the required aggregations (counts, averages, rankings). Keeping the full workload inside
PostgreSQL also simplifies deployment because there is no additional Spark cluster to maintain.

## Task II – PostgreSQL Analytics

PostgreSQL-based analytics are implemented in `src/task2_analytics.py`. They can be executed either as Python functions (import the module) or through the CLI wrapper:

```bash
# Example invocations
python -m src.task2_analytics contracts --year 2020 --top 5 --contract-year 2024
python -m src.task2_analytics age --year 2018 --top 3 --order desc
python -m src.task2_analytics nationality
python -m src.task2_analytics histogram --output nationalities.png
python -m src.task2_analytics youtube --limit 250 --dump-path data/youtube_comments.jsonl
```

### Available Analytics

1.  **Contract Expirations** – `top_clubs_with_expiring_contracts`
    * Executes a PostgreSQL window-function query to return the clubs with the highest number of players whose `contract_valid_until` year is at least the specified threshold.
    * Results include ties for the last place when counts match.

2.  **Average Age Ranking** – `rank_clubs_by_average_age`
    * Calculates average age per club in the chosen season using SQL averages and supports ascending/descending order.
    * Validates user inputs and preserves ties for the final rank.

3.  **Most Popular Nationality per Year** – `most_popular_nationality_by_year`
    * Produces a PostgreSQL result set summarizing the most frequent nationality for each season.

4.  **Nationality Histogram** – `nationality_histogram`
    * Deduplicates players across seasons using their `sofifa_id` and plots the nationality distribution with Matplotlib. The histogram can be saved or displayed.

5.  **YouTube Popularity Analysis** – `find_most_discussed_player`
    * Uses the publisher/subscriber model in `youtube_pubsub.py` to collect comment streams from recent videos retrieved with the YouTube Data API.
    * Aggregates mentions of `short_name` values from the 2022 roster and returns the player with the highest mention count. The raw comment dump is persisted for grading evidence.

Each function issues SQL queries directly against PostgreSQL using SQLAlchemy connections. Connection properties are reused from `FIFA_DB_URL`.

## Explaining the Assignment

The Course Project asks students to build an end-to-end data pipeline and analytical stack around the FIFA player dataset:

* **Task I** focuses on data engineering—standardizing CSV files from multiple seasons, merging male and female rosters, creating a dedicated schema in PostgreSQL, and ensuring every row can be uniquely identified. The README must also discuss feature descriptions and whether a NoSQL option would be preferable. This repository supplies scripts and documentation to meet those requirements.
* **Task II** moves to analytical SQL over PostgreSQL. Students must implement parameterized queries for contract expiration trends, age-based club rankings, yearly nationality leaders, and deduplicated nationality histograms. Additionally, they must integrate external data (YouTube comments) using a publisher/subscriber pattern to determine the most discussed player for 2022.

Combined, these deliverables demonstrate proficiency in database design, scalable ETL, distributed query processing, and streaming/text analytics, forming the foundation for later machine learning and deployment tasks.
