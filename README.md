# FIFA Player Ingestion – Task 1

This repository contains a focused implementation of Task 1 for the course project: building a
repeatable PySpark→PostgreSQL ingestion pipeline for the FIFA player datasets. The codebase creates
the required `fifa` schema, consolidates yearly CSV files for male (2015–2022) and female
(2016–2022) rosters, and loads a single relational table through JDBC.

## Repository Layout

```text
.
├── Course_Project_1 (1).pdf      # Original project description
├── README.md                     # This document
├── requirements.txt              # Python dependencies
├── data/
│   ├── README.md                 # Dataset download instructions
│   └── file_map.yml              # Expected CSV filenames per year
└── src/
    ├── __init__.py
    ├── config.py                 # Environment and dataset configuration helpers
    ├── db_utils.py               # PostgreSQL schema creation and JDBC utilities
    └── task1_load.py             # PySpark ingestion script for Task 1
```

## Environment Setup

1. Install Python 3.10+ and Java 8/11 (required for PySpark).
2. Create and activate a virtual environment.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Download the Kaggle FIFA datasets into the `data/` directory (follow `data/README.md`).
5. Provide a PostgreSQL connection string via the `FIFA_DB_URL` environment variable (e.g.
   `postgresql+psycopg2://user:pass@localhost:5432/fifa`).

### PostgreSQL JDBC Driver

Spark downloads the PostgreSQL JDBC driver from Maven at runtime. The script defaults to
`org.postgresql:postgresql:42.7.4`, but you can override the coordinate via the
`FIFA_POSTGRES_MAVEN` environment variable if you need a different version or repository proxy.
No manual jar management is required as long as outbound Maven access is available.

## Running the Ingestion Pipeline

Execute the Task 1 script after the CSV files are in place:

```bash
python -m src.task1_load --data-dir data
```

The script performs the following steps:

- Reads the file-year mapping in `data/file_map.yml` and selects the required seasons
  (male 2015–2022, female 2016–2022).
- Normalises column names across releases, fills aliases, and adds `year`, `gender`, and `player_id`
  (a deterministic gender+year+`sofifa_id` identifier) fields.
- Aligns typing for key attributes (contract year, age, height, weight, value, etc.) while retaining
  the original analytic features for later use.
- Creates the `fifa` schema when absent and writes the unified `fifa.players` table via Spark’s JDBC
  connector, truncating previous loads so the pipeline is re-runnable.

## Dataset Features

Each yearly CSV exposes a rich mix of identifying information, contextual metadata, and numerical
ratings. Highlights include:

- **Identity**: `sofifa_id`, `short_name`, `long_name`, and player URLs uniquely identify each
  athlete across seasons.
- **Demographics**: `age`, `height_cm`, `weight_kg`, `dob`, and `nationality_name` track player
  profile details.
- **Club/National Context**: `club_name`, `league_name`, `league_level`, `club_position`,
  `club_team_id`, `nation_team_id`, and associated flag/logo URLs describe affiliations and roles.
- **Contract & Value**: `contract_valid_until`, `value_eur`, `wage_eur`, `release_clause_eur`, and
  `club_joined` capture contract span and financial metrics.
- **Skill Ratings**: Overall/potential scores plus granular attributes (pace, shooting, passing,
  defending, goalkeeping sub-ratings, position-specific metrics like `cam`, `cdm`, `rb`, etc.)
  quantify on-field abilities.
- **Traits & Tags**: Columns such as `player_positions`, `player_traits`, `player_tags`, and
  `preferred_foot` summarise play style and positional versatility.

## Why Not Neo4J?

Task 1 centres on ingesting structured, row-oriented CSVs and supporting relational workloads such as
joining, filtering, aggregating by season or demographic, and feeding BI/reporting tools. PostgreSQL
maps cleanly to these needs, since it enforces a tabular schema, handles bulk loads efficiently through JDBC,
and integrates directly with PySpark. Neo4J (or similar graph stores) works best when the primary questions
require complex relationship traversals and graph algorithms; this dataset is already normalised as
flat records and does not capture rich graph edges beyond standard foreign keys. Switching to a graph
database would increase operational overhead without improving Task 1’s objectives, so PostgreSQL is
the more appropriate choice here.
