# 🌦️ Weather ETL Pipeline

A production-style Weather ETL pipeline built using:

* **DuckDB** (in-process analytical database)
* **Hive-style partitioned Parquet**
* Incremental processing with partition tracking
* Modular architecture (`bronze.py`, `silver.py`, `gold.py`, `metadata.py`)
* **uv** as the package manager

This project demonstrates modern data engineering best practices using a lightweight local lakehouse architecture.

---

# 🏗 Architecture

The pipeline follows a **Medallion Architecture** pattern:

```
Raw Data  →  Bronze  →  Silver  →  Gold
```

### Layer Responsibilities

* **Bronze** → Raw ingestion & partition detection
* **Silver** → Data cleaning & validation
* **Gold** → Aggregated analytics

Each layer:

1. Detects available partitions
2. Checks already processed partitions
3. Computes the difference
4. Processes only new partitions
5. Writes partitioned Parquet output
6. Updates `pipeline_metadata`

This ensures fast, incremental, idempotent execution.

---

# 📂 Project Structure

```
.
├── data/                  # Bronze (raw weather parquet partitions)
│   └── city=<city>/date=<YYYY-MM-DD>/weather.parquet
├── silver/                # Silver (cleaned parquet)
├── gold/                  # Gold (aggregated parquet)
├── bronze.py              # Bronze layer logic
├── silver.py              # Silver layer logic
├── gold.py                # Gold layer logic
├── metadata.py            # Metadata table setup
├── main.py                # Pipeline orchestrator
├── sql-data-cleaning.ipynb
├── pyproject.toml         # Project config (uv)
└── uv.lock
```

---

# 🧱 Partitioning Strategy

This project uses **Hive-style partitioning**:

```
city=London/date=2026-02-13/weather.parquet
```

Benefits:

* Automatic partition column inference
* Partition pruning during queries
* Reduced I/O
* Lakehouse-compatible structure
* Scalable data layout

DuckDB automatically reads `city` and `date` from folder paths when:

```sql
read_parquet('silver/**/*.parquet', hive_partitioning = true)
```

---

# 📈 Incremental Processing

The pipeline avoids full rebuilds.

Instead, it processes **only new partitions**.

Metadata tracking is handled inside DuckDB:

```sql
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    layer TEXT,
    city TEXT,
    date DATE,
    processed_at TIMESTAMP,
    PRIMARY KEY (layer, city, date)
);
```

### Workflow Example

* New partition arrives in `data/`
* Bronze processes it
* Silver processes only new Bronze partitions
* Gold processes only new Silver partitions
* Metadata table updates automatically

This makes the pipeline:

* Idempotent
* Efficient
* Production-friendly

---

# 📦 Package Management (uv)

This project uses **uv** instead of pip.

### Install dependencies

```bash
uv sync
```

Or create a new environment:

```bash
uv venv
uv pip install duckdb pandas pyarrow
```

### Run the pipeline

```bash
uv run python main.py
```

### Open Jupyter Notebook

```bash
uv run jupyter notebook
```

---

# 🚀 Pipeline Orchestration

`main.py` coordinates execution:

```python
import duckdb
from metadata import initialize_metadata
import bronze
import silver
import gold

def main():
    con = duckdb.connect("pipeline.duckdb")
    initialize_metadata(con)
    bronze.run(con)
    silver.run(con)
    gold.run(con)

if __name__ == "__main__":
    main()
```

Each layer runs incrementally by default.

---

# 🥈 Silver Layer

* Cleans null and invalid values
* Normalizes column types
* Standardizes schema
* Writes partitioned Parquet
* Updates metadata

---

# 🥇 Gold Layer

Aggregates data at the `city + date` level.

Example metrics:

* Average temperature
* Minimum temperature
* Maximum temperature
* Average humidity
* Total record count

Outputs partitioned Parquet and updates metadata.

---

# 🧠 Querying with DuckDB

Query Gold layer directly:

```python
import duckdb

con = duckdb.connect()

df = con.execute("""
    SELECT *
    FROM read_parquet('gold/**/*.parquet', hive_partitioning=true)
    WHERE city = 'London'
      AND date = '2026-02-13';
""").df()

print(df)
```

DuckDB automatically applies partition pruning for fast queries.

---

# 🧠 What This Project Demonstrates

* Medallion architecture (Bronze → Silver → Gold)
* Incremental ETL design
* Metadata-driven processing
* Hive-style partitioning
* SQL-based data transformations
* Modular pipeline structure
* DuckDB analytics on Parquet
* Lakehouse-style local data platform

---

# 🔮 Future Improvements

* CLI flags (`--full-refresh`)
* Parallel partition processing
* Logging module
* Docker containerization
* Cloud storage support (S3 / GCS / Azure)
* Automated testing (pytest)
* CI/CD pipeline
* Data quality validation layer

---

# 📌 Summary

This project implements a scalable, partition-aware, incremental ETL pipeline using DuckDB and Parquet.

It mirrors real-world data engineering workflows and serves as a strong foundation for production-ready analytics pipelines.

