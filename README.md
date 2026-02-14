Here is a **clean, properly formatted, ready-to-paste `README.md`** — no formatting issues:

---

# 🌦️ Weather Data Lakehouse (DuckDB + uv)

A production-style **Medallion Architecture** data pipeline built using:

* DuckDB (in-process analytical database)
* Hive-style partitioned Parquet
* `uv` as the package manager
* Bronze → Silver → Gold layered modeling

This project demonstrates modern data engineering practices using a lightweight, local-first lakehouse design.

---

# 🏗 Architecture

The pipeline follows a Medallion (Bronze / Silver / Gold) architecture pattern:

```
data/   → Bronze (raw weather data)
silver/ → Cleaned & standardized data
gold/   → Aggregated & analytics-ready datasets
```

---

# 📂 Project Structure

```
.
├── data/              # Bronze layer (raw weather data)
│   └── city=<city>/date=<YYYY-MM-DD>/weather.parquet
│
├── silver/            # Cleaned & transformed data
│   └── city=<city>/date=<YYYY-MM-DD>/data_0.parquet
│
├── gold/              # Aggregated analytics layer
│   └── city=<city>/date=<YYYY-MM-DD>/data_0.parquet
│
├── main.py            # Pipeline entrypoint
├── sql-data-cleaning.ipynb
├── requirements.txt
```

---

# 🗂 Partitioning Strategy

The dataset uses **Hive-style partitioning**:

```
city=Delhi/date=2026-02-13/weather.parquet
```

### Benefits

* Partition pruning
* Faster queries
* Reduced I/O
* Scalable storage layout
* Lakehouse-compatible design

DuckDB automatically detects partition columns (`city`, `date`) from folder names.

---

# 📦 Package Management (uv)

This project uses **uv** instead of pip.

## Install dependencies

```bash
uv sync
```

If starting from scratch:

```bash
uv venv
uv pip install duckdb pandas pyarrow jupyter
```

Run the pipeline:

```bash
uv run python main.py
```

Run the notebook:

```bash
uv run jupyter notebook
```

---

# 🚀 Querying Partitioned Data with DuckDB

## Read Bronze Layer

```python
import duckdb

con = duckdb.connect()

df = con.execute("""
    SELECT *
    FROM read_parquet('data/**/*.parquet', hive_partitioning=true)
""").df()
```

DuckDB automatically extracts:

* `city`
* `date`

---

## Filter with Partition Pruning

```sql
SELECT *
FROM read_parquet('data/**/*.parquet', hive_partitioning=true)
WHERE city = 'Delhi'
  AND date = '2026-02-13';
```

Only the relevant partition is scanned.

---

# 🥈 Silver Layer (Cleaning Example)

```sql
CREATE OR REPLACE TABLE silver_weather AS
SELECT
    city,
    CAST(date AS DATE) AS date,
    temperature,
    humidity,
    wind_speed
FROM read_parquet('data/**/*.parquet', hive_partitioning=true)
WHERE temperature IS NOT NULL;
```

Export as partitioned Parquet:

```sql
COPY silver_weather
TO 'silver'
(FORMAT PARQUET, PARTITION_BY (city, date));
```

---

# 🥇 Gold Layer (Aggregation Example)

```sql
CREATE OR REPLACE TABLE gold_daily_summary AS
SELECT
    city,
    date,
    AVG(temperature) AS avg_temp,
    MAX(temperature) AS max_temp,
    MIN(temperature) AS min_temp,
    AVG(humidity) AS avg_humidity
FROM read_parquet('silver/**/*.parquet', hive_partitioning=true)
GROUP BY city, date;
```

Export:

```sql
COPY gold_daily_summary
TO 'gold'
(FORMAT PARQUET, PARTITION_BY (city, date));
```

---

# ⚡ Why DuckDB?

* Vectorized execution engine
* Columnar processing
* In-process (no server required)
* Direct Parquet querying
* Automatic partition pruning
* Ideal for local analytics & pipelines

---

# 🎯 What This Project Demonstrates

* Modern lakehouse architecture
* Hive-style partitioning
* SQL-first data transformations
* Bronze → Silver → Gold modeling
* Efficient analytical querying
* Reproducible pipelines using `uv`
* Production-style data engineering design

---

# 🔮 Future Improvements

* Incremental loads
* CLI interface
* Logging & monitoring
* Airflow orchestration
* Docker support
* CI/CD pipeline
* Cloud storage integration (S3/GCS/Azure)

---

# 📌 Summary

This project implements a lightweight local lakehouse using DuckDB with a scalable partitioned layout and clean separation of data layers — mirroring production-grade data engineering workflows in a minimal, efficient setup.

