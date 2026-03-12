# 🌦️ Weather ETL Pipeline

A **production-style Weather ETL pipeline** built using:

* **Apache Spark (PySpark + Spark SQL)**
* **Hive-style partitioned Parquet**
* **Incremental partition-based processing**
* **Medallion Architecture (Bronze → Silver → Gold)**
* **Metadata-driven pipeline execution**
* **uv** as the package manager

This project demonstrates modern **data engineering best practices** using a lightweight **local lakehouse architecture powered by Spark**.

---

# 🏗 Architecture

The pipeline follows a **Medallion Architecture** pattern:

```
Raw Data  →  Bronze  →  Silver  →  Gold
```

### Layer Responsibilities

**Bronze**

* Raw ingestion
* Partition detection
* Writes raw API data to Parquet

**Silver**

* Data cleaning
* Schema normalization
* Type casting
* Data validation

**Gold**

* Aggregated analytics
* City-level weather metrics

---

Each layer performs **incremental processing**:

1. Detect available partitions
2. Check previously processed partitions
3. Compute the difference
4. Process only new partitions
5. Write partitioned output
6. Update metadata

This ensures:

* **Idempotent execution**
* **Efficient processing**
* **Scalable data pipelines**

---

# 📂 Project Structure

```
.
├── data/                  # Bronze layer (raw parquet partitions)
│   └── city=<city>/date=<YYYY-MM-DD>/weather.parquet
│
├── silver/                # Silver layer (cleaned parquet)
├── gold/                  # Gold layer (aggregated parquet)
│
├── ingestion.py           # Weather API ingestion
├── bronze.py              # Bronze layer processing
├── silver.py              # Silver layer transformations
├── gold.py                # Gold layer aggregations
├── metadata.py            # Metadata table setup
├── main.py                # Pipeline orchestrator
│
├── sql-data-cleaning.ipynb
│
├── pyproject.toml         # Project configuration
└── uv.lock
```

---

# 🧱 Partitioning Strategy

This project uses **Hive-style partitioning**.

Example structure:

```
data/
  city=London/
      date=2026-02-13/
          weather.parquet
```

Benefits:

* Automatic partition column inference
* Partition pruning
* Reduced I/O
* Efficient Spark queries
* Scalable data layout

Spark automatically reads partition columns from directory names.

Example:

```python
spark.read.parquet("silver/**/*.parquet")
```

Spark infers `city` and `date` from folder paths.

---

# 📈 Incremental Processing

The pipeline **avoids full rebuilds**.

Instead, it processes **only new partitions**.

A metadata table tracks processed partitions.

```sql
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    layer STRING,
    city STRING,
    date DATE,
    processed_at TIMESTAMP
)
USING PARQUET;
```

---

### Example Workflow

1. New weather data arrives in `data/`
2. Bronze detects new partitions
3. Silver processes only new Bronze partitions
4. Gold processes only new Silver partitions
5. Metadata table is updated

This makes the pipeline:

* **Incremental**
* **Idempotent**
* **Efficient**

---

# ⚡ Spark Processing

The pipeline uses **Apache Spark with Spark SQL** for distributed data processing.

Spark handles:

* Parquet scanning
* Partition pruning
* SQL transformations
* Aggregations
* Parallel execution

Example transformation:

```sql
SELECT
    city,
    date,
    AVG(temperature) AS avg_temp,
    MAX(temperature) AS max_temp,
    MIN(temperature) AS min_temp,
    COUNT(*) AS record_count
FROM silver
GROUP BY city, date
```

---

# 🚀 Running the Pipeline

### Install dependencies

```bash
uv sync
```

---

### Run the pipeline

```bash
uv run python main.py
```

---

### Start Jupyter Notebook

```bash
uv run jupyter notebook
```

---

# ⚙️ Pipeline Orchestration

`main.py` coordinates pipeline execution.

```python
from pyspark.sql import SparkSession
from metadata import initialize_metadata
import silver
import gold

def main():

    spark = SparkSession.builder \
        .appName("weather-etl") \
        .getOrCreate()

    initialize_metadata(spark)

    silver.run(spark)
    gold.run(spark)

    spark.stop()

if __name__ == "__main__":
    main()
```

Each layer runs **incrementally by default**.

---

# 🥈 Silver Layer

Responsibilities:

* Remove invalid records
* Normalize schema
* Cast column types
* Validate data
* Write partitioned Parquet

Output format:

```
silver/
  city=<city>/
      date=<date>/
```

---

# 🥇 Gold Layer

Aggregates weather data at **city + date** level.

Example metrics:

* Average temperature
* Maximum temperature
* Minimum temperature
* Record count

Outputs partitioned Parquet:

```
gold/
  city=<city>/
      date=<date>/
```

---

# 🔎 Querying Gold Data with Spark

Example query:

```python
df = spark.read.parquet("gold/**/*.parquet")

df.filter(
    (df.city == "London") &
    (df.date == "2026-02-13")
).show()
```

Spark automatically performs **partition pruning**, reducing file scans.

---

# 🧠 What This Project Demonstrates

This project showcases core **Data Engineering concepts**:

* Apache Spark data processing
* Medallion architecture
* Incremental ETL pipelines
* Metadata-driven orchestration
* Hive-style partitioning
* SQL-based transformations
* Modular pipeline design
* Lakehouse-style data layout

---

# 🔮 Future Improvements

Potential enhancements:

* **Delta Lake** support
* **Kafka ingestion**
* **Airflow orchestration**
* **Parallel partition processing**
* **Data quality checks**
* **Structured logging**
* **Docker containerization**
* **Cloud storage support (S3 / GCS / Azure)**
* **CI/CD pipeline**
* **Automated testing (pytest)**

---

# 📌 Summary

This project implements a **scalable, partition-aware, incremental ETL pipeline using Apache Spark and Parquet**.

It mirrors real-world **data engineering workflows** and serves as a strong foundation for **production-grade analytics pipelines**.