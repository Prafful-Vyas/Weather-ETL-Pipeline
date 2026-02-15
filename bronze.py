import duckdb
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

BRONZE_SOURCE_PATH = "data/**/*.parquet"
BRONZE_OUTPUT_PATH = "data"  # Bronze layer stays in /data


# ---------------------------------------------------------------------
# Partition Detection
# ---------------------------------------------------------------------

def get_source_partitions(con: duckdb.DuckDBPyConnection) -> set:
    """
    Detect available raw partitions using hive partitioning.
    """
    partitions = con.execute(f"""
        SELECT DISTINCT city, date
        FROM read_parquet('{BRONZE_SOURCE_PATH}', hive_partitioning=true)
    """).fetchall()

    return set(partitions)


def get_processed_partitions(con: duckdb.DuckDBPyConnection) -> set:
    """
    Fetch already processed Bronze partitions from metadata.
    """
    processed = con.execute("""
        SELECT city, date
        FROM pipeline_metadata
        WHERE layer = 'bronze'
    """).fetchall()

    return set(processed)


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------

def validate_partition(con: duckdb.DuckDBPyConnection):
    """
    Basic data quality checks before writing Bronze.
    """
    row_count = con.execute("SELECT COUNT(*) FROM tmp_bronze").fetchone()[0]

    if row_count == 0:
        raise ValueError("Empty Bronze partition detected.")

    null_check = con.execute("""
        SELECT COUNT(*) FROM tmp_bronze
        WHERE temperature IS NULL
    """).fetchone()[0]

    if null_check > 0:
        logger.warning("Null temperature values detected in Bronze layer.")


# ---------------------------------------------------------------------
# Processing Logic
# ---------------------------------------------------------------------

def process_partition(con: duckdb.DuckDBPyConnection, city: str, date: str):
    """
    Process a single Bronze partition.
    """
    logger.info(f"Processing Bronze partition: {city} - {date}")

    con.execute(f"""
        CREATE OR REPLACE TABLE tmp_bronze AS
        SELECT *
        FROM read_parquet('{BRONZE_SOURCE_PATH}', hive_partitioning=true)
        WHERE city = '{city}'
          AND date = '{date}';
    """)

    validate_partition(con)

    con.execute(f"""
        COPY tmp_bronze
        TO '{BRONZE_OUTPUT_PATH}'
        (FORMAT PARQUET, PARTITION_BY (city, date));
    """)

    con.execute("""
        INSERT OR REPLACE INTO pipeline_metadata
        VALUES ('bronze', ?, ?, CURRENT_TIMESTAMP)
    """, [city, date])

    logger.info(f"Finished Bronze partition: {city} - {date}")


# ---------------------------------------------------------------------
# Public Runner
# ---------------------------------------------------------------------

def run(con: duckdb.DuckDBPyConnection, full_refresh: bool = False):
    """
    Execute Bronze incremental pipeline.
    """
    logger.info("Starting Bronze layer processing")

    available = get_source_partitions(con)

    if full_refresh:
        logger.info("Full refresh mode enabled")
        to_process = available
    else:
        processed = get_processed_partitions(con)
        to_process = available - processed

    logger.info(f"{len(to_process)} Bronze partitions to process")

    for city, date in to_process:
        process_partition(con, city, date)

    logger.info("Bronze layer completed")
