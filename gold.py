import duckdb
import logging

logger = logging.getLogger(__name__)

SILVER_SOURCE_PATH = "silver/**/*.parquet"
GOLD_OUTPUT_PATH = "gold"


# ---------------------------------------------------------------------
# Partition Detection
# ---------------------------------------------------------------------

def get_silver_partitions(con: duckdb.DuckDBPyConnection) -> set:
    """
    Detect available Silver partitions using hive partitioning.
    """
    partitions = con.execute(f"""
        SELECT DISTINCT city, date
        FROM read_parquet('{SILVER_SOURCE_PATH}', hive_partitioning=true)
    """).fetchall()

    return set(partitions)


def get_processed_partitions(con: duckdb.DuckDBPyConnection) -> set:
    """
    Fetch already processed Gold partitions from metadata.
    """
    processed = con.execute("""
        SELECT city, date
        FROM pipeline_metadata
        WHERE layer = 'gold'
    """).fetchall()

    return set(processed)


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------

def validate_partition(con: duckdb.DuckDBPyConnection):
    """
    Validate aggregated Gold data before writing.
    """
    row_count = con.execute("SELECT COUNT(*) FROM tmp_gold").fetchone()[0]

    if row_count == 0:
        raise ValueError("Empty Gold partition detected.")

    null_check = con.execute("""
        SELECT COUNT(*) FROM tmp_gold
        WHERE avg_temp IS NULL
    """).fetchone()[0]

    if null_check > 0:
        raise ValueError("Gold aggregation produced NULL averages.")


# ---------------------------------------------------------------------
# Processing Logic
# ---------------------------------------------------------------------

def process_partition(con: duckdb.DuckDBPyConnection, city: str, date: str):
    """
    Process a single Gold partition.
    """
    logger.info(f"Processing Gold partition: {city} - {date}")

    con.execute(f"""
        CREATE OR REPLACE TABLE tmp_gold AS
        SELECT
            city,
            date,
            AVG(temperature) AS avg_temp,
            MAX(temperature) AS max_temp,
            MIN(temperature) AS min_temp,
            AVG(humidity) AS avg_humidity,
            COUNT(*) AS record_count
        FROM read_parquet('{SILVER_SOURCE_PATH}', hive_partitioning=true)
        WHERE city = '{city}'
          AND date = '{date}'
        GROUP BY city, date;
    """)

    validate_partition(con)

    con.execute(f"""
        COPY tmp_gold
        TO '{GOLD_OUTPUT_PATH}'
        (FORMAT PARQUET, PARTITION_BY (city, date));
    """)

    con.execute("""
        INSERT OR REPLACE INTO pipeline_metadata
        VALUES ('gold', ?, ?, CURRENT_TIMESTAMP)
    """, [city, date])

    logger.info(f"Finished Gold partition: {city} - {date}")


# ---------------------------------------------------------------------
# Public Runner
# ---------------------------------------------------------------------

def run(con: duckdb.DuckDBPyConnection, full_refresh: bool = False):
    """
    Execute Gold incremental pipeline.
    """
    logger.info("Starting Gold layer processing")

    available = get_silver_partitions(con)

    if full_refresh:
        logger.info("Full refresh mode enabled")
        to_process = available
    else:
        processed = get_processed_partitions(con)
        to_process = available - processed

    logger.info(f"{len(to_process)} Gold partitions to process")

    for city, date in to_process:
        process_partition(con, city, date)

    logger.info("Gold layer completed")
