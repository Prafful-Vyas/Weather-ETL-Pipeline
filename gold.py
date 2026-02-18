import duckdb
import logging

logger = logging.getLogger(__name__)

SILVER_SOURCE_PATH = "silver/**/*.parquet"


# ---------------------------------------------------------------------
# Partition Detection
# ---------------------------------------------------------------------

def get_silver_partitions(con: duckdb.DuckDBPyConnection) -> set:
    """
    Detect available Silver partitions using hive partitioning.
    Handles missing silver folder safely.
    """
    try:
        partitions = con.execute(f"""
            SELECT DISTINCT city, date
            FROM read_parquet('{SILVER_SOURCE_PATH}', hive_partitioning=true)
        """).fetchall()

        return set(partitions)

    except duckdb.IOException:
        logger.warning("No Silver data found.")
        return set()


def get_processed_partitions(con: duckdb.DuckDBPyConnection) -> set:
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
    row_count = con.execute(
        "SELECT COUNT(*) FROM tmp_gold"
    ).fetchone()[0]

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

def process_partition(con: duckdb.DuckDBPyConnection, city: str, date):
    logger.info(f"Processing Gold partition: {city} - {date}")

    con.execute(f"""
        CREATE OR REPLACE TABLE tmp_gold AS
        SELECT
            city,
            CAST(date AS DATE) AS date,
            AVG(temperature) AS avg_temp,
            MAX(temperature) AS max_temp,
            MIN(temperature) AS min_temp,
            COUNT(*) AS record_count
        FROM read_parquet('{SILVER_SOURCE_PATH}', hive_partitioning=true)
        WHERE city = '{city}'
          AND date = '{date}'
        GROUP BY city, date
    """)

    validate_partition(con)

    con.execute(f"""
        COPY tmp_gold
        TO 'gold'
        (FORMAT PARQUET, PARTITION_BY (city, date), OVERWRITE TRUE);
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
    logger.info("Starting Gold layer processing")

    available = get_silver_partitions(con)

    if not available:
        logger.info("No Silver partitions available. Skipping Gold.")
        return

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
