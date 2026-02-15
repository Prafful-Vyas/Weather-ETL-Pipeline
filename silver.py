import duckdb
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def get_bronze_partitions(con):
    return set(con.execute("""
        SELECT DISTINCT city, date
        FROM read_parquet('data/**/*.parquet', hive_partitioning=true)
    """).fetchall())


def get_processed_partitions(con):
    return set(con.execute("""
        SELECT city, date
        FROM pipeline_metadata
        WHERE layer = 'silver'
    """).fetchall())


def process_partition(con, city, date):
    logger.info(f"Processing Silver partition: {city} - {date}")

    con.execute(f"""
        CREATE OR REPLACE TABLE tmp_silver AS
        SELECT
            city,
            CAST(date AS DATE) AS date,
            temperature,
            humidity,
            wind_speed
        FROM read_parquet('data/**/*.parquet', hive_partitioning=true)
        WHERE city = '{city}'
          AND date = '{date}'
          AND temperature IS NOT NULL;
    """)

    row_count = con.execute(
        "SELECT COUNT(*) FROM tmp_silver"
    ).fetchone()[0]

    if row_count == 0:
        raise ValueError(f"Empty Silver partition: {city} - {date}")

    con.execute("""
        COPY tmp_silver
        TO 'silver'
        (FORMAT PARQUET, PARTITION_BY (city, date));
    """)

    con.execute("""
        INSERT OR REPLACE INTO pipeline_metadata
        VALUES ('silver', ?, ?, CURRENT_TIMESTAMP)
    """, [city, date])

    logger.info(f"Finished Silver partition: {city} - {date}")


def run(con):
    bronze = get_bronze_partitions(con)
    processed = get_processed_partitions(con)

    to_process = bronze - processed

    logger.info(f"{len(to_process)} partitions to process")

    for city, date in to_process:
        process_partition(con, city, date)
