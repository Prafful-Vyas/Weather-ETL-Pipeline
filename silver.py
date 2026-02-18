import logging

logger = logging.getLogger(__name__)

bronze_path = "data/**/*.parquet"


def get_bronze_partitions(con):
    return set(con.execute(f"""
        SELECT DISTINCT city, date
        FROM read_parquet('{bronze_path}', hive_partitioning=true)
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
            STRPTIME(time, '%Y-%m-%dT%H:%M') AS timestamp,
            CAST(temperature_2m AS DOUBLE) AS temperature,
            CAST(wind_speed_10m AS DOUBLE) AS wind_speed,
            CAST(wind_direction_10m AS INTEGER) AS wind_direction,
            CAST(weather_code AS INTEGER) AS weather_code
        FROM read_parquet('{bronze_path}', hive_partitioning=true)
        WHERE city = '{city}'
          AND date = '{date}'
          AND temperature_2m IS NOT NULL
    """)

    row_count = con.execute(
        "SELECT COUNT(*) FROM tmp_silver"
    ).fetchone()[0]

    if row_count == 0:
        raise ValueError(f"Empty Silver partition: {city} - {date}")

    # Write partitioned silver data
    con.execute("""
        COPY tmp_silver
        TO 'silver'
        (FORMAT PARQUET, PARTITION_BY (city, date), OVERWRITE TRUE);
    """)

    # Update metadata
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
