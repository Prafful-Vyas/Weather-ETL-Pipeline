import logging
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

SILVER_SOURCE_PATH = "silver/**/*.parquet"


# ---------------------------------------------------------------------
# Partition Detection
# ---------------------------------------------------------------------

def get_silver_partitions(spark) -> set:
    """
    Detect available Silver partitions using hive partitioning.
    Handles missing silver folder safely.
    """
    try:
        df = (
            spark.read
            .option("basePath", "silver")
            .parquet(SILVER_SOURCE_PATH)
            .select("city", "date")
            .distinct()
        )

        return {(r.city, r.date) for r in df.collect()}

    except Exception:
        logger.warning("No Silver data found.")
        return set()


def get_processed_partitions(spark) -> set:
    df = spark.sql("""
        SELECT city, date
        FROM pipeline_metadata
        WHERE layer = 'gold'
    """)

    return {(r.city, r.date) for r in df.collect()}


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------

def validate_partition(df):

    row_count = df.count()

    if row_count == 0:
        raise ValueError("Empty Gold partition detected.")

    null_check = df.filter(F.col("avg_temp").isNull()).count()

    if null_check > 0:
        raise ValueError("Gold aggregation produced NULL averages.")


# ---------------------------------------------------------------------
# Processing Logic
# ---------------------------------------------------------------------

def process_partition(spark, city: str, date):

    logger.info(f"Processing Gold partition: {city} - {date}")

    silver_df = (
        spark.read
        .option("basePath", "silver")
        .parquet(SILVER_SOURCE_PATH)
        .filter((F.col("city") == city) & (F.col("date") == date))
    )

    tmp_gold = (
        silver_df.groupBy("city", "date")
        .agg(
            F.avg("temperature").alias("avg_temp"),
            F.max("temperature").alias("max_temp"),
            F.min("temperature").alias("min_temp"),
            F.count("*").alias("record_count")
        )
    )

    validate_partition(tmp_gold)

    (
        tmp_gold.write
        .mode("overwrite")
        .partitionBy("city", "date")
        .parquet("gold")
    )

    spark.sql(f"""
        INSERT INTO pipeline_metadata
        VALUES ('gold', '{city}', '{date}', CURRENT_TIMESTAMP)
    """)

    logger.info(f"Finished Gold partition: {city} - {date}")


# ---------------------------------------------------------------------
# Public Runner
# ---------------------------------------------------------------------

def run(spark, full_refresh: bool = False):

    logger.info("Starting Gold layer processing")

    available = get_silver_partitions(spark)

    if not available:
        logger.info("No Silver partitions available. Skipping Gold.")
        return

    if full_refresh:
        logger.info("Full refresh mode enabled")
        to_process = available
    else:
        processed = get_processed_partitions(spark)
        to_process = available - processed

    logger.info(f"{len(to_process)} Gold partitions to process")

    for city, date in to_process:
        process_partition(spark, city, date)

    logger.info("Gold layer completed")