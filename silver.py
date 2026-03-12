import logging
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

bronze_path = "data/**/*.parquet"


def get_bronze_partitions(spark):
    df = (
        spark.read
        .option("basePath", "data")
        .parquet(bronze_path)
        .select("city", "date")
        .distinct()
    )

    return {(row.city, row.date) for row in df.collect()}


def get_processed_partitions(spark):
    df = spark.sql("""
        SELECT city, date
        FROM pipeline_metadata
        WHERE layer = 'silver'
    """)

    return {(row.city, row.date) for row in df.collect()}


def process_partition(spark, city, date):

    logger.info(f"Processing Silver partition: {city} - {date}")

    bronze_df = (
        spark.read
        .option("basePath", "data")
        .parquet(bronze_path)
        .filter((F.col("city") == city) & (F.col("date") == date))
        .filter(F.col("temperature_2m").isNotNull())
    )

    silver_df = bronze_df.select(
        F.col("city"),
        F.col("date").cast("date").alias("date"),
        F.to_timestamp("time", "yyyy-MM-dd'T'HH:mm").alias("timestamp"),
        F.col("temperature_2m").cast("double").alias("temperature"),
        F.col("wind_speed_10m").cast("double").alias("wind_speed"),
        F.col("wind_direction_10m").cast("int").alias("wind_direction"),
        F.col("weather_code").cast("int").alias("weather_code")
    )

    row_count = silver_df.count()

    if row_count == 0:
        raise ValueError(f"Empty Silver partition: {city} - {date}")

    (
        silver_df.write
        .mode("overwrite")
        .partitionBy("city", "date")
        .parquet("silver")
    )

    spark.sql(f"""
        INSERT INTO pipeline_metadata
        VALUES ('silver', '{city}', '{date}', CURRENT_TIMESTAMP)
    """)

    logger.info(f"Finished Silver partition: {city} - {date}")


def run(spark):

    bronze = get_bronze_partitions(spark)
    processed = get_processed_partitions(spark)

    to_process = bronze - processed

    logger.info(f"{len(to_process)} partitions to process")

    for city, date in to_process:
        process_partition(spark, city, date)