import logging
import asyncio
from pyspark.sql import SparkSession

from metadata import initialize_metadata
import ingestion
import bronze
import silver
import gold

logging.basicConfig(level=logging.INFO)


CITIES = {
    "Delhi": (28.6139, 77.2090),
    "London": (51.5072, -0.1276),
    "NewYork": (40.7128, -74.0060),
    "Tokyo": (35.6762, 139.6503),
}


async def run_ingestion(spark):
    weather_data = await ingestion.fetch_multiple(CITIES)
    bronze.save_raw(spark, weather_data)


def create_spark():
    spark = (
        SparkSession.builder
        .appName("weather-pipeline")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .enableHiveSupport()   # optional but useful for SQL tables
        .getOrCreate()
    )
    return spark


def main():
    spark = create_spark()

    # initialize metadata tables
    initialize_metadata(spark)

    # ingestion layer
    asyncio.run(run_ingestion(spark))

    # transformations
    silver.run(spark)

    # final marts
    gold.run(spark, full_refresh=True)

    spark.stop()


if __name__ == "__main__":
    main()