import duckdb
import logging
import asyncio

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


async def run_ingestion():
    weather_data = await ingestion.fetch_multiple(CITIES)
    bronze.save_raw(weather_data)


def main():
    con = duckdb.connect("pipeline.duckdb")

    initialize_metadata(con)

    asyncio.run(run_ingestion())

    silver.run(con)

    gold.run(con, full_refresh=True)


if __name__ == "__main__":
    main()
