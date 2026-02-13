import asyncio
import aiohttp
import pandas as pd
import logging
import os
import signal
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==========================================================
# CONFIGURATION
# ==========================================================

BASE_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3

LOCATIONS = [
    {"city": "London", "lat": 51.5074, "lon": -0.1278},
    {"city": "NewYork", "lat": 40.7128, "lon": -74.0060},
    {"city": "Tokyo", "lat": 35.6762, "lon": 139.6503},
    {"city": "Delhi", "lat": 28.6139, "lon": 77.2090},
]

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

CPU_COUNT = os.cpu_count()

# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("weather-etl")

# ==========================================================
# GRACEFUL SHUTDOWN
# ==========================================================

shutdown_event = asyncio.Event()

def handle_shutdown():
    logger.warning("Shutdown signal received...")
    shutdown_event.set()

signal.signal(signal.SIGINT, lambda s, f: handle_shutdown())
signal.signal(signal.SIGTERM, lambda s, f: handle_shutdown())

# ==========================================================
# EXTRACT (ASYNC + RETRY)
# ==========================================================

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
async def fetch_weather(session, location):
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "current_weather": "true"
    }

    async with session.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        data = await response.json()
        return location, data


async def extract_weather():
    connector = aiohttp.TCPConnector(limit=100)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_weather(session, loc) for loc in LOCATIONS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Extraction failed: {result}")
            else:
                valid_results.append(result)

        return valid_results


# ==========================================================
# TRANSFORM (CPU BOUND → MULTIPROCESSING)
# ==========================================================

def transform(record):
    location, weather_data = record
    current = weather_data.get("current_weather")

    if not current:
        raise ValueError(f"No current_weather for {location['city']}")

    return {
        "city": location["city"],
        "latitude": location["lat"],
        "longitude": location["lon"],
        "temperature": float(current["temperature"]),
        "windspeed": float(current["windspeed"]),
        "winddirection": float(current["winddirection"]),
        "weathercode": int(current["weathercode"]),
        "observation_time": current["time"],
        "ingestion_time": datetime.utcnow(),
    }


# ==========================================================
# LOAD (THREAD SAFE PARQUET WRITE)
# ==========================================================

def write_city_partition(df):
    date_part = datetime.utcnow().strftime("%Y-%m-%d")

    for city in df["city"].unique():
        city_df = df[df["city"] == city]

        partition_path = OUTPUT_DIR / f"city={city}" / f"date={date_part}"
        partition_path.mkdir(parents=True, exist_ok=True)

        file_path = partition_path / "weather.parquet"

        city_df.to_parquet(
            file_path,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        logger.info(f"Written → {file_path}")


# ==========================================================
# PIPELINE ORCHESTRATION
# ==========================================================

async def run_pipeline():
    logger.info("Starting extraction...")
    extracted = await extract_weather()

    if not extracted:
        logger.warning("No valid data extracted.")
        return

    logger.info("Starting transformation...")
    with ProcessPoolExecutor(max_workers=CPU_COUNT) as pool:
        transformed_records = list(pool.map(transform, extracted))

    df = pd.DataFrame(transformed_records)

    logger.info("Starting load phase...")
    with ThreadPoolExecutor(max_workers=4) as thread_pool:
        future = thread_pool.submit(write_city_partition, df)
        future.result()

    logger.info("Pipeline completed successfully.")


# ==========================================================
# ENTRY POINT
# ==========================================================

async def main():
    await run_pipeline()

if __name__ == "__main__":
    asyncio.run(main())
