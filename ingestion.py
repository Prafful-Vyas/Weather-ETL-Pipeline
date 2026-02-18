import httpx
import asyncio
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://api.open-meteo.com/v1/forecast"


async def fetch_weather(client: httpx.AsyncClient, city:str, lat: float, lon: float):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": [
            "temperature_2m",
            "wind_speed_10m",
            "wind_direction_10m",
            "weather_code"
        ]
    }

    retries = 3

    for attempt in range(retries):
        try:
            response = await client.get(BASE_URL, params=params)
            response.raise_for_status()
            return city, response.json()
        
        except (httpx.ConnectTimeout, httpx.ReadTimeout) as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)  # exponential backoff


async def fetch_multiple(cities: dict):
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [
            fetch_weather(client, city, lat, lon)
            for city, (lat, lon) in cities.items()
        ]
        results = await asyncio.gather(*tasks)

        # Convert list of tuples → dict
        return dict(results)

