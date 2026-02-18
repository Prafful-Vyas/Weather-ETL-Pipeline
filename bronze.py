import os
import pandas as pd
from datetime import date

def save_raw(weather_data: dict):
    today = date.today().isoformat()

    for city, data in weather_data.items():
        if data is None:
            continue

        path = f"data/city={city}/date={today}"
        os.makedirs(path, exist_ok=True)

        df = pd.json_normalize(data["current"])

        df.to_parquet(f"{path}/weather.parquet", index=False)