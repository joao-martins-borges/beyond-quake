import requests
import time
from datetime import datetime, timezone
import json

from database import postgres as db

class USGS:
    
    endpoint_by_frequency = {
        "hourly": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        "daily": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
        "weekly": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson",
        "monthly": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
        "yearly": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_year.geojson"
    }


    def __init__(self, interval: int = 10, db: db.Database = None, frequency: str = "hourly"):
        self.interval = interval
        self.endpoint = self.endpoint_by_frequency[frequency]
    
    def fetch_data(self):
        response = requests.get(self.endpoint)
        response.raise_for_status()
        return response.json()
    
    def parse_earthquakes(self, data):
        earthquakes = []
        for eq in data.get("features", []):
            props = eq["properties"]
            coords = eq["geometry"]["coordinates"]

            longitude, latitude, depth = coords
            magnitude = props["mag"]
            place = props["place"]
            timestamp = datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc)

            earthquakes.append({
                "location": place,
                "magnitude": magnitude,
                "depth_km": depth,
                "latitude": latitude,
                "longitude": longitude,
                "time_utc": timestamp,
            })
        return earthquakes

if __name__ == "__main__":
    usgs = USGS()
    data = usgs.fetch_data()
    print(json.dumps(data, indent=2))