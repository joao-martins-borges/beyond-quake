import requests
from datetime import datetime, timezone, timedelta
import asyncio

from database import postgres as db

class USGS:
    
    endpoint = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}&endtime={end}"


    def __init__(self, interval: int = 120, db: db.Database = None):
        self.interval = interval
        self.db = db
        self.last_timestamp = None
    
    def fetch_data(self, start_time: str, end_time: str):
        url = self.endpoint.format(start=start_time, end=end_time)
        response = requests.get(url)
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
            id = eq["id"]

            event_time = datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc)

            updated_time = props.get("updated")
            if updated_time:
                updated_time = datetime.fromtimestamp(updated_time / 1000, tz=timezone.utc)
            else:
                updated_time = event_time

            earthquakes.append({
                "id": id,
                "location": place,
                "magnitude": magnitude,
                "depth_km": depth,
                "time_utc": event_time,
                "updated_utc": updated_time
            })
        return earthquakes
    
    def ingest_earthquakes(self, earthquakes):
        for earthquake in earthquakes:
            if not self.last_timestamp or earthquake["updated_utc"] > self.last_timestamp:
                self.db.insert_earthquake(earthquake)

                if not self.last_timestamp or earthquake["updated_utc"] > self.last_timestamp:
                    self.last_timestamp = earthquake["updated_utc"]

    #For demonstration and testing purposes
    def initial_load(self):
        start_time = datetime(2025, 9, 1, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        end_time = datetime(2025, 9, 6, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        try:
            data = self.fetch_data(start_time=start_time, end_time=end_time)
            earthquakes = self.parse_earthquakes(data)

            if earthquakes:
                self.ingest_earthquakes(earthquakes)
                print(f"Initial load completed: {len(earthquakes)} earthquakes ingested for June 2025.")
            else:
                print("No earthquakes found for June 2025.")
        except Exception as e:
            print(f"Error during initial load: {e}")

    async def run_polling(self):
        self.initial_load()
        while True:
            try:
                start_time = (
                    (self.last_timestamp - timedelta(seconds=600)).strftime("%Y-%m-%dT%H:%M:%S")
                    if self.last_timestamp
                    else (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S")
                )
                end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

                data = self.fetch_data(start_time=start_time, end_time=end_time)
                earthquakes = self.parse_earthquakes(data)

                if earthquakes:
                    self.ingest_earthquakes(earthquakes)
                    print(f"Ingested {len(earthquakes)} earthquakes. Last timestamp: {self.last_timestamp}")
                else:
                    print("No new earthquakes.")
            except Exception as e:
                print(f"Error fetching earthquakes: {e}")

            await asyncio.sleep(self.interval)