from sys import monitoring
from fastapi import FastAPI
from dotenv import load_dotenv
import os
import asyncio
from contextlib import asynccontextmanager
from pydantic import BaseModel

from routers.earthquake import router as earthquake_router
from database import postgres as db
from ingestion.usgs import USGS

load_dotenv()

# DATABASE OBJECT DEFINITION
monitoring_db = db.Database(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

#PYDANTIC MODELS
class Review(BaseModel):
    id: int
    location: str
    magnitude: float
    depth: float
    timestamp: str

def init_database(db: db.Database): 
    queries = [
        '''
        CREATE SCHEMA IF NOT EXISTS bronze;
        ''',
        '''
        CREATE TABLE IF NOT EXISTS bronze.earthquakes (
            id TEXT PRIMARY KEY,
            location VARCHAR(255) NOT NULL,
            magnitude FLOAT NOT NULL,
            depth FLOAT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL
        );
        '''
    ]
    for query in queries:
        db.execute(query=query,params="")

async def fetch_and_ingest_loop():
    usgs = USGS(db=monitoring_db)
    while True:
        try:
            data = usgs.fetch_data()
            earthquakes = usgs.parse_earthquakes(data)
            usgs.ingest_earthquakes(earthquakes)
        except Exception as e:
            print(f"Error while fetching data from USGS API and ingesting: {e}")
        await asyncio.sleep(10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    
    task = asyncio.create_task(fetch_and_ingest_loop())
    yield
    
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

async def fetch_and_ingest_loop():
    usgs = USGS(db=monitoring_db, interval=120)
    await usgs.run_polling()

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(fetch_and_ingest_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan)
app.include_router(earthquake_router, prefix="")

if __name__ == "__main__":
    init_database(db=monitoring_db)