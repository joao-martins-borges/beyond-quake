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

class Review(BaseModel):
    id: int
    location: str
    magnitude: float
    depth: float
    timestamp: str

#create database objects if they don't exist
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
            timestamp TIMESTAMPTZ NOT NULL,
            updated_utc TIMESTAMPTZ NOT NULL
        );
        '''
    ]
    for query in queries:
        db.execute(query=query,params=None)

#start fetching data, periodically, through the USGS API
async def fetch_and_ingest_loop():
    usgs = USGS(db=monitoring_db, interval=120)
    await usgs.run_polling()

#asynchronously start by loading the initial data in the database and fetching the data periodically
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database(db=monitoring_db)
    task = asyncio.create_task(fetch_and_ingest_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(
    lifespan=lifespan,
    title="Beyond Quake API",
    description=(
        "API for ingesting USGS earthquake data and querying recent events.\n\n"
        "- Use /earthquakes to list latest events.\n"
        "- Use /earthquakes/{earthquake_id} to get a single event."
    ),
    version="1.0.0",
    openapi_tags=[
        {
            "name": "earthquakes",
            "description": "Operations for listing and fetching earthquake events.",
        }
    ],
)
app.include_router(earthquake_router, prefix="")

if __name__ == "__main__":
    init_database(db=monitoring_db)