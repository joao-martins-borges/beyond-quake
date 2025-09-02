from fastapi import FastAPI
from dotenv import load_dotenv
import os

from pydantic import BaseModel

from routers.earthquake import router as earthquake_router
from database import postgres as db

load_dotenv()

app = FastAPI()

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

#INIT DATABASE
def initDatabase(db: db.Database): 
    queries = [
        '''
        CREATE SCHEMA IF NOT EXISTS bronze;
        ''',
        '''
        CREATE TABLE IF NOT EXISTS bronze.earthquakes (
            id SERIAL PRIMARY KEY,
            location VARCHAR(255) NOT NULL,
            magnitude FLOAT NOT NULL,
            depth FLOAT NOT NULL,
            timestamp VARCHAR(255) NOT NULL,
        );
        '''
    ]
    for query in queries:
        db.execute(query=query,params="")

initDatabase(db=monitoring_db)