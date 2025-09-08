from fastapi import APIRouter, Depends
from database.db import get_db
from database.postgres import Database
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

router = APIRouter(
    prefix="/earthquakes",
)

@router.get("")
async def get_latest_earthquakes(schema: str = "bronze", db: Database = Depends(get_db), limit: int = 3):
    logging.info("Fetching latest %s earthquakes from schema '%s'", limit, schema)
    latest_earthquakes_query = f"SELECT * FROM {schema}.earthquakes order by timestamp desc limit {limit}"
    results = db.fetch_all(latest_earthquakes_query)
    books_list = [
        {"id": row[0], "location": row[1], "magnitude": row[2], "depth": row[3], "timestamp": row[4], "updated_utc": row[5]}
        for row in results
    ]
    logging.info("Found %s earthquakes", len(books_list))
    return books_list

@router.get("/{earthquake_id}")
async def get_earthquake_details(earthquake_id: str, schema: str = "bronze", db: Database = Depends(get_db)):
    logging.info("Fetching details for earthquake_id=%s from schema '%s'", earthquake_id, schema)
    earthquake_query = f"SELECT * FROM {schema}.earthquakes WHERE id = %s"
    result = db.fetch_one(earthquake_query, (earthquake_id,))
    if result:
        earthquake = {
            "id": result[0],
            "location": result[1],
            "magnitude": result[2],
            "depth": result[3],
            "timestamp": result[4],
            "updated_utc": result[5]
        }
        logging.info("Earthquake %s found", earthquake_id)
        return earthquake
    logging.warning("Earthquake %s not found", earthquake_id)
    return {"Error": f"Earthquake with id {earthquake_id} not found"}
