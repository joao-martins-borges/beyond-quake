from fastapi import APIRouter, Depends, Query, HTTPException
from database.db import get_db
from database.postgres import Database
import logging
from typing import List
from pydantic import BaseModel, Field
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class Earthquake(BaseModel):

    id: str = Field(..., description="USGS earthquake identifier")
    location: str = Field(..., description="Human-readable location description")
    magnitude: float = Field(..., ge=0, description="Richter magnitude")
    depth: float = Field(..., description="Depth in kilometers")
    timestamp: datetime = Field(..., description="Event time in UTC")
    updated_utc: datetime = Field(..., description="Last updated time in UTC")

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "us7000abcd",
                "location": "10 km SE of Townsville",
                "magnitude": 5.3,
                "depth": 12.1,
                "timestamp": "2025-01-01T12:34:56+00:00",
                "updated_utc": "2025-01-01T12:40:00+00:00",
            }
        }
    }


class ErrorDetail(BaseModel):
    detail: str = Field(..., description="Error message")

    model_config = {
        "json_schema_extra": {
            "example": {"detail": "Earthquake with id us7000abcd not found"}
        }
    }


router = APIRouter(
    prefix="/earthquakes",
    tags=["earthquakes"],
)

@router.get(
    "",
    response_model=List[Earthquake],
    summary="List latest earthquakes",
    description=(
        "Returns the most recent earthquakes from the bronze schema, ordered by time descending. "
        "Optionally filter by magnitude range using min_magnitude and/or max_magnitude parameters."
    ),
    responses={
        400: {"model": ErrorDetail, "description": "Invalid request parameters"},
        500: {"model": ErrorDetail, "description": "Internal server error"},
    },
)
async def get_latest_earthquakes(
    db: Database = Depends(get_db),
    limit: int = Query(
        3,
        ge=1,
        le=100,
        description="Maximum number of records to return (1-100)",
    ),
    min_magnitude: float = Query(
        None,
        ge=0,
        description="Minimum magnitude filter (inclusive)",
    ),
    max_magnitude: float = Query(
        None,
        ge=0,
        description="Maximum magnitude filter (inclusive)",
    ),
):
    where_conditions = []
    query_params = []
    
    if min_magnitude is not None:
        where_conditions.append("magnitude >= %s")
        query_params.append(min_magnitude)
    
    if max_magnitude is not None:
        where_conditions.append("magnitude <= %s")
        query_params.append(max_magnitude)
    
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    latest_earthquakes_query = f"SELECT * FROM bronze.earthquakes{where_clause} ORDER BY timestamp DESC LIMIT %s"
    query_params.append(limit)
    
    logging.info("Fetching latest %s earthquakes from bronze schema with filters: min_mag=%s, max_mag=%s", 
                limit, min_magnitude, max_magnitude)
    results = db.fetch_all(latest_earthquakes_query, query_params)
    earthquake_list = [
        {"id": row[0], "location": row[1], "magnitude": row[2], "depth": row[3], "timestamp": row[4], "updated_utc": row[5]}
        for row in results
    ]
    logging.info("Found %s earthquakes", len(earthquake_list))
    return earthquake_list

@router.get(
    "/{earthquake_id}",
    response_model=Earthquake,
    summary="Get earthquake by ID",
    description="Returns details for a single earthquake by its USGS identifier.",
    responses={
        404: {"model": ErrorDetail, "description": "Earthquake not found"},
        400: {"model": ErrorDetail, "description": "Invalid request parameters"},
        500: {"model": ErrorDetail, "description": "Internal server error"},
    },
)
async def get_earthquake_details(
    earthquake_id: str,
    db: Database = Depends(get_db),
):
    logging.info("Fetching details for earthquake_id=%s from bronze schema", earthquake_id)
    earthquake_query = "SELECT * FROM bronze.earthquakes WHERE id = %s"
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
    raise HTTPException(
        status_code=404, 
        detail=f"Earthquake with id {earthquake_id} not found"
    )
