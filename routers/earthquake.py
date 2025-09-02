from fastapi import APIRouter, Depends
from database.db import get_db
from database.postgres import Database

router = APIRouter(
    prefix="/earthquakes",
)

@router.get("")
async def get_latest_earthquakes(db: Database = Depends(get_db), limit: int = 10):
    all_books_query = f"SELECT * FROM bronze.earthquakes limit {limit}"
    results = db.fetch_all(all_books_query)
    print(results)
    books_list = [
        {"id": row[0], "title": row[1], "author": row[2], "description": row[3]}
        for row in results
    ]
    return books_list