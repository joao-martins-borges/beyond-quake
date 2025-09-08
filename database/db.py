from database.postgres import Database
from fastapi import Depends
from dotenv import load_dotenv
import os

def get_db():
    db = Database(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", 5432))
    )
    try:
        yield db
    finally:
        db.close()
