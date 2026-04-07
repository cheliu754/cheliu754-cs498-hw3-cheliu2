import os
from typing import Any, Dict

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from pymongo.read_preferences import ReadPreference
from pymongo.write_concern import WriteConcern

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DB")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION")

if not MONGODB_URI or not DB_NAME or not COLLECTION_NAME:
    raise RuntimeError("Missing MONGODB_URI in environment variables.")


client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
base_collection: Collection = db[COLLECTION_NAME]

app = FastAPI(title="EV Consistency API")

@app.get("/")
def root() -> Dict[str, str]:
    return {"message": "EV Consistency API is running"}


@app.get("/health")
def health() -> Dict[str, str]:
    try:
        client.admin.command("ping")
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"MongoDB ping failed: {exc}")


@app.post("/insert-fast")
def insert_fast(payload: Dict[str, Any]) -> Dict[str, str]:
    try:
        fast_collection = base_collection.with_options(
            write_concern=WriteConcern(w=1)
        )
        result = fast_collection.insert_one(payload)
        return {"inserted_id": str(result.inserted_id)}
    except PyMongoError as exc:
        raise HTTPException(status_code=500, detail=f"insert-fast failed: {exc}")


@app.post("/insert-safe")
def insert_safe(payload: Dict[str, Any]) -> Dict[str, str]:
    try:
        safe_collection = base_collection.with_options(
            write_concern=WriteConcern(w="majority")
        )
        result = safe_collection.insert_one(payload)
        return {"inserted_id": str(result.inserted_id)}
    except PyMongoError as exc:
        raise HTTPException(status_code=500, detail=f"insert-safe failed: {exc}")


@app.get("/count-tesla-primary")
def count_tesla_primary() -> Dict[str, int]:
    try:
        primary_collection = base_collection.with_options(
            read_preference=ReadPreference.PRIMARY
        )
        count = primary_collection.count_documents({"Make": "TESLA"})
        return {"count": count}
    except PyMongoError as exc:
        raise HTTPException(status_code=500, detail=f"count-tesla-primary failed: {exc}")


@app.get("/count-bmw-secondary")
def count_bmw_secondary() -> Dict[str, int]:
    try:
        secondary_collection = base_collection.with_options(
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        count = secondary_collection.count_documents({"Make": "BMW"})
        return {"count": count}
    except PyMongoError as exc:
        raise HTTPException(status_code=500, detail=f"count-bmw-secondary failed: {exc}")