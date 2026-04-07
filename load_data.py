import os
import sys
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError

load_dotenv()

DEFAULT_BATCH_SIZE = 2000

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("MONGODB_DB")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION")

if not MONGODB_URI or not DB_NAME or not COLLECTION_NAME:
    raise RuntimeError("Missing MONGODB_URI in environment variables.")

def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value


def normalize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {key: clean_value(value) for key, value in record.items()}


def insert_batches(csv_path: str, batch_size: int = DEFAULT_BATCH_SIZE) -> None:
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    df = pd.read_csv(csv_path)

    total_inserted = 0
    batch: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        batch.append(normalize_record(row.to_dict()))

        if len(batch) >= batch_size:
            result = collection.insert_many(batch, ordered=False)
            total_inserted += len(result.inserted_ids)
            print(f"Inserted {total_inserted} records...")
            batch = []

    if batch:
        result = collection.insert_many(batch, ordered=False)
        total_inserted += len(result.inserted_ids)

    print(f"Done. Inserted {total_inserted} records into {DB_NAME}.{COLLECTION_NAME}")


def create_indexes() -> None:
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    collection.create_index("Make")
    print("Created index on Make")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python load_data.py <csv_path> [batch_size]")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    batch_size = int(sys.argv[2]) if len(sys.argv) >= 3 else DEFAULT_BATCH_SIZE

    try:
        insert_batches(csv_file_path, batch_size)
        create_indexes()
    except BulkWriteError as exc:
        print(f"Bulk write error: {exc.details}")
        sys.exit(1)
    except PyMongoError as exc:
        print(f"MongoDB error: {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"Unexpected error: {exc}")
        sys.exit(1)