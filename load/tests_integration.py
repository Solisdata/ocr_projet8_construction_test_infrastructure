import time
from pymongo import MongoClient
import os
from dotenv import load_dotenv  

load_dotenv()

def get_mongo_uri():
    is_docker = os.path.exists('/.dockerenv')
    env_uri = os.environ.get("MONGO_URI")
    
    if is_docker:
        return env_uri if env_uri else "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
    else:
        if env_uri and "mongo1" in env_uri:
            return "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
        elif env_uri:
            return env_uri
        else:
            return "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"

MONGO_URI = get_mongo_uri()
print(f" Utilisation de l'URI : {MONGO_URI}")

DB_NAME = os.environ.get("MONGO_DB", "meteo_db")
COLLECTION_NAME = "stations_hourly"

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000
)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def measure_query_time(query_func, description=""):
    start = time.time()
    cursor = query_func()
    results = list(cursor)
    end = time.time()
    print(f"{description} : {end-start:.4f}s, {len(results)} documents")
    return end-start

# Tests
measure_query_time(lambda: collection.find({}, {"id": 1}), "Toutes les stations")
measure_query_time(lambda: collection.find({"id": "ILAMAD25"}, {"hourly": 1}), "Hourly station ILAMAD25")
measure_query_time(lambda: collection.find({"hourly.dh_utc": {"$gte": "2024-01-23T12:00:00"}}), "Filtre par date")

client.close()