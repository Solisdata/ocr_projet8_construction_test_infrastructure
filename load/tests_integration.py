import time
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"  # Docker local
DB_NAME = "meteo_db"
COLLECTION_NAME = "stations_hourly"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

def measure_query_time(query_func, description=""):
    start = time.time()
    result = query_func()
    end = time.time()
    print(f"{description} : {end-start:.4f}s, {len(list(result))} documents")
    return end-start

# 1. Toutes les stations
measure_query_time(lambda: collection.find({}, {"id": 1}), "Toutes les stations")

# 2. Hourly d’une station
measure_query_time(lambda: collection.find({"id": "ILAMAD25"}, {"hourly": 1}), "Hourly station ILAMAD25")

# 3. Filtrage par date (ex: dernière mesure)
measure_query_time(lambda: collection.find({"hourly.dh_utc": {"$gte": "2026-01-23T12:00:00"}}), "Filtre par date")