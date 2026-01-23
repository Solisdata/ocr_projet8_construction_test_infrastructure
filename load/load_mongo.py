
# LOAD
# INSERTION DANS MONGODB


from pymongo import MongoClient
from collections import defaultdict
import json

with open("data/stations_transformed.json", "r", encoding="utf-8") as f:
    documents = json.load(f)

print(f"✓ {len(documents)} documents chargés depuis JSON")

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME  = "meteo_db"

def get_mongodb_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print("\nConnexion à MongoDB établie.")
    return db["stations_hourly"]

# Collection unique
stations_col = get_mongodb_collection()

# Vider la collection (dev uniquement)
stations_col.delete_many({})
print("✓ Collection stations_hourly vidée")


# INSERTION
if documents:
    result = stations_col.insert_many(documents)
    print(f"✓ {len(result.inserted_ids)} stations insérées avec hourly imbriquées")


# AJOUT : CRÉATION D'INDEX 
# Index unique sur l'ID station
stations_col.create_index("id", unique=True)
print("✓ Index unique créé sur 'id'")

# Index sur les dates pour requêtes temporelles
stations_col.create_index("hourly.dh_utc")
print("✓ Index créé sur 'hourly.dh_utc'")


