
# LOAD
# INSERTION DANS MONGODB


from pymongo import MongoClient
from collections import defaultdict
import json
import os
from dotenv import load_dotenv  

load_dotenv()

print(f"AWS Key trouvée : {'Oui' if os.environ.get('AWS_ACCESS_KEY_ID') else 'Non'}")
print(f"MONGO_URI : {os.environ.get('MONGO_URI')}")

with open("data/stations_transformed.json", "r", encoding="utf-8") as f:
    documents = json.load(f)

print(f"OK : {len(documents)} documents charges depuis JSON")


def get_mongo_uri():
    # 1. On vérifie si on est dans Docker
    is_docker = os.path.exists('/.dockerenv')
    
    # 2. On récupère la valeur du .env
    env_uri = os.environ.get("MONGO_URI")
    
    if is_docker:
        # Dans Docker, on veut "mongodb://mongo:27017/"
        return env_uri if env_uri else "mongodb://mongo:27017/"
    else:
        # En local, on FORCE localhost, même si le .env dit "mongo"
        return "mongodb://localhost:27017/"


MONGO_URI = get_mongo_uri()
DB_NAME = "meteo_db"

def get_mongodb_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print("\nConnexion à MongoDB établie.")
    return db["stations_hourly"]

# Collection unique
stations_col = get_mongodb_collection()

# Vider la collection (dev uniquement)
stations_col.delete_many({})
print("OK Collection stations_hourly vidée")


# INSERTION
if documents:
    result = stations_col.insert_many(documents)
    print(f"OK {len(result.inserted_ids)} stations insérées avec hourly imbriquées")


# AJOUT : CRÉATION D'INDEX 
# Index unique sur l'ID station
stations_col.create_index("id", unique=True)
print("OK Index unique créé sur 'id'")

# Index sur les dates pour requêtes temporelles
stations_col.create_index("hourly.dh_utc")
print("OK Index créé sur 'hourly.dh_utc'")


