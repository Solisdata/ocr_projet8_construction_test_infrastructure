from pymongo import MongoClient
import pprint

# Configuration (doit correspondre à votre script de chargement)
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = 'meteo_data'

def check_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 1. Vérifier les stations
    count_stations = db.stations.count_documents({})
    print(f"--- Collection 'stations' : {count_stations} document(s) ---")
    if count_stations > 0:
        pprint.pprint(db.stations.find_one())

    # 2. Vérifier les relevés (readings)
    count_readings = db.readings.count_documents({})
    print(f"\n--- Collection 'readings' : {count_readings} document(s) ---")
    if count_readings > 0:
        print("Exemple du dernier relevé inséré :")
        pprint.pprint(db.readings.find_one(sort=[('_id', -1)]))

if __name__ == "__main__":
    check_data()
