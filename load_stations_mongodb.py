# load_stations_mongodb.py

import json
import boto3
from pymongo import MongoClient
from datetime import datetime

# --- CONFIGURATION ---
# S3
s3 = boto3.client('s3')
bucket_name = 'ocr-projet8'
# test station amateur france
key = 'raw_meteo_data/meteo_station_amateur_france/2026_01_16_1768568074480_0.jsonl'

# Connexion MongoDB
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = 'meteo_data'
STATIONS_COLLECTION_NAME = 'stations'  # mettre dans la même collection
READINGS_COLLECTION_NAME = 'readings'  # Nouvelle collection pour les relevés

# --- INFORMATION SUR LA STATION ---
# Ce sont les détails statiques de la station.
# Ils seront insérés une seule fois dans la collection 'stations'.
station_metadata = {
    "id": "ILAMAD25",
    "name": "La Madeleine",
    "latitude": 50.659,
    "longitude": 3.07,
    "elevation": 23,
    "city": "La Madeleine",
    "state": "-/-",
    "hardware": "other",
    "software": "EasyWeatherPro_V5.1.6"
    }


# --- MAPPING DES CHAMPS ---
# Fait correspondre les noms de champs source (de S3) aux noms de champs cibles (infoclimat).
# a uniformisé avec le format du Json Infoclimat
FIELD_MAPPING = {
    "Time": "dh_utc",
    "Temperature": "temperature",
    "Pressure": "pression",
    "Humidity": "humidite",
    "Dew Point": "point_de_rosee",
    "Speed": "vent_moyen",
    "Gust": "vent_rafales",
    "Wind": "vent_direction",
    "Precip. Rate.": "pluie_1h",
    "Precip. Accum.": "pluie_3h",
}


def main():
    # --- CONNEXION ---
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    stations_collection = db[STATIONS_COLLECTION_NAME]
    readings_collection = db[READINGS_COLLECTION_NAME]
    print("Connexion à MongoDB établie.")

    # --- 1. UPSERT DES MÉTADONNÉES DE LA STATION ---
    stations_collection.update_one(
        {'station_id': station_metadata['id']},
        {'$set': station_metadata, '$setOnInsert': {'created_at': datetime.utcnow()}},
        upsert=True
    )
    print(f"Métadonnées de la station '{station_metadata['id']}' assurées dans '{STATIONS_COLLECTION_NAME}'.")

    # --- 2. TRAITEMENT DES RELEVÉS DEPUIS S3 ---
    print(f"Lecture du fichier {key} depuis S3...")
    response = s3.get_object(Bucket=bucket_name, Key=key)
    total_readings_inserted = 0
    batch_readings = []

    for line in response['Body'].iter_lines():
        if not line:
            continue

        try:
            data_origine = json.loads(line.decode('utf-8'))
            # Extraction directe de _airbyte_data (structure plate)
            data_reading = data_origine.get("_airbyte_data", {})

            if not data_reading:
                continue

            reading = {"id_station": station_metadata['id']}
            
            # Mapping des champs
            for source_field, target_field in FIELD_MAPPING.items():
                if source_field in data_reading:
                    reading[target_field] = data_reading[source_field]

            batch_readings.append(reading)

            if len(batch_readings) >= 100:
                readings_collection.insert_many(batch_readings)
                total_readings_inserted += len(batch_readings)
                print(f"  -> Inséré {len(batch_readings)} relevés.")
                batch_readings = []

        except json.JSONDecodeError:
            print(f"Erreur de décodage JSON pour la ligne: {line}")
        except Exception as e:
            print(f"Une erreur inattendue est survenue: {e}")
            
    # Insérer les derniers éléments restants
    if batch_readings:
        readings_collection.insert_many(batch_readings)
        total_readings_inserted += len(batch_readings)

    print(f"\nTerminé. Total de {total_readings_inserted} relevés insérés dans '{READINGS_COLLECTION_NAME}'.")


if __name__ == "__main__":
    main()