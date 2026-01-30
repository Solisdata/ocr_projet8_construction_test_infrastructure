from pymongo import MongoClient
import json
import os
from dotenv import load_dotenv
import boto3  

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION")
BUCKET_NAME = "ocr-projet8"
S3_KEY = "refined/stations_transformed.json"

print(f"AWS Key trouvée : {'Oui' if AWS_ACCESS_KEY_ID else 'Non'}")
print(f"MONGO_URI : {os.environ.get('MONGO_URI')}")

# --- S3 ---
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION
    )

def load_json_from_s3(bucket_name, key):
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = response["Body"].read().decode("utf-8")
    documents = json.loads(data)
    print(f"OK : {len(documents)} documents chargés depuis S3")
    return documents

# --- Mongo ---
def get_mongo_uri():
    is_docker = os.path.exists('/.dockerenv')
    env_uri = os.environ.get("MONGO_URI")
    if is_docker:
        return env_uri if env_uri else "mongodb://mongo:27017/"
    else:
        return "mongodb://localhost:27017/"

MONGO_URI = get_mongo_uri()
DB_NAME = "meteo_db"

def get_mongodb_collection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print("\nConnexion à MongoDB établie.")
    return db["stations_hourly"]

# --- Main ---
def main():
    # Charger les documents depuis S3
    documents = load_json_from_s3(BUCKET_NAME, S3_KEY)

    # Connexion Mongo
    stations_col = get_mongodb_collection()

    # Dev uniquement : vider la collection
    stations_col.delete_many({})
    print("OK Collection stations_hourly vidée")

    # Insertion
    if documents:
        result = stations_col.insert_many(documents)
        print(f"OK {len(result.inserted_ids)} stations insérées avec hourly imbriquées")

    # Création d'index
    stations_col.create_index("id", unique=True)
    stations_col.create_index("hourly.dh_utc")
    print("OK Index créés sur 'id' et 'hourly.dh_utc'")

if __name__ == "__main__":
    main()