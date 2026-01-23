# load_meteo_data_mongodb.py

import json
import boto3
from pymongo import MongoClient
from datetime import datetime
from collections import defaultdict
import json



# fonction pour prendre le dernier fichier chargé dans le bucket
def get_latest_s3_key(bucket_name, prefix):
    s3 = boto3.client("s3")

    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    if "Contents" not in response:
        raise ValueError(f"Aucun fichier trouvé dans {prefix}")

    latest_object = max(
        response["Contents"],
        key=lambda x: x["LastModified"]
    )

    print(f"✓ Dernier fichier détecté : {latest_object['Key']}")
    return latest_object["Key"]


# je charge le fichier
def load_s3_jsonl(bucket_name, key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=key)
    lines = response["Body"].read().decode("utf-8").splitlines()

    records = [json.loads(line) for line in lines]
    print(f"\n CHARGEMENT FICHIER {key}] ")
    print(f"{len(records)} lignes JSONL chargées")
    print("type fichier:",type(key))
    return records


#j'extrait les la partie qui m'interessent qui se trouvent dans airbyte_date
def extract_airbyte_data(rows, source_name=""):
    meteo_data = [row["_airbyte_data"] for row in rows]

    print(f"\n EXTRACT [{source_name}] ")
    print(f" type :", type(meteo_data))
    print(f" nb lignes :", len(meteo_data))
    print(f" clés :", meteo_data[0].keys())

    return meteo_data

##nettoyage de Hourly

## NORMALISATION de Hourly
from datetime import datetime

def normalize_hourly_amateur(df, station_id):
    """
    Transforme une liste de mesures amateurs en format standard (comme Infoclimat).
    - Nettoie les chaînes de caractères (°F, mph, in, % etc.)
    - Convertit en unités standard : °C, m/s, mm, hPa
    - Gère les valeurs manquantes en mettant None
    """
    
    df_normalized = []

    # correspondance clés source → clés standard
    key_map = {
        "Temperature": "temperature",
        "Pressure": "pression",
        "Humidity": "humidite",
        "Dew Point": "point_de_rosee",
        "Speed": "vent_moyen",
        "Gust": "vent_rafales",
        "Precip. Rate.": "pluie_1h",
        "Precip. Accum.": "pluie_3h",
        "Time": "dh_utc"
    }

    def clean_val(val, target_key):
        if val is None:
            return None
        val_str = str(val).replace("\xa0","").replace("%","").replace("°F","") \
                           .replace("in","").replace("mph","").replace("w/m²","").strip()
        try:
            v = float(val_str)
        except ValueError:
            return None

        # Conversion unités
        if target_key == "temperature" or target_key == "point_de_rosee":
            v = (v - 32) * 5/9       # °F → °C
        elif target_key in ["vent_moyen", "vent_rafales"]:
            v = v * 0.44704          # mph → m/s
        elif target_key in ["pluie_1h","pluie_3h"]:
            v = v * 25.4             # in → mm
        elif target_key == "pression":
            v = v * 33.8639          # inHg → hPa
        # humidite reste en %
        return v

    for row in df:
        doc = {"id_station": station_id}

        for src_key, target_key in key_map.items():
            val = row.get(src_key)
            if target_key == "dh_utc":
                # convertit Time en ISO string (HH:MM:SS)
                try:
                    doc[target_key] = datetime.strptime(val, "%H:%M:%S").isoformat()
                except Exception:
                    doc[target_key] = None
            else:
                doc[target_key] = clean_val(val, target_key)

        df_normalized.append(doc)

    print(f"\nNORMALISATION [{station_id}]")
    print(f"✓ {len(df_normalized)} lignes normalisées")
    print("Clés :", df_normalized[0].keys())
    print("Extrait :", df_normalized[0])  

    return df_normalized


#Modifications des données 

def remove_duplicates(records, unique_fields):
    # Crée un dictionnaire avec pour clé le tuple des champs uniques
    cleaned_dict = {tuple(record[f] for f in unique_fields): record for record in records}
    
    # Convertit les valeurs du dictionnaire en liste
    cleaned_list = list(cleaned_dict.values())
    
    print(f"\nRemove duplicate")
    print(f"✓ Total lignes avant suppression : {len(records)}")
    print(f"✓ Total lignes après suppression : {len(cleaned_list)}")
    print(f"✓ Doublons supprimés : {len(records) - len(cleaned_list)}")
    
    return cleaned_list



#pour les données horaires 

# ========== AJOUT : FONCTIONS DE PARSING ==========
def parse_float(value):
    """Convertit en float, retourne None si impossible"""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def parse_int(value):
    """Convertit en int, retourne None si impossible"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_datetime(value):
    """Convertit string en datetime MongoDB"""
    if not value:
        return None
    try:
        # Format de tes données : "2024-10-05 00:00:00"
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def clean_and_convert_hourly(hourly_list):
    """
    Nettoie, convertit les types ET filtre les valeurs aberrantes en UNE SEULE PASSE
    """
    cleaned_list = []
    
    for h in hourly_list:
        # Conversion de tous les champs en une fois
        converted = {
            "id_station": h.get("id_station"),
            "dh_utc": parse_datetime(h.get("dh_utc")),
            "temperature": parse_float(h.get("temperature")),
            "pression": parse_float(h.get("pression")),
            "humidite": parse_int(h.get("humidite")),
            "point_de_rosee": parse_float(h.get("point_de_rosee")),
            "visibilite": parse_float(h.get("visibilite")),
            "vent_moyen": parse_float(h.get("vent_moyen")),
            "vent_rafales": parse_float(h.get("vent_rafales")),
            "vent_direction": parse_int(h.get("vent_direction")),
            "pluie_1h": parse_float(h.get("pluie_1h")),
            "pluie_3h": parse_float(h.get("pluie_3h")),
            "neige_au_sol": parse_float(h.get("neige_au_sol")),
            "nebulosite": parse_int(h.get("nebulosite")),
            "temps_omm": h.get("temps_omm")
        }
        
        # Vérification des valeurs aberrantes (température)
        temp = converted["temperature"]
        if temp is None or -50 <= temp <= 50:
            cleaned_list.append(converted)
        else:
            print(f"⚠ Température aberrante supprimée : {converted['id_station']} "
                  f"{converted['dh_utc']} = {temp}°C")
    
    print(f"\n🧹 CLEANING + CONVERSION")
    print(f"  Lignes initiales : {len(hourly_list)}")
    print(f"  Lignes après cleaning : {len(cleaned_list)}")
    
    return cleaned_list


#enregistrement en json :

# Fonction pour rendre tout JSON-serializable
def make_serializable(obj):
    if isinstance(obj, list):
        return [make_serializable(o) for o in obj]
    elif isinstance(obj, dict):
        return {k: make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj



def main():
    # EXTRACT CONFIGURATION CONNEXION SOURCE S3

    BUCKET_NAME = "ocr-projet8"
    df1_infoclimat_prefix = "raw_meteo_data/ocr_infoclimat/"
    df2_amateur_france = "raw_meteo_data/meteo_station_amateur_france/"
    df3_amateur_belgique = "raw_meteo_data/meteo_station_amateur_belgique/"

    df1_infoclimat_key = get_latest_s3_key(BUCKET_NAME, df1_infoclimat_prefix)
    df2_amateur_france_key = get_latest_s3_key(BUCKET_NAME, df2_amateur_france)
    df3_amateur_belgique_key = get_latest_s3_key(BUCKET_NAME, df3_amateur_belgique)

    df1_infoclimat = load_s3_jsonl(BUCKET_NAME, df1_infoclimat_key)
    df2_amateur_france = load_s3_jsonl(BUCKET_NAME, df2_amateur_france_key)
    df3_amateur_belgique = load_s3_jsonl(BUCKET_NAME, df3_amateur_belgique_key)

    meteo_infoclimat = extract_airbyte_data(df1_infoclimat, "infoclimat")
    meteo_france_amateur = extract_airbyte_data(df2_amateur_france, "france_amateur")
    meteo_belgique_amateur = extract_airbyte_data(df3_amateur_belgique, "belgique_amateur")

    #j'extrait les données qui m'interessent (metadata, stations et hourly)
    stations = [] #liste de dictionnaires : description des stations
    hourly_list = [] #dictionnaire de listes par stations --> transformé en liste de dictionnaires
    metadata = {} # dictionnaire unique : 

    for i in meteo_infoclimat:
        # Stations
        stations.extend(i.get("stations", [])) #Pour chaque liste du JSON, on prend les dictionnaires qui sont dans stations quand on trouve la clé stations
        
        # Hourly
        hourly_dict = i.get("hourly", {})
        for station_id, measures in hourly_dict.items():
            if station_id == "_params":  # ignorer les paramètres généraux
                continue
            for measure in measures:
                # Ajouter l'id de la station à chaque mesure
                # measure["station_id"] = station_id
                hourly_list.append(measure)
            #on parcourt toutes les stations dans hourly  et on accumule toutes les mesures dans une liste globale en on ajoute l’identifiant de la station à chaque mesure,

        # Metadata
        metadata = i.get("metadata", {})

    print("\n CLE INFOCLIMAT")
    print("clé stations:", stations[0].keys())
    print(f"✓ Nombre de lignes stations chargées : {len(stations)}") 
    print("clé hourly:", hourly_list[0].keys())
    print(f"✓ Nombre de lignes horaires chargées : {len(hourly_list)}") 


    #STATIONS AMATEURS

    ## j'ajoute les infos sur la station à la collection stations
    stations.append({
        "id": "ILAMAD25",
        "name": "La Madeleine",
        "latitude": 50.659,
        "longitude": 3.07,
        "elevation": 23,
        "city": "La Madeleine",
        "state": "-/-",
        "hardware": "other",
        "software": "EasyWeatherPro_V5.1.6"
    })

    stations.append({
        "id": "IICHTE19",
        "name": "WeerstationBS",
        "latitude": 51.092,
        "longitude": 2.999,
        "elevation": 15,
        "city": "Ichtegem",
        "state": "-/-",
        "hardware": "other",
        "software": "EasyWeatherV1.6.6"
    })


    hourly_france = normalize_hourly_amateur(meteo_france_amateur, "ILAMAD25")
    hourly_belgique = normalize_hourly_amateur(meteo_belgique_amateur, "IICHTE19")

    # Suppression des doublons dans hourly_list
    hourly_list = remove_duplicates(hourly_list, ["id_station", "dh_utc"])
    hourly_france = remove_duplicates(hourly_france, ["id_station", "dh_utc"])
    hourly_belgique = remove_duplicates(hourly_belgique, ["id_station", "dh_utc"])

    # Pour les stations
    stations = remove_duplicates(stations, ["id"])

    hourly_list_cleaned = clean_and_convert_hourly(hourly_list)
    hourly_france_cleaned = clean_and_convert_hourly(hourly_france)
    hourly_belgique_cleaned = clean_and_convert_hourly(hourly_belgique)

    # Combiner toutes les données dans une seule liste
    all_hourly = hourly_list_cleaned + hourly_france_cleaned + hourly_belgique_cleaned

    print(f"Total mesures horaires à insérer : {len(all_hourly)}")

    # ----------------------------
    # REGROUPEMENT DES HOURLY PAR STATION
    # ----------------------------
    hourly_by_station = defaultdict(list)

    for h in all_hourly:
        hourly_data = {k: v for k, v in h.items() if k != "id_station"}
        hourly_by_station[h["id_station"]].append(hourly_data)

    print("✓ Hourly regroupées par station")

    # ----------------------------
    # CONSTRUCTION DES DOCUMENTS STATION
    # ----------------------------
    documents = []

    for station in stations:
        doc = {
            **station,                  # infos station
            "metadata": metadata,       # metadata snapshot
            "hourly": hourly_by_station.get(station.get("id"), [])
        }
        documents.append(doc)

    print(f"✓ Documents stations prêts : {len(documents)}")
    # Conversion + sauvegarde
    documents_serializable = make_serializable(documents)

    with open("data/stations_transformed.json", "w", encoding="utf-8") as f:
        json.dump(documents_serializable, f, ensure_ascii=False, indent=2)

    print(f"✓ JSON sauvegardé avec {len(documents_serializable)} documents")

if __name__ == "__main__":
    main()