# ocr_projet8_construction_test_infrastructure

Le pipeline récupère des données météo depuis 3 sources différentes, les nettoie, les normalise et les charge dans MongoDB.

### Sources de données
- **Infoclimat** : Données pros déjà bien formatées
- **Station amateur France** (La Madeleine) : Données en unités US (°F, mph, etc.)
- **Station amateur Belgique** (Ichtegem) : Pareil, unités US

### Pipeline d'integration 
# Dans extract_transform/main_script_extract_transform.py

### Phase 1  :  EXTRACT (Récupération)
Je récupère les derniers fichiers depuis S3.
Je charge les fichiers JSONL.


### Phase 2 : TRANSFORM (Transformation)
### 1. Normaliser les unités
Les stations amateurs utilisent des unités US, je convertis tout en unités métriques :

```python
# normalize_hourly_amateur()
température_celsius = (température_fahrenheit - 32) * 5/9
vent_ms = vent_mph * 0.44704
pluie_mm = pluie_inches * 25.4
pression_hpa = pression_inHg * 33.8639
```

#### 2. Supprimer les doublons

```python
# remove_duplicates()
# Clé unique : id_station + timestamp (dh_utc)
```

#### 3. Nettoyer et convertir les types

```python
# clean_and_convert_hourly()
température = parse_float(row["temperature"])  # "20.5" → 20.5
humidité = parse_int(row["humidity"])          # "75" → 75
date = parse_datetime(row["dh_utc"])           # "2026-01-23 12:00:00" → datetime
```

**Gestion d'erreur :** Si une valeur est incohérente (genre "abc" au lieu de "20.5"), je mets `None`.

#### 4. Filtrer les valeurs aberrantes
J'enlève les températures aberrantes

#### 5. Regrouper par station
Je transforme une liste plate en dictionnaire par station :

```python
# Avant : [mesure1, mesure2, mesure3, ...]
# Après : {"ILAMAD25": [mesures...], "IICHTE19": [mesures...], ...}
```

#### 6. Construire le document final
Chaque station devient un document MongoDB avec toutes ses mesures imbriquées :

```python
{
    "id": "ILAMAD25",
    "name": "La Madeleine",
    "latitude": 50.659,
    "longitude": 3.07,
    "metadata": {...},
    "hourly": [
        {"dh_utc": "2026-01-23T00:00:00", "temperature": 15.2, ...},
        {"dh_utc": "2026-01-23T01:00:00", "temperature": 14.8, ...},
        ...
    ]
}
```

#### 7. Sauvegarder en JSON
Fichier intermédiaire `data/stations_transformed.json` 

---

### Phase 3 : LOAD (Chargement)

```python
# Dans load/load_mongo.py

# Je lis le JSON transformé

# Connexion MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["meteo_db"]
collection = db["stations_hourly"]


# Je créé les index pour la perf
collection.create_index("id", unique=True)           # Pas de doublons de stations
collection.create_index("hourly.dh_utc")             # Requêtes temporelles rapides
```

---

## 🧪 Les tests 

### Tests unitaires (avant Extract/Transform)

```bash
pytest -v extract_transform/test_extract_transform.py
```

**Ce que je teste :**
- `parse_float()` : Est-ce que "3.14" devient bien 3.14 ?
- `parse_int()` : Est-ce que "10" devient bien 10 ?
- `normalize_hourly_amateur()` : Est-ce que 68°F devient bien ~20°C ?
- `remove_duplicates()` : Est-ce que les doublons sont virés ?
- `clean_and_convert_hourly()` : Est-ce que les valeurs aberrantes sont supprimées ?

**Si un test fail → Le pipeline ne démarre pas.**


##  Comment lancer le pipeline : 
Avec l'orchestrateur 

```bash
python run_pipeline.py


## 📊 Métriques calculées

Le pipeline calcule automatiquement :

- **Taux de doublons** : Combien de lignes identiques j'ai supprimées
- **Taux d'erreur par champ** : Pour chaque colonne (température, pression, etc.), combien de valeurs sont `None`
- **Taux d'erreur global** : Sur toutes les valeurs, combien sont invalides

Exemple de sortie :
```
Taux d'erreur par champ:
temperature: 12 erreurs sur 5000 lignes (0.24%)
pression: 45 erreurs sur 5000 lignes (0.90%)
humidite: 8 erreurs sur 5000 lignes (0.16%)
...

✓ Taux d'erreur global : 1.23% (615/50000 valeurs invalides)
```

---