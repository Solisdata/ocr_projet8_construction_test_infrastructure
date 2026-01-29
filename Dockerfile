# Image Python officielle
FROM python:3.13-slim

# Répertoire de travail
WORKDIR /app

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY . .

# Commande par défaut
CMD ["python", "-m", "orchestration.run_pipeline"]