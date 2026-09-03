# utilisation de la version nécessaire
FROM quay.io/jupyter/pyspark-notebook

# copie des fichiers locaux vers le conteneur 
COPY requirements.txt .

# installation des dépendances
RUN pip install --no-cache-dir -r requirements.txt

