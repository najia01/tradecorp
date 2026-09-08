import os
import requests
import json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Chargement des variables d'environnement depuis le fichier .env
load_dotenv('/home/jovyan/.env')

# Configuration de l'API et d'Azure
API_URL = "https://api.exchangerate-api.com/v4/latest/USD"
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "raw"
BLOB_NAME = "reference/exchange_rates.json"

def get_exchange_rates():
    try:
        response = requests.get(API_URL)

        if response.status_code == 200:
            # On retourne le texte brut du JSON, prêt pour l'upload
            return response.text
        else:
            print("Error:", response.status_code)
            return None
    except requests.exceptions.RequestException as e:
        print('Error:', e)
        return None

def main():
    # Appel de la fonction de récupération des taux
    exchange_rates = get_exchange_rates()

    # Si les données sont récupérées, on les envoie sur Azure
    if exchange_rates:
        donnees_json = json.loads(exchange_rates)
        nombre_devises = len(donnees_json.get('rates', {}))
        print(f"Succès : {nombre_devises} devises récupérées depuis l'API.")
        try:
            # On récupère directement la chaîne de connexion complète
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            
            # On utilise la méthode from_connection_string (plus simple !)
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
            
            blob_client.upload_blob(exchange_rates, overwrite=True)
            print(f'Succès : uploadés dans {CONTAINER_NAME}/{BLOB_NAME}')
            
        except Exception as e:
            print('Erreur lors de la connexion à Azure :', e)

if __name__ == '__main__':
    main()