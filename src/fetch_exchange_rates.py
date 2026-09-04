import os
import requests
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Chargement des variables d'environnement depuis le fichier .env
load_dotenv()

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
        try:
            account_name = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
            account_url = f"https://{account_name}.blob.core.windows.net"
            
            blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
            blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
            
            blob_client.upload_blob(exchange_rates, overwrite=True)
            print(f'Succès : uploadés dans {CONTAINER_NAME}/{BLOB_NAME}')
        except Exception as e:
            print('Erreur lors de la connexion à Azure :', e)

if __name__ == '__main__':
    main()