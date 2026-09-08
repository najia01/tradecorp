import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from transformer import build_enriched
from reader import read_raw_data

load_dotenv('/home/jovyan/.env')

def clean_data(df):
    try:
        # 1. Définir un chemin temporaire local dans le conteneur
        local_dir = "/tmp/build_enriched.parquet"
        
        # 2. Écrire le DataFrame en local au format Parquet
        df.write.mode("overwrite").parquet(local_dir)
        
        # 3. Se connecter à Azure avec le SDK Python
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = "clean"
        
        # 4. Nettoyage préalable des anciens blobs dans le dossier Azure
        container_client = blob_service_client.get_container_client(container_name)
        blobs_to_delete = container_client.list_blobs(name_starts_with="build_enriched.parquet/")
        for b in blobs_to_delete:
            container_client.delete_blob(b.name)
        
        # 5. Uploader chaque fichier généré vers Azure
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_dir)
                blob_name = f"build_enriched.parquet/{relative_path.replace(os.sep, '/')}"
                
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                
                with open(local_file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

        print("Succès : Les fichiers Parquet ont été uploadés avec succès via le SDK Python !")

    except Exception as e:
        print(f"ERREUR FATALE LORS DE L'ÉCRITURE : {e}")
        raise

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TradeCorpWriter").getOrCreate()
    
    # On passe bien 'spark' en argument à la fonction de lecture
    raw_dfs = read_raw_data(spark)
    df_transformed = build_enriched(raw_dfs)
    clean_data(df_transformed)