import os
from pyspark.sql import SparkSession
from utils import download_blob_to_local, get_adls_client

LOCAL_RAW_DIR = "/home/jovyan/data/raw/"
FILES = [
    "categories.csv",
    "customers.csv",
    "employees.csv",
    "order_details.csv",
    "orders.csv",
    "products.csv",
    "shippers.csv",
    "suppliers.csv"
]

def lister_fichiers_azure(container_name="raw"):
    """Vérifie et liste le contenu du conteneur Azure."""
    blob_service_client = get_adls_client()
    container_client = blob_service_client.get_container_client(container_name)
    
    print("\n--- Ce que Python voit réellement sur Azure ---")
    try:
        blobs = container_client.list_blobs()
        for blob in blobs:
            print(f"Fichier détecté : '{blob.name}'")
    except Exception as e:
        print(f"Impossible d'accéder au conteneur : {e}")
    print("----------------------------------------------\n")

def download_raw_data(container_name="raw"):
    """Télécharge les fichiers CSV depuis le conteneur Azure vers le dossier local."""
    print("Téléchargement des fichiers depuis Azure en cours...")
    for fichier in FILES:
        print(f"-> Téléchargement de : '{fichier}'")
        local_path = os.path.join(LOCAL_RAW_DIR, fichier)
        download_blob_to_local(container_name, fichier, local_path)
    print("✅ Téléchargement terminé avec succès !\n")

def read_raw_data(spark):
    """Lit les fichiers CSV locaux dans des DataFrames PySpark."""
    dataframes = {}
    for nom in FILES:
        nom_sans_ext = nom.replace(".csv", "")
        fichier_path = os.path.join(LOCAL_RAW_DIR, nom)
        dataframes[nom_sans_ext] = spark.read.csv(fichier_path, header=True, inferSchema=True)
    return dataframes

def main():
    # Initialisation de la session Spark
    spark = SparkSession.builder.appName("TestReader").getOrCreate()
    
    # 1. Étape de diagnostic Azure
    lister_fichiers_azure("raw")
    
    try:
        # 2. Téléchargement des fichiers
        download_raw_data("raw")
        
        # 3. Lecture avec PySpark
        dfs = read_raw_data(spark)
        print(f"{len(dfs)} DataFrames Spark chargés avec succès !")
        
        # 4. Affichage de test pour prouver que les données sont valides
        if "customers" in dfs:
            print("\nAperçu de la table customers :")
            dfs["customers"].show(5)
            
    except Exception as e:
        print(f"ERREUR LORS DU PIPELINE : {e}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()