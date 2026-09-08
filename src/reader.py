import os
from pyspark.sql import SparkSession
from utils import download_blob_to_local, get_adls_client
from dotenv import load_dotenv


load_dotenv('/home/jovyan/.env')


LOCAL_RAW_DIR = "/home/jovyan/data/raw/"
LOCAL_REF_DIR = "/home/jovyan/data/raw/reference/"
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

REFERENCE_FILES = [
    "reference/country_currency.csv",
    "reference/exchange_rates.json"
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
    "Télécharge les fichiers CSV depuis le conteneur Azure vers le dossier local."
    print("Téléchargement des fichiers depuis Azure en cours...")
    for fichier in FILES:
        print(f"-> Téléchargement de : '{fichier}'")
        local_path = os.path.join(LOCAL_RAW_DIR, fichier)
        download_blob_to_local(container_name, fichier, local_path)
    
    # création obligatoire du dossier dans le conteneur
    os.makedirs(LOCAL_REF_DIR, exist_ok=True)
        
    for ref_file in REFERENCE_FILES:
        print(f"-> Téléchargement de la référence : '{ref_file}'")
        # ref_file contient déjà "reference/..."
        local_path = os.path.join(LOCAL_RAW_DIR, ref_file)
        download_blob_to_local(container_name, ref_file, local_path)
        
    print("Téléchargement terminé avec succès !\n")

def read_raw_data(spark):
    # Lit les fichiers CSV locaux dans des DataFrames PySpark.
    dataframes = {}
    for nom in FILES:
        nom_sans_ext = nom.replace(".csv", "")
        fichier_path = os.path.join(LOCAL_RAW_DIR, nom)
        dataframes[nom_sans_ext] = spark.read.csv(fichier_path, header=True, inferSchema=True)
        
    return dataframes

def read_reference_data(spark):
    # "lecture des fichiers de référence (CSV et JSON) "
    ref_dfs = {}
    
    #lecture du mapping pays/devise (CSV)
    country_curr_path = os.path.join(LOCAL_RAW_DIR, "reference/country_currency.csv")
    ref_dfs["country_currency"] = spark.read.csv(country_curr_path, header=True, inferSchema=True)
    
    #lecture des taux de change (JSON)[cite: 1]
    exchange_rates_path = os.path.join(LOCAL_RAW_DIR, "reference/exchange_rates.json")
    ref_dfs["exchange_rates"] = spark.read.json(exchange_rates_path)
    
    return ref_dfs


def main():
    #initialisation de la session Spark
    spark = SparkSession.builder.appName("TestReader").getOrCreate()
    
    #étape de diagnostic Azure
    lister_fichiers_azure("raw")
    
    try:
        #téléchargement des fichiers
        download_raw_data("raw")
        
        #lecture avec PySpark
        dfs = read_raw_data(spark)
        ref_dfs = read_reference_data(spark)
        
        print(f"{len(dfs)} DF Spark et {len(ref_dfs)} DF de référence chargés avec succès !")
        
        #affichage de test pour prouver que les données sont valides
        if "customers" in dfs:
            print("\nAperçu de la table customers :")
            dfs["customers"].show(5)
            
        if "country_currency" in ref_dfs:
            print("\nAperçu de la table des devises :")
            ref_dfs["country_currency"].show(5)
            
    except Exception as e:
        print(f"ERREUR LORS DU PIPELINE : {e}")
        raise  # <-- Cette ligne est cruciale pour alerter Airflow !
    finally:
        spark.stop()

if __name__ == "__main__":
    main()