import sys
import os
import logging
from pyspark.sql import SparkSession
from reader import read_raw_data
from transformer import build_enriched
from writer import clean_data

#configuration du système de journalisation 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

azure_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

if not azure_key:
    raise ValueError("Erreur : La clé Azure n'a pas été trouvée dans l'environnement.")

def run_pipeline():
        #initialisation de la SparkSession
    spark = SparkSession.builder \
        .appName("TradeCorp-Pipeline") \
        .config("fs.azure.account.key.najiastockage.dfs.core.windows.net", azure_key) \
        .getOrCreate()
        
    try:
        logging.info("--- DÉBUT DU PIPELINE TRADECORP ---")
        
        # Lecture des données brutes depuis ADLS Gen2
        logging.info("Étape 1 : Lecture des données")
        df = read_raw_data(spark)
        
        # Transformation et enrichissement
        logging.info("Étape 2 : Transformation et enrichissement")
        df_transformed = build_enriched(df)
        
        # Écriture des données nettoyées dans la zone clean
        logging.info("Étape 3 : Écriture au format Parquet")
        clean_data(df_transformed)
        
        logging.info("--- PIPELINE EXÉCUTÉ AVEC SUCCÈS ---")
        
    except Exception as e:
        logging.error(f"Erreur critique dans le pipeline : {e}")
        sys.exit(1)
        
    finally:
        # Arrêt propre de Spark (garanti même en cas d'échec)
        logging.info("Arrêt de la session Spark...")
        spark.stop()

if __name__ == "__main__":
    run_pipeline()