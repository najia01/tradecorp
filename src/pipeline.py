import sys
import os
import logging
from pyspark.sql import SparkSession
from reader import download_raw_data, read_raw_data, read_reference_data
from transformer import build_enriched
from enrichment import add_currency_column
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
        download_raw_data("raw")
        df = read_raw_data(spark)
        ref_dfs = read_reference_data(spark)
        
        # Transformation et enrichissement
        logging.info("Étape 2 : Transformation et enrichissement")
        df_transformed = build_enriched(df)
        
        # Enrichissement
        logging.info("Étape 3 : Enrichissement avec devises et taux de change")
        
        # Transformation de la ligne JSON en dictionnaire Python
        ligne_taux = ref_dfs["exchange_rates"].select("rates").first()
        rates_dict = ligne_taux["rates"].asDict() if ligne_taux else {}
        
        df_final = add_currency_column(
            df_main=df_transformed, 
            df_country_currency=ref_dfs["country_currency"], 
            rates_dict=rates_dict, 
            spark=spark
        )
        
        # Écriture des données nettoyées dans la zone clean
        logging.info("Étape 4 : Écriture au format Parquet")
        clean_data(df_final)
        
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