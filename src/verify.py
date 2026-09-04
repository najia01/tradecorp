import os
from pyspark.sql import SparkSession

azure_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

spark = SparkSession.builder \
    .appName("Verification-Parquet") \
    .config("fs.azure.account.key.najiastockage.dfs.core.windows.net", azure_key) \
    .getOrCreate()

# Adaptez le nom du dossier final selon ce que vous avez configuré dans writer.py
chemin_clean = "abfss://clean@najiastockage.dfs.core.windows.net/build_enriched.parquet"

df_verif = spark.read.parquet(chemin_clean)

print("\n--- Vérification du Schéma ---")
df_verif.select("customer_country", "sous_total", "currency", "sous_total_local").printSchema()

print("\n--- Aperçu des 5 premières lignes ---")
df_verif.select("customer_country", "sous_total", "currency", "sous_total_local").show(5)

spark.stop()