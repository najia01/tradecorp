def clean_data (df):
# définition du chemin d'accès ABFSS
    adls_path = "abfss://clean@najiastockage.dfs.core.windows.net/build_enriched.parquet"

# écriture du DataFrame au format parquet
    df.write.mode("overwrite").parquet(adls_path)