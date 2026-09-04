from pyspark.sql import functions as F

def add_currency_column(df_main, df_country_currency, rates_dict, spark):
    
    #ajout de la devise selon le pays 
    df_avec_devise = df_main.join(
        df_country_currency,
        df_main["customer_country"] == df_country_currency["country"],
        "left"
    ).drop("country")
    
    #conversion du dictionnaire Python en DataFrame Spark
    #transforme {"EUR": 0.92, "GBP": 0.78} en liste [("EUR", 0.92), ("GBP", 0.78)]
    liste_taux = [(devise, float(taux)) for devise, taux in rates_dict.items()]
    df_taux = spark.createDataFrame(liste_taux, ["code_devise", "valeur_taux"])
    
    #ajout du taux de change 
    df_final = df_avec_devise.join(
        df_taux,
        df_avec_devise["currency"] == df_taux["code_devise"],
        "left"
    ).drop("code_devise")
    
    #sécurité : si une devise n'est pas trouvée, on met le taux à 1.0 (par défaut)
    df_final = df_final.fillna({"valeur_taux": 1.0})
    
    #calcul du montant final dans la devise locale
    df_final = df_final.withColumn(
        "sous_total_local", 
        F.round(F.col("sous_total") * F.col("valeur_taux"), 2)
    ).drop("valeur_taux")
    
    return df_final

