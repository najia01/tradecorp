import os
from azure.storage.blob import BlobServiceClient
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

# connexion à ADLS GEN2 

def get_adls_client():
    account_name = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    
# Construction  de l'URL Azure
    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=account_key)

def download_blob_to_local(container_name, blob_name, local_path):
    blob_service_client = get_adls_client()
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
# Téléchargement et écriture du fichier
    with open(local_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
        
# --------Fonctions de nettoyage des tables------------
def clean_customers(df):
    return( df
           .withColumn("company_name", F.trim(F.col("company_name")))
           .withColumn("contact_name", F.initcap(F.trim(F.col("contact_name"))))
           .withColumn("country", F.upper(F.trim(F.col("country"))))
           .dropDuplicates(["customer_id"])
        
    )
    
def clean_orders(df):
    return( df 
           .filter(F.col("shipped_date").isNotNull())
           .withColumn("order_date", F.col("order_date").cast(DateType()))
           .withColumn("required_date", F.col("required_date").cast(DateType()))
           .withColumn("shipped_date", F.col("shipped_date").cast(DateType()))
           .withColumn("freight", F.col("freight").cast(DoubleType()))
           .withColumnRenamed("ship_via","shipper_id")
           .withColumn("is_shipped", F.col("shipped_date").isNotNull())
)
    
def clean_order_details(df):
    return( df
           .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
           .withColumn("quantity", F.col("quantity").cast(IntegerType())) 
           .withColumn("discount", F.col("discount").cast(DoubleType()))
           .withColumnRenamed("unit_price","prix_unitaire")
           .withColumnRenamed("quantity","quantite")
           
)
    
def add_sous_total(df):
        return( df
           .withColumn("sous_total", F.round(F.col("prix_unitaire")* F.col("quantite")*(1-F.col("discount")),2))
 )

def clean_employees(df):
    return(df
           .withColumn("full_name", F.concat_ws(' ',F.col("first_name"),F.col("last_name")))
           .select("employee_id","first_name","last_name","title","hire_date","city","country","full_name")
)
    
def clean_products(df):
    return(df
           .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
           .withColumn("en_stock", F.col("units_in_stock") > 0)
)
        
