from pyspark.sql import functions as F
from utils import (
    clean_customers,
    clean_orders,
    clean_order_details,
    add_sous_total,
    clean_employees,
    clean_products
)

def build_enriched(df):
    
    # nettoyage et préparation de chaque table via les fonctions de utils.py
    df_customers = clean_customers(df["customers"])
    df_orders = clean_orders(df["orders"])
    df_order_details_clean = clean_order_details(df["order_details"])
    df_order_details = add_sous_total(df_order_details_clean)
    df_employees = clean_employees(df["employees"])
    df_products = clean_products(df["products"])
    
    # les tables categories et shippers restent telles quelles
    df_categories = df["categories"]
    df_shippers = df["shippers"]

    # ajout du nom de la catégorie à chaque produit
    df_products_cat = df_products.join(
        df_categories,
        df_products.category_id == df_categories.category_id,
        "left"
    ).select(
        df_products["*"],
        df_categories["category_name"]
    )

    # renommage des colonnes 
    df_customers_renamed = df_customers.withColumnRenamed("company_name", "customer_name") \
                                       .withColumnRenamed("country", "customer_country") \
                                       .withColumnRenamed("city", "customer_city")

    df_shippers_renamed = df_shippers.withColumnRenamed("company_name", "shipper_name")

    #jointure  des tables
    df_enriched = (
        df_order_details.join(df_orders, "order_id", "inner")
        .join(df_customers_renamed, "customer_id", "left")
        .join(df_products_cat, "product_id", "left")
        .join(df_employees, "employee_id", "left")
        .join(df_shippers_renamed, "shipper_id", "left")
    )

    #sélection finale selon le schéma exact requis
    df_final = df_enriched.select(
        "order_id",
        "customer_id",
        "employee_id",
        "product_id",
        "order_date",
        "required_date",
        "shipped_date",
        "freight",
        "is_shipped",
        "prix_unitaire",
        "quantite",
        "discount",
        "sous_total",
        "customer_name",
        "customer_country",
        "customer_city",
        "product_name",
        "category_name",
        "en_stock",
        "full_name",
        "shipper_name"
    )

    return df_final