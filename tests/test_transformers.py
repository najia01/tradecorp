import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from utils import clean_orders, add_sous_total, clean_customers

# création des données factices
def test_suppression_lignes(spark):
    donnees =[
        {"order_date": "2023-01-01", "required_date": "2024-01-10", "shipped_date": "2023-01-05", "freight": 10.0, "ship_via": 1},
        {"order_date": "2024-01-03", "required_date": "2024-10-10", "shipped_date": None, "freight": 13.0, "ship_via": 2}
    ]
    
    df_test = spark.createDataFrame(donnees)
    
    # application de la fonction clean_orders
    df_resultat = clean_orders(df_test)
    
    # validation et vérification
    assert df_resultat.count() == 1
    
# TEST POUR QUESTION Q45 AVEC LA FONCTION ADD_SOUS_TOTAL
def test_add_sous_total(spark):
    calcul = [
        {"prix_unitaire" : 10, "quantite" : 2, "discount" : 0.10}
    ]
    
    df_test_calcul = spark.createDataFrame(calcul)
    df_calcul_total = add_sous_total(df_test_calcul)
    
    # renvoi du tableau sous forme de liste à Python et extraction de la 1ere ligne
    ligne_resultat = df_calcul_total.collect()[0]
    
    # certifie que la colonne sous_total a bien été créée et = 18
    assert ligne_resultat.sous_total == 18
    

# TEST POUR CLEAN_CUSTOMERS Q46
def test_clean_cutomers(spark):
    customers_inital = [
        {"customer_id": "ANTON", "company_name": "tradecorp" ,"contact_name": "  david dubois  " , "country": "france" }
    ]
    
    df_customers_data = spark.createDataFrame(customers_inital)
    df_customers_final = clean_customers(df_customers_data)
    
    ligne_resultat = df_customers_final.collect()[0]
    
    assert ligne_resultat.contact_name == "David Dubois"
    assert ligne_resultat.country == "FRANCE"
    
    
    
    