import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
# initialisation de Spark une seule fois pour l'ensemble des tests du projet
    return SparkSession.builder.appName("TradeCorp-Tests").getOrCreate()