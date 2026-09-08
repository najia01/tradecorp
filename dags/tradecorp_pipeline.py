from airflow import DAG
from datetime import datetime, timedelta
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "tradecorp",
    "retries": 1,                          # réessayer 1 fois en cas d'échec
    "retry_delay": timedelta(minutes=5),   # attendre 5 min avant de réessayer
}

MOUNTS = [
    Mount(source="//c/wamp64/www/TradeCorp/src", target="/home/jovyan/src", type="bind"),
    Mount(source="//c/wamp64/www/TradeCorp/data", target="/home/jovyan/data", type="bind"),
    Mount(source="//c/wamp64/www/TradeCorp/.env", target="/home/jovyan/.env", type="bind")
]

with DAG(
    dag_id="tradecorp_etl_pipeline",       # nom unique du DAG
    default_args=default_args,
    start_date=datetime(2024, 1, 1),       # date à partir de laquelle il "existe"
    schedule_interval="0 6 * * *",         # cron : tous les jours à 6h
    catchup=False,                          # ne pas rattraper le passé
    tags=["tradecorp", "etl","spark"],
) as dag:

# Récupération du taux de change
    t1 = DockerOperator(
    task_id="fetch_exchange_rates",
    image="tradecorp-spark",                          # l'image à lancer
    command="spark-submit /home/jovyan/src/fetch_exchange_rates.py",
    docker_url="unix://var/run/docker.sock",          # accès au Docker de l'hôte
    network_mode="tradecorp_default",                 # même réseau que les autres services
    auto_remove=True,                                 # supprimer le conteneur à la fin
    mount_tmp_dir=False,
    mounts=MOUNTS,                                    # volumes src/, data/, .env
)
# Lecture des données
    t2 = DockerOperator(
    task_id="reader",
    image="tradecorp-spark",                          # l'image à lancer
    command="spark-submit /home/jovyan/src/reader.py",
    docker_url="unix://var/run/docker.sock",          # accès au Docker de l'hôte
    network_mode="tradecorp_default",                 # même réseau que les autres services
    auto_remove=True,                                 # supprimer le conteneur à la fin
    mount_tmp_dir=False,
    mounts=MOUNTS,                                    # volumes src/, data/, .env
)    
    # Transformation des données
    t3 = DockerOperator(
    task_id="transformer",
    image="tradecorp-spark",                          # l'image à lancer
    command="spark-submit /home/jovyan/src/transformer.py",
    docker_url="unix://var/run/docker.sock",          # accès au Docker de l'hôte
    network_mode="tradecorp_default",                 # même réseau que les autres services
    auto_remove=True,                                 # supprimer le conteneur à la fin
    mount_tmp_dir=False,
    mounts=MOUNTS,                                    # volumes src/, data/, .env
)
    # Ecriture et upload des données
    t4 = DockerOperator(
    task_id="writer",
    image="tradecorp-spark",                          # l'image à lancer
    command="spark-submit /home/jovyan/src/writer.py",
    docker_url="unix://var/run/docker.sock",          # accès au Docker de l'hôte
    network_mode="tradecorp_default",                 # même réseau que les autres services
    auto_remove=True,                                 # supprimer le conteneur à la fin
    mount_tmp_dir=False,
    mounts=MOUNTS,                                    # volumes src/, data/, .env
)
    
   # l'ordre d'exécution  
t1 >> t2 >> t3 >> t4                 