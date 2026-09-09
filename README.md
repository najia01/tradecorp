# TradeCorp ETL Pipeline - Data Platform

Bienvenue sur le dépôt de mon projet Data Engineering réalisé pour l'entreprise TradeCorp. Ce projet retrace l'évolution complète d'une infrastructure data : de l'analyse exploratoire initiale (Jalon 1) à l'industrialisation du code (Jalon 2), jusqu'à l'automatisation totale du pipeline dans le Cloud (Jalon 3).

L'objectif final était de lire des données brutes, d'appliquer des transformations métier complexes avec **PySpark**, de stocker les résultats dans **Azure Data Lake Storage Gen2 (ADLS)**, et d'orchestrer le tout sans aucune intervention humaine via **Apache Airflow** et **Docker**.

### Jalon 1 : Exploration et Prototypage (Jupyter Notebooks)

J'ai commencé par appréhender les données brutes et prototyper mes transformations directement dans des notebooks.

- Ingestion et Analyse (Notebook 1) : j'ai chargé les 8 tables relationnelles CSV dans PySpark et inspecté les schémas (`inferSchema=True`). Enfin, j'ai réalisé un test de performance croisé entre Pandas et Spark : cela m'a permis de valider que si Spark est indispensable pour l'échelle du Big Data, son approche distribuée ajoute un temps de latence sur les petits fichiers bruts.

- Nettoyage (Notebook 2) : j'ai traité les données colonne par colonne : suppression des commandes non livrées, cast des dates en `DateType`, calcul du `sous_total` et standardisation des chaînes de caractères (`TRIM`, `initcap`, `upper`).

- Jointures et agrégations (Notebook 3) : j'ai créé le DataFrame global `df_orders_enriched` en gérant les conflits de nommage, puis calculé les indicateurs clés (CA par client, palmarès) à l'aide de Window Functions.

- Validation (Notebook 4) : j'ai sauvegardé les données en Parquet avec un partitionnement par pays (`.partitionBy("customer_country")`) pour optimiser les requêtes, avant de les exporter vers une base PostgreSQL via JDBC.

### Jalon 2 : Industrialisation et Programmation Modulaire

Pour sortir du mode "prototype", j'ai refactorisé mon code en une architecture Python modulaire de production.

- Séparation des responsabilités : j'ai découpé mon code en plusieurs scripts (`utils.py` pour les règles métier, `reader.py` pour l'ingestion, `transformer.py` pour les jointures, `writer.py` pour l'export Azure, et `pipeline.py` pour l'orchestration globale).

- Enrichissement dynamique : j'ai ajouté un module croisant les commandes avec un fichier de référence pour déterminer la devise, en appliquant les taux de change du jour récupérés via une API. Un mécanisme de fallback (`fillna(1.0)`) sécurise les cas de pays inconnus.

- Qualité logicielle : j'ai mis en place des tests unitaires avec `pytest` pour valider ma logique métier, exécutés via `spark-submit` pour garantir la bonne initialisation de l'environnement Java/Spark.

### Jalon 3 : Automatisation et Orchestration (Apache Airflow)

La dernière étape a consisté à rendre le pipeline 100% autonome.

- Le DAG Airflow : j'ai créé le DAG `tradecorp_etl_pipeline` avec 4 tâches chaînées (`fetch_exchange_rates` -> `reader` -> `transformer` -> `writer`) s'exécutant dans des conteneurs éphémères grâce au `DockerOperator`.

- Le planning automatique et le paramètre `catchup` : en exemple nous avions une `start_date` ancienne qui débutait au 1er janvier 2024. Par défaut, Airflow aurait tenté de lancer rétroactivement des centaines de pipelines pour rattraper le temps perdu, ce qui aurait surchargé le système inutilement. En configurant explicitement `catchup=False`, j'ai indiqué au DAG d'ignorer les périodes passées et de planifier uniquement la prochaine exécution en direct, tous les jours à 6h du matin.

---

## Preuves d'exécution (Logs Airflow)

Pour vérifier le bon fonctionnement de mon orchestration, voici des extraits de mes logs Airflow prouvant le succès des tâches clés.

**1. Tâche `fetch_exchange_rates` (Récupération de l'API)**
Le log confirme l'exécution de mon script Python dans le conteneur Spark et la récupération réussie des devises :

```
[2026-09-08, 07:16:59 UTC] Executing the command: spark-submit /home/jovyan/src/fetch_exchange_rates.py
[2026-09-08, 07:17:02 UTC] Succès : 166 devises récupérées depuis l'API.
[2026-09-08, 07:17:03 UTC] Succès : uploadés dans raw/reference/exchange_rates.json
[2026-09-08, 07:17:03 UTC] Marking task as SUCCESS. dag_id=tradecorp_etl_pipeline, task_id=fetch_exchange_rates

```

(Source des logs : capture d'ecran Airflow)

**2. Tâche `writer` (Envoi vers Azure Data Lake)**
Le log prouve l'exécution finale de la chaîne d'écriture, gérée par le DockerOperator :

```
[2026-09-08, 07:17:27 UTC] Executing <Task(DockerOperator): writer> on 2026-09-07 06:00:00+00:00
[2026-09-08, 07:17:29 UTC] Executing the command: spark-submit /home/jovyan/src/writer.py
[2026-09-08, 07:17:32 UTC] Marking task as SUCCESS. dag_id=tradecorp_etl_pipeline, task_id=writer
[2026-09-08, 07:17:32 UTC] Task exited with return code 0

```

(Source des logs :capture d'écran Airflow)

---

## Lancement manuel (Commandes utiles)

Si vous souhaitez exécuter le projet sans passer par l'orchestrateur Airflow :

1. Lancer la suite de tests unitaires :

```bash
docker exec tradecorp_spark python -m pytest tests/test_transformers.py

```

2. Exécuter le pipeline complet de bout en bout :

```bash
docker exec tradecorp_spark spark-submit --packages org.apache.hadoop:hadoop-azure:3.3.2,com.microsoft.azure:azure-storage:8.6.6 /home/jovyan/src/pipeline.py

```
