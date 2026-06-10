# CHU — Cloud Healthcare Unit · Data Lake médaillon (HDFS + Spark)

Pipeline décisionnel santé **Big Data** suivant l'architecture **médaillon**
(Bronze → Silver → Gold) sur **HDFS + Spark** (Docker), avec **conformité RGPD**
et chargement final dans un **Data Warehouse PostgreSQL**.

## Architecture

```
SOURCES (CSV/FTP + base opérationnelle PostgreSQL)
        │  ingestion (hdfs put)
        ▼
BRONZE   HDFS /datalake/raw      ── fichiers bruts (hospitalisations, finess,
        │                            satisfaction, deces)
        │  bronze_to_silver.py (Spark)  ── nettoyage + RGPD
        ▼
SILVER   HDFS /datalake/silver   ── Parquet nettoyé (8 jeux, dont snapshot
        │                            opérationnel pour les consultations)
        │  silver_to_gold.py (Spark)    ── modélisation
        ▼
GOLD     HDFS /datalake/gold     ── constellation : 5 dimensions + 2 faits
        │  + JDBC
        ▼
DATA WAREHOUSE  PostgreSQL "Cloud Healthcare Unit"  ── tables GOLD_*  → Power BI
```

### Schéma en constellation (2 faits)
- **`FAIT_HOSPITALISATION`** et **`FAIT_CONSULTATION`**, partageant les dimensions
  `DIM_TEMPS`, `DIM_PATIENT`, `DIM_DIAGNOSTIC` (+ `DIM_ETABLISSEMENT` pour
  l'hospitalisation, `DIM_PROFESSIONNEL` pour la consultation).

### RGPD
Le filtrage des données personnelles (nom, prénom, n° sécu, e-mail, téléphone,
adresse) est appliqué **dès la couche Silver**, avant toute persistance.

## Prérequis
- **Docker Desktop** démarré (~4 Go RAM libres).
- **PostgreSQL local** avec :
  - base `postgres` = données opérationnelles (Consultation, Patient, Diagnostic,
    Professionnel) — source du fait consultation ;
  - base `Cloud Healthcare Unit` = entrepôt cible (tables `GOLD_*`).

## Exécution
```bash
cd datalake
docker compose up -d              # HDFS + Spark
bash ingest.sh                    # (ou ./ingest.ps1) -> /datalake/raw

# Pipeline médaillon
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.5.4 /app/bronze_to_silver.py

docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.5.4 /app/silver_to_gold.py

# Benchmark Parquet vs PostgreSQL
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.5.4 /app/benchmark_gold.py
python datalake/benchmark/plot_benchmark.py        # -> graphe PNG
````--packages org.postgresql:postgresql:42.5.4` est **indispensable** (driver JDBC).



## Arborescence
```
datalake/
├── docker-compose.yml     HDFS (namenode/datanode) + Spark (master/worker)
├── hadoop.env             configuration Hadoop/HDFS
├── ingest.sh / ingest.ps1 ingestion des sources -> HDFS /datalake/raw
├── README.md              guide détaillé du lac
├── spark/
│   ├── bronze_to_silver.py    Bronze -> Silver (nettoyage + RGPD)
│   ├── silver_to_gold.py      Silver -> Gold (constellation + PostgreSQL)
│   └── benchmark_gold.py      Parquet vs PostgreSQL
└── benchmark/
    └── plot_benchmark.py      graphe des temps de réponse
sources/                    données brutes (non versionnées)
```

## Documentation
- Guide du lac : [`datalake/README.md`](datalake/README.md)
- **Rapport d'architecture détaillé** (théorie + exemples factuels par étape,
  RGPD, benchmark, comparaison Hive/Spark/Impala) :
  [`docs/RAPPORT_ARCHITECTURE_MEDAILLON.md`](docs/RAPPORT_ARCHITECTURE_MEDAILLON.md)
