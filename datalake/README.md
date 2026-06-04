# Data Lake HDFS — CHU

Couche **data lake** (zone d'atterrissage des données brutes distribuées) basée
sur **HDFS + Hive + Spark** (images Big Data Europe), en complément du Data
Warehouse PostgreSQL. Elle illustre l'architecture Big Data attendue : ingestion
de fichiers volumineux/hétérogènes, exploration SQL-sur-HDFS (Hive) et traitement
distribué (Spark).

```
Fichiers sources (FTP/CSV/dump)
        │  ingestion (hdfs put)
        ▼
   HDFS  /datalake/raw/{hospitalisation,finess,deces,satisfaction}
        │                         │
   Hive (SQL-on-HDFS)        Spark (traitement distribué + RGPD)
   exploration du lac             │  JDBC
                                  ▼
                       PostgreSQL  (Data Warehouse étoile)
```

## Prérequis
- **Docker Desktop démarré** (le daemon doit tourner).
- ~4 Go de RAM libres pour les conteneurs.

## 1. Démarrer le cluster
```bash
cd datalake
docker compose up -d
# Vérifier : http://localhost:9870 (HDFS)  ·  http://localhost:8080 (Spark)
```
Attendre ~1 min que le DataNode et Hive soient prêts (les conteneurs ont des
`SERVICE_PRECONDITION`).

## 2. Ingérer les sources dans HDFS
```bash
# Linux / Git Bash
bash ingest.sh
# Windows PowerShell
./ingest.ps1
```
Les fichiers de `../sources` (montés en lecture seule dans le namenode) sont
copiés dans `/datalake/raw/...`. Vérifier dans l'UI HDFS (Utilities → Browse).

## 3. Explorer le lac avec Hive (SQL-on-HDFS)
```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
# puis coller le contenu de hive/create_external_tables.sql
#               puis hive/explore_queries.sql
```
On interroge les fichiers HDFS **sans les copier** (schema-on-read).

## 4. Traiter le lac avec Spark → PostgreSQL
```bash
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.5.4 \
  /app/spark_to_postgres.py
```
Démontre : lecture HDFS → **RGPD (suppression nom/prénom)** → agrégation
distribuée → écriture dans PostgreSQL (table `lake_deces_par_annee`).

## 5. Arrêter
```bash
docker compose down      # conserve les données HDFS
docker compose down -v   # purge aussi les volumes HDFS
```

## Ports / interfaces
| Service | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| HDFS DataNode | http://localhost:9864 |
| HiveServer2   | http://localhost:10002 (JDBC : `localhost:10000`) |
| Spark Master  | http://localhost:8080 |

## Articulation avec le reste du projet
Le lac est la **zone brute** ; le Data Warehouse PostgreSQL (`../ETL`) reste la
**zone exploitée** (modèle en étoile). Deux chemins d'alimentation coexistent :
1. **ETL pandas** (`ETL/`) — lit la landing zone, applique le RGPD, charge l'étoile.
2. **Spark/Hive** — lit/explore directement HDFS (chemin « Big Data » distribué).
Dans les deux cas, le **filtrage RGPD précède** tout chargement.
