"""Exemple de traitement Spark : Data Lake HDFS -> Data Warehouse PostgreSQL.

Démontre le chemin alternatif (moteur distribué) au pipeline pandas :
  1. lecture du registre des décès depuis HDFS,
  2. application RGPD (suppression des colonnes nom/prénom),
  3. agrégation distribuée (décès par année),
  4. écriture dans PostgreSQL (table de contrôle).

Soumission (depuis le conteneur spark-master) :
  docker exec -it spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.5.4 \
    /app/spark_to_postgres.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Colonnes directement identifiantes à retirer (RGPD) — cf. chu_config/pii_fields.py
PII_DECES = ["nom", "prenom", "numero_acte_deces"]

HDFS_DECES = "hdfs://namenode:9000/datalake/raw/deces"
# PostgreSQL tourne sur l'hôte Windows -> host.docker.internal depuis le conteneur
JDBC_URL = "jdbc:postgresql://host.docker.internal:5432/postgres"
JDBC_PROPS = {"user": "postgres", "password": "Test123", "driver": "org.postgresql.Driver"}


def main() -> None:
    spark = (SparkSession.builder
             .appName("CHU - Lac HDFS vers DWH")
             .getOrCreate())

    # 1) Lecture brute depuis le lac
    deces = spark.read.csv(HDFS_DECES, sep=",", header=True, inferSchema=False)

    # 2) RGPD : on supprime les identifiants directs AVANT tout traitement
    deces = deces.drop(*[c for c in PII_DECES if c in deces.columns])

    # 3) Agrégation distribuée : décès par année
    agg = (deces
           .withColumn("annee", F.substring("date_deces", 1, 4))
           .filter(F.col("annee").rlike("^[0-9]{4}$"))
           .groupBy("annee")
           .agg(F.count("*").alias("nb_deces"))
           .orderBy("annee"))

    agg.show(50, truncate=False)

    # 4) Écriture dans PostgreSQL
    (agg.write
        .mode("overwrite")
        .jdbc(JDBC_URL, "lake_deces_par_annee", properties=JDBC_PROPS))

    print("OK — table 'lake_deces_par_annee' écrite dans PostgreSQL depuis le lac HDFS.")
    spark.stop()


if __name__ == "__main__":
    main()
