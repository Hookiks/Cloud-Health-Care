"""Benchmark de la couche GOLD — Parquet (HDFS) vs PostgreSQL (JDBC).

Mesure le temps de réponse de requêtes analytiques sur la constellation gold,
selon le moteur de stockage :
  • PARQUET  : Spark lit le Parquet HDFS et agrège (colonne, distribué) ;
  • POSTGRES : l'agrégation est poussée dans PostgreSQL (option JDBC `query`),
               Spark ne récupère que le résultat.

Sortie : tableau console + CSV /app/benchmark_results.csv (visualisé ensuite
par datalake/benchmark/plot_benchmark.py).

Soumission :
  docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.5.4 /app/benchmark_gold.py
"""
import time
import csv

from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:9000"
GOLD = HDFS + "/datalake/gold"
DWH_URL = "jdbc:postgresql://host.docker.internal:5432/Cloud Healthcare Unit"
PG_USER, PG_PWD, PG_DRIVER = "postgres", "Test123", "org.postgresql.Driver"

REPS = 5
RESULT_CSV = "/app/benchmark_results.csv"

# (libellé, requête Parquet [vues temp], requête PostgreSQL [tables GOLD_*])
CASES = [
    ("Consultations/sexe",
     "SELECT p.sexe, count(*) n FROM fait_consultation f "
     "JOIN dim_patient p ON f.patient_id=p.patient_id GROUP BY p.sexe",
     'SELECT p.sexe, count(*) n FROM "GOLD_FAIT_CONSULTATION" f '
     'JOIN "GOLD_DIM_PATIENT" p ON f.patient_id=p.patient_id GROUP BY p.sexe'),
    ("Consultations/annee",
     "SELECT t.annee, count(*) n FROM fait_consultation f "
     "JOIN dim_temps t ON f.date_key=t.date_key GROUP BY t.annee",
     'SELECT t.annee, count(*) n FROM "GOLD_FAIT_CONSULTATION" f '
     'JOIN "GOLD_DIM_TEMPS" t ON f.date_key=t.date_key GROUP BY t.annee'),
    ("Hospit/region",
     "SELECT e.region, count(*) n FROM fait_hospitalisation f "
     "JOIN dim_etablissement e ON f.finess=e.finess GROUP BY e.region",
     'SELECT e.region, count(*) n FROM "GOLD_FAIT_HOSPITALISATION" f '
     'JOIN "GOLD_DIM_ETABLISSEMENT" e ON f.finess=e.finess GROUP BY e.region'),
    ("Scan consultations",
     "SELECT count(*) n FROM fait_consultation",
     'SELECT count(*) n FROM "GOLD_FAIT_CONSULTATION"'),
]


def main():
    spark = (SparkSession.builder
             .appName("CHU Gold — Benchmark Parquet vs PostgreSQL")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Vues temporaires sur le Parquet gold (réévaluées à chaque requête)
    for tbl, view in [("FAIT_CONSULTATION", "fait_consultation"),
                      ("FAIT_HOSPITALISATION", "fait_hospitalisation"),
                      ("DIM_PATIENT", "dim_patient"), ("DIM_TEMPS", "dim_temps"),
                      ("DIM_ETABLISSEMENT", "dim_etablissement")]:
        spark.read.parquet(f"{GOLD}/{tbl}").createOrReplaceTempView(view)

    def t_parquet(sql):
        spark.sql(sql).collect()                       # warm-up
        s = time.perf_counter()
        for _ in range(REPS):
            spark.sql(sql).collect()
        return (time.perf_counter() - s) / REPS * 1000

    def t_postgres(sql):
        def run():
            (spark.read.format("jdbc").option("url", DWH_URL)
             .option("query", sql).option("user", PG_USER)
             .option("password", PG_PWD).option("driver", PG_DRIVER).load().collect())
        run()                                          # warm-up
        s = time.perf_counter()
        for _ in range(REPS):
            run()
        return (time.perf_counter() - s) / REPS * 1000

    rows = []
    print(f"\n{'Requete':<22}{'Parquet (ms)':>14}{'PostgreSQL (ms)':>18}")
    print("-" * 54)
    for label, q_parq, q_pg in CASES:
        p = t_parquet(q_parq)
        g = t_postgres(q_pg)
        rows.append((label, round(p, 1), round(g, 1)))
        print(f"{label:<22}{p:>14.1f}{g:>18.1f}")

    with open(RESULT_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["requete", "parquet_ms", "postgres_ms"])
        w.writerows(rows)
    print(f"\nResultats -> {RESULT_CSV}")
    spark.stop()


if __name__ == "__main__":
    main()
