from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS         = "hdfs://namenode:9000"
SILVER_DECES = HDFS + "/datalake/silver/deces"
GOLD_DECES   = HDFS + "/datalake/gold/FAIT_DECES"
DWH_URL      = "jdbc:postgresql://host.docker.internal:5432/Cloud Healthcare Unit"
PG_USER, PG_PWD, PG_DRIVER = "postgres", "Test123", "org.postgresql.Driver"

DEPT_REGION = {
    **{d: "Auvergne-Rhône-Alpes"        for d in ["01","03","07","15","26","38","42","43","63","69","73","74"]},
    **{d: "Bourgogne-Franche-Comté"     for d in ["21","25","39","58","70","71","89","90"]},
    **{d: "Bretagne"                    for d in ["22","29","35","56"]},
    **{d: "Centre-Val de Loire"         for d in ["18","28","36","37","41","45"]},
    **{d: "Corse"                       for d in ["2A","2B","20"]},
    **{d: "Grand Est"                   for d in ["08","10","51","52","54","55","57","67","68","88"]},
    **{d: "Hauts-de-France"             for d in ["02","59","60","62","80"]},
    **{d: "Île-de-France"               for d in ["75","77","78","91","92","93","94","95"]},
    **{d: "Normandie"                   for d in ["14","27","50","61","76"]},
    **{d: "Nouvelle-Aquitaine"          for d in ["16","17","19","23","24","33","40","47","64","79","86","87"]},
    **{d: "Occitanie"                   for d in ["09","11","12","30","31","32","34","46","48","65","66","81","82"]},
    **{d: "Pays de la Loire"            for d in ["44","49","53","72","85"]},
    **{d: "Provence-Alpes-Côte d'Azur" for d in ["04","05","06","13","83","84"]},
    "971": "Guadeloupe", "972": "Martinique",
    "973": "Guyane",     "974": "La Réunion", "976": "Mayotte",
}


def main():
    spark = (SparkSession.builder
             .appName("CHU Gold — ajout FAIT_DECES")
             .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    rows = [(k, v) for k, v in DEPT_REGION.items()]
    ref = spark.createDataFrame(rows, ["departement", "region"])

    df = spark.read.parquet(SILVER_DECES)

    dept = F.when(
        F.substring("code_lieu_deces", 1, 2) == "97",
        F.substring("code_lieu_deces", 1, 3)
    ).otherwise(F.substring("code_lieu_deces", 1, 2))

    df = (df.withColumn("annee", F.year("date_deces"))
            .withColumn("departement", dept)
            .where(F.col("annee").isNotNull()))

    df = df.join(F.broadcast(ref), "departement", "left")
    df = df.na.fill({"region": "Inconnu"})

    df = df.withColumn("sexe",
        F.when(F.col("sexe") == "1", "Homme")
         .when(F.col("sexe") == "2", "Femme")
         .otherwise("Inconnu"))

    agg = (df.groupBy("annee", "region", "sexe")
             .agg(F.count("*").cast("int").alias("nb_deces")))

    agg.write.mode("overwrite").parquet(GOLD_DECES)

    (agg.write.format("jdbc")
        .option("url", DWH_URL).option("dbtable", '"GOLD_FAIT_DECES"')
        .option("user", PG_USER).option("password", PG_PWD).option("driver", PG_DRIVER)
        .mode("overwrite").save())

    n = spark.read.parquet(GOLD_DECES).count()
    print(f"  [GOLD] FAIT_DECES {n:>12} lignes -> Parquet + PostgreSQL(GOLD_FAIT_DECES)")

    print("\n=== Décès par région — 2019 ===")
    (agg.where(F.col("annee") == 2019)
        .groupBy("region")
        .agg(F.sum("nb_deces").alias("nb_deces"))
        .orderBy("nb_deces", ascending=False)
        .show(20, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
