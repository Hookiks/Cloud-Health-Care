from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000"
RAW = HDFS + "/datalake/raw"
SILVER = HDFS + "/datalake/silver"

PG_OP_URL = "jdbc:postgresql://host.docker.internal:5432/postgres"
PG_USER, PG_PWD, PG_DRIVER = "postgres", "Test123", "org.postgresql.Driver"

# RGPD 
PII_EXACT = {"tel", "nom", "prenom", "adresse", "email", "num_secu", "numsecu"}
PII_CONTAINS = ["telephone", "telecopie", "secu", "mail",
                "numero_acte", "voie", "prenom", "adresse"]


def is_pii(col: str) -> bool:
    c = col.lower()
    return c in PII_EXACT or any(tok in c for tok in PII_CONTAINS)


def apply_rgpd(df, label: str):
    drop = [c for c in df.columns if is_pii(c)]
    if drop:
        print(f"  [RGPD] {label}: colonnes supprimées -> {drop}")
    return df.drop(*drop)


def trim_strings(df):
    for name, typ in df.dtypes:
        if typ == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df


def write_silver(spark, df, name: str) -> None:
    path = f"{SILVER}/{name}"
    df.write.mode("overwrite").parquet(path)
    n = spark.read.parquet(path).count()  
    print(f"  [SILVER] {name:<16} {n:>10} lignes -> {path}")


def read_pg(spark, table_quoted: str):
    return (spark.read.format("jdbc")
            .option("url", PG_OP_URL)
            .option("dbtable", table_quoted)
            .option("user", PG_USER)
            .option("password", PG_PWD)
            .option("driver", PG_DRIVER)
            .load())


def main() -> None:
    spark = (SparkSession.builder
             .appName("CHU Médaillon")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("== Sources fichiers du lac (HDFS) ==")

    #Hospitalisations 
    h = (spark.read.option("header", "true").option("sep", ";")
         .option("encoding", "UTF-8").csv(f"{RAW}/hospitalisation"))
    h = (h.withColumnRenamed("Num_Hospitalisation", "num_hospitalisation")
           .withColumnRenamed("Id_patient", "id_patient")
           .withColumnRenamed("identifiant_organisation", "finess")
           .withColumnRenamed("Code_diagnostic", "code_diag")
           .withColumnRenamed("Suite_diagnostic_consultation", "suite_diagnostic")
           .withColumnRenamed("Date_Entree", "date_entree")
           .withColumnRenamed("Jour_Hospitalisation", "jours_hospitalisation"))
    h = (h.withColumn("num_hospitalisation", F.col("num_hospitalisation").cast("int"))
           .withColumn("id_patient", F.col("id_patient").cast("int"))
           .withColumn("jours_hospitalisation", F.col("jours_hospitalisation").cast("int"))
           .withColumn("date_entree", F.to_date("date_entree", "dd/MM/yyyy")))
    write_silver(spark, apply_rgpd(trim_strings(h), "hospitalisations"), "hospitalisations")

    # Établissements FINESS RGPD 
    e = (spark.read.option("header", "true").option("sep", ";")
         .option("encoding", "UTF-8").csv(f"{RAW}/finess"))
    write_silver(spark, apply_rgpd(trim_strings(e), "finess"), "finess")

    # Satisfaction 2019
    s = (spark.read.option("header", "true").option("sep", ";")
         .option("encoding", "UTF-8").csv(f"{RAW}/satisfaction"))
    write_silver(spark, trim_strings(s), "satisfaction")

    #Satisfaction 2020 e-Satis 
    s20 = (spark.read.option("header", "true").option("sep", ";")
           .option("encoding", "UTF-8").csv(f"{RAW}/satisfaction2020"))
    s20 = (trim_strings(s20)
           .withColumn("score_all_rea_ajust", F.col("score_all_rea_ajust").cast("double"))
           .withColumn("taux_reco_brut", F.col("taux_reco_brut").cast("double")))
    write_silver(spark, s20, "satisfaction2020")

    #Activité professionnelle
    act = (spark.read.option("header", "true").option("sep", ";")
           .option("encoding", "UTF-8").csv(f"{RAW}/activite"))
    act = (trim_strings(act)
           .where(F.col("identifiant_organisation").isNotNull())
           .withColumnRenamed("identifiant_organisation", "finess")
           .select("identifiant", "finess")
           .dropDuplicates(["identifiant"]))
    write_silver(spark, act, "activite")

    #Décès 
    d = (spark.read.option("header", "true").option("sep", ",")
         .option("encoding", "UTF-8").csv(f"{RAW}/deces"))
    d = (d.withColumn("date_deces", F.to_date("date_deces", "yyyy-MM-dd"))
           .withColumn("date_naissance", F.to_date("date_naissance", "yyyy-MM-dd")))
    write_silver(spark, apply_rgpd(trim_strings(d), "deces"), "deces")

    print("Sources opérationnelles")
    write_silver(spark, apply_rgpd(trim_strings(read_pg(spark, '"Patient"')), "patient"), "patient")
    write_silver(spark, trim_strings(read_pg(spark, '"Diagnostic"')), "diagnostic")
    write_silver(spark, apply_rgpd(trim_strings(read_pg(spark, '"Professionnel_de_sante"')), "professionnel"), "professionnel")
    write_silver(spark, apply_rgpd(trim_strings(read_pg(spark, '"Consultation"')), "consultation"), "consultation")

    print("Bronze -> Silver terminé.")
    spark.stop()


if __name__ == "__main__":
    main()
