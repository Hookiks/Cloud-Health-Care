"""Médaillon — étape SILVER -> GOLD (Spark).

Lit les Parquet Silver et construit un schéma en CONSTELLATION à 4 faits :
  • FAIT_HOSPITALISATION  (lac : hospitalisations + finess)
  • FAIT_CONSULTATION     (opérationnel : consultation ; établissement reconstruit
                           via activite_professionnel_sante -> besoin n°1)
  • FAIT_SATISFACTION     (e-Satis 2020, par région -> besoin n°8)
  • FAIT_DECES            (ajouté par add_fait_deces.py -> besoin n°7)
partageant les dimensions DIM_TEMPS, DIM_PATIENT, DIM_DIAGNOSTIC
(+ DIM_ETABLISSEMENT pour hospitalisation/consultation, DIM_PROFESSIONNEL pour la consultation).

Écrit le résultat en PARQUET dans /datalake/gold/<table>/ puis le charge dans
PostgreSQL "Cloud Healthcare Unit" via JDBC (tables préfixées GOLD_, pour ne pas
écraser le schéma relationnel construit par l'ETL pandas).

Soumission :
  docker exec spark-master spark-submit \
    --master spark://spark-master:7077 /app/silver_to_gold.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://namenode:9000"
SILVER = HDFS + "/datalake/silver"
GOLD = HDFS + "/datalake/gold"

# Data Warehouse cible (nom de base avec espaces -> accepté littéralement par le driver)
DWH_URL = "jdbc:postgresql://host.docker.internal:5432/Cloud Healthcare Unit"
PG_USER, PG_PWD, PG_DRIVER = "postgres", "Test123", "org.postgresql.Driver"

# Référentiel département -> région (découpage 2016, métropole + DOM)
_REGIONS = {
    "Auvergne-Rhône-Alpes": ["01","03","07","15","26","38","42","43","63","69","73","74"],
    "Bourgogne-Franche-Comté": ["21","25","39","58","70","71","89","90"],
    "Bretagne": ["22","29","35","56"],
    "Centre-Val de Loire": ["18","28","36","37","41","45"],
    "Corse": ["2A","2B","20"],
    "Grand Est": ["08","10","51","52","54","55","57","67","68","88"],
    "Hauts-de-France": ["02","59","60","62","80"],
    "Île-de-France": ["75","77","78","91","92","93","94","95"],
    "Normandie": ["14","27","50","61","76"],
    "Nouvelle-Aquitaine": ["16","17","19","23","24","33","40","47","64","79","86","87"],
    "Occitanie": ["09","11","12","30","31","32","34","46","48","65","66","81","82"],
    "Pays de la Loire": ["44","49","53","72","85"],
    "Provence-Alpes-Côte d'Azur": ["04","05","06","13","83","84"],
    "Guadeloupe": ["971"], "Martinique": ["972"], "Guyane": ["973"],
    "La Réunion": ["974"], "Mayotte": ["976"],
}

# Normalisation des libellés région du fichier e-Satis 2020 vers les libellés canoniques
_SAT_ALIAS = {
    "Hauts de France": "Hauts-de-France",
    "Ile de France": "Île-de-France",
    "Nouvelle Aquitaine": "Nouvelle-Aquitaine",
    "PACA": "Provence-Alpes-Côte d'Azur",
    "Océan Indien": "La Réunion",
    "Ocean Indien": "La Réunion",
}


def write_gold(spark, df, name: str) -> None:
    """Écrit une table gold : Parquet (HDFS) + PostgreSQL (JDBC)."""
    path = f"{GOLD}/{name}"
    df.write.mode("overwrite").parquet(path)
    n = spark.read.parquet(path).count()
    (df.write.format("jdbc")
       .option("url", DWH_URL).option("dbtable", f'"GOLD_{name}"')
       .option("user", PG_USER).option("password", PG_PWD).option("driver", PG_DRIVER)
       .mode("overwrite").save())
    print(f"  [GOLD] {name:<22} {n:>9} lignes -> Parquet + PostgreSQL(GOLD_{name})")


def region_df(spark):
    rows = [(dep, reg) for reg, deps in _REGIONS.items() for dep in deps]
    return spark.createDataFrame(rows, ["departement", "region"])


# DOM (zone « DOM », le reste « Métropole ») — pour DIM_LOCALISATION
_DOM = {"Guadeloupe", "Martinique", "Guyane", "La Réunion", "Mayotte"}


def localisation_df(spark):
    """DIM_LOCALISATION au grain région (clé naturelle = region), partagée par
    FAIT_DECES et FAIT_SATISFACTION."""
    regions = sorted(set(_REGIONS.keys()) | {"Inconnu"})
    rows = [(r, "DOM" if r in _DOM else ("Inconnu" if r == "Inconnu" else "Métropole"))
            for r in regions]
    return spark.createDataFrame(rows, ["region", "zone"])


def main() -> None:
    spark = (SparkSession.builder
             .appName("CHU Médaillon — Silver vers Gold")
             .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
             .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    hosp = spark.read.parquet(f"{SILVER}/hospitalisations")
    finess = spark.read.parquet(f"{SILVER}/finess")
    patient = spark.read.parquet(f"{SILVER}/patient")
    diag = spark.read.parquet(f"{SILVER}/diagnostic")
    prof = spark.read.parquet(f"{SILVER}/professionnel")
    consult = spark.read.parquet(f"{SILVER}/consultation")
    activite = spark.read.parquet(f"{SILVER}/activite")             # identifiant -> finess
    satis = spark.read.parquet(f"{SILVER}/satisfaction2020")

    # =================== DIMENSIONS ===================
    # --- DIM_PATIENT (clé naturelle : id_patient) ---
    dim_patient = (patient.select(
        F.col("Id_patient").cast("int").alias("patient_id"),
        F.col("Sexe").alias("sexe"),
        F.col("Age").cast("int").alias("age"),
        F.col("Ville").alias("ville"),
        F.col("Code_postal").alias("code_postal"),
        F.col("Groupe_sanguin").alias("groupe_sanguin"))
        .where(F.col("patient_id").isNotNull()).dropDuplicates(["patient_id"]))
    dim_patient = dim_patient.withColumn("tranche_age",
        F.when(F.col("age") < 18, "0-17").when(F.col("age") < 30, "18-29")
         .when(F.col("age") < 45, "30-44").when(F.col("age") < 60, "45-59")
         .when(F.col("age") < 75, "60-74").otherwise("75+"))
    write_gold(spark, dim_patient, "DIM_PATIENT")

    # --- DIM_DIAGNOSTIC (référentiel + codes des hospitalisations) ---
    diag_op = diag.select(F.col("Code_diag").alias("code_diag"),
                          F.col("Diagnostic").alias("libelle_diagnostic"))
    diag_h = hosp.select("code_diag", F.col("suite_diagnostic").alias("libelle_diagnostic"))
    dim_diag = (diag_op.unionByName(diag_h)
                .where(F.col("code_diag").isNotNull())
                .dropDuplicates(["code_diag"]))
    write_gold(spark, dim_diag, "DIM_DIAGNOSTIC")

    # --- DIM_ETABLISSEMENT (clé naturelle : finess) + région ---
    dep2 = F.substring("code_postal", 1, 2)
    dep3 = F.substring("code_postal", 1, 3)
    etab = (finess.select(
        F.col("identifiant_organisation").alias("finess"),
        F.col("raison_sociale_site").alias("raison_sociale"),
        F.col("commune"), F.col("code_postal"),
        F.when(dep2 == "97", dep3).otherwise(dep2).alias("departement"))
        .where(F.col("finess").isNotNull()).dropDuplicates(["finess"]))
    dim_etab = (etab.join(region_df(spark), "departement", "left")
                .na.fill({"region": "Inconnu"}))
    write_gold(spark, dim_etab, "DIM_ETABLISSEMENT")

    # --- DIM_PROFESSIONNEL (clé naturelle : identifiant) ---
    dim_prof = (prof.select(
        F.col("Identifiant").alias("identifiant"),
        F.col("Civilite").alias("civilite"),
        F.col("Categorie_professionnelle").alias("categorie_professionnelle"),
        F.col("Profession").alias("profession"),
        F.col("Code_specialite").alias("code_specialite"))
        .where(F.col("identifiant").isNotNull()).dropDuplicates(["identifiant"]))
    write_gold(spark, dim_prof, "DIM_PROFESSIONNEL")

    # --- DIM_TEMPS (union des dates des 2 faits) ---
    dh = hosp.select(F.col("date_entree").alias("d"))
    dc = consult.select(F.col("Date").cast("date").alias("d"))
    dim_temps = (dh.unionByName(dc).where(F.col("d").isNotNull()).dropDuplicates(["d"])
                 .select(
                    F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
                    F.col("d").alias("date_complete"),
                    F.year("d").alias("annee"),
                    F.quarter("d").alias("trimestre"),
                    F.month("d").alias("mois"),
                    F.dayofmonth("d").alias("jour")))
    write_gold(spark, dim_temps, "DIM_TEMPS")

    # --- DIM_LOCALISATION (grain région) — partagée par DECES + SATISFACTION ---
    write_gold(spark, localisation_df(spark), "DIM_LOCALISATION")

    # =================== FAITS (constellation) ===================
    # --- FAIT_HOSPITALISATION ---
    fait_hosp = (hosp.select(
        "num_hospitalisation",
        F.col("id_patient").alias("patient_id"),
        "finess", "code_diag",
        F.date_format("date_entree", "yyyyMMdd").cast("int").alias("date_key"),
        F.year("date_entree").alias("annee"),
        "jours_hospitalisation")
        .withColumn("nb_hospitalisation", F.lit(1)))
    write_gold(spark, fait_hosp, "FAIT_HOSPITALISATION")

    # --- FAIT_CONSULTATION (durée + rattachement établissement -> besoin n°1) ---
    hhmmss = r"(\d{2}:\d{2}:\d{2})"
    deb = F.unix_timestamp(F.regexp_extract(F.col("Heure_debut").cast("string"), hhmmss, 1), "HH:mm:ss")
    fin = F.unix_timestamp(F.regexp_extract(F.col("Heure_fin").cast("string"), hhmmss, 1), "HH:mm:ss")
    base_consult = consult.select(
        F.col("Num_consultation").alias("num_consultation"),
        F.col("Id_patient").cast("int").alias("patient_id"),
        F.col("Id_prof_sante").alias("identifiant"),
        F.col("Code_diag").alias("code_diag"),
        F.date_format(F.col("Date").cast("date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.year(F.col("Date").cast("date")).alias("annee"),
        ((fin - deb) / 60).cast("int").alias("duree_minutes"))
    # Établissement reconstruit via le praticien : Id_prof_sante -> organisation (FINESS)
    fait_consult = (base_consult
        .join(activite, "identifiant", "left")       # ajoute la colonne finess
        .withColumn("nb_consultation", F.lit(1)))
    write_gold(spark, fait_consult, "FAIT_CONSULTATION")

    # --- FAIT_SATISFACTION (e-Satis 2020, agrégé par région -> besoin n°8) ---
    sat = satis.replace(_SAT_ALIAS, subset=["region"])
    fait_satis = (sat.where(F.col("score_all_rea_ajust").isNotNull())
        .groupBy("region")
        .agg(F.countDistinct("finess").alias("nb_etablissements"),
             F.round(F.avg("score_all_rea_ajust"), 2).alias("score_satisfaction"),
             F.round(F.avg("taux_reco_brut"), 2).alias("taux_recommandation"))
        .withColumn("annee", F.lit(2020)))
    write_gold(spark, fait_satis, "FAIT_SATISFACTION")

    print("Silver -> Gold terminé (constellation chargée dans PostgreSQL).")
    spark.stop()


if __name__ == "__main__":
    main()
