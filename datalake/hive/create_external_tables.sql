-- =====================================================================
--  Hive — Tables EXTERNES sur la zone brute du Data Lake HDFS.
--  "Schema-on-read" : Hive expose les fichiers HDFS comme des tables SQL
--  sans les copier. Permet d'explorer le lac avant l'ETL.
--
--  Exécution :
--    docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 \
--           -f /opt/.../create_external_tables.sql
--  (ou copier-coller dans beeline)
-- =====================================================================

CREATE DATABASE IF NOT EXISTS chu_lake;
USE chu_lake;

-- ---- Hospitalisations (séparateur ';') -------------------------------
DROP TABLE IF EXISTS raw_hospitalisations;
CREATE EXTERNAL TABLE raw_hospitalisations (
    num_hospitalisation      STRING,
    id_patient               STRING,
    identifiant_organisation STRING,
    code_diagnostic          STRING,
    suite_diagnostic         STRING,
    date_entree              STRING,
    jour_hospitalisation     STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/datalake/raw/hospitalisation'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---- Registre des décès (séparateur ',') -----------------------------
DROP TABLE IF EXISTS raw_deces;
CREATE EXTERNAL TABLE raw_deces (
    nom                STRING,
    prenom             STRING,
    sexe               STRING,
    date_naissance     STRING,
    code_lieu_naissance STRING,
    lieu_naissance     STRING,
    pays_naissance     STRING,
    date_deces         STRING,
    code_lieu_deces    STRING,
    numero_acte_deces  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/datalake/raw/deces'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ---- Établissements FINESS (séparateur ';') --------------------------
DROP TABLE IF EXISTS raw_etablissements;
CREATE EXTERNAL TABLE raw_etablissements (
    adresse STRING, cedex STRING, code_commune STRING, code_postal STRING,
    commune STRING, complement_destinataire STRING, complement_point_geographique STRING,
    email STRING, enseigne_commerciale_site STRING, finess_etablissement_juridique STRING,
    finess_site STRING, identifiant_organisation STRING, indice_repetition_voie STRING,
    mention_distribution STRING, numero_voie STRING, pays STRING, raison_sociale_site STRING,
    siren_site STRING, siret_site STRING, telecopie STRING, telephone STRING,
    telephone_2 STRING, type_voie STRING, voie STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/datalake/raw/finess'
TBLPROPERTIES ('skip.header.line.count'='1');

SHOW TABLES;
