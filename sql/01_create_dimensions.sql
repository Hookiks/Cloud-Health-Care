-- =====================================================================
--  CHU Data Warehouse — Dimensions (schéma en étoile)
--  Exécuté sur la base cible chu_dw.
-- =====================================================================

DROP TABLE IF EXISTS "FAIT_CONSULTATION"      CASCADE;
DROP TABLE IF EXISTS "FAIT_HOSPITALISATION"   CASCADE;
DROP TABLE IF EXISTS "FAIT_DECES"             CASCADE;
DROP TABLE IF EXISTS "FAIT_SATISFACTION"      CASCADE;

DROP TABLE IF EXISTS "DIM_TEMPS"          CASCADE;
DROP TABLE IF EXISTS "DIM_PATIENT"        CASCADE;
DROP TABLE IF EXISTS "DIM_DIAGNOSTIC"     CASCADE;
DROP TABLE IF EXISTS "DIM_PROFESSIONNEL"  CASCADE;
DROP TABLE IF EXISTS "DIM_ETABLISSEMENT"  CASCADE;
DROP TABLE IF EXISTS "DIM_LOCALISATION"   CASCADE;
DROP TABLE IF EXISTS "DIM_MUTUELLE"       CASCADE;

-- ---------------------------------------------------------------------
CREATE TABLE "DIM_TEMPS" (
    date_key        INTEGER PRIMARY KEY,          -- AAAAMMJJ
    date_complete   DATE        NOT NULL,
    annee           SMALLINT    NOT NULL,
    trimestre       SMALLINT    NOT NULL,
    mois            SMALLINT    NOT NULL,
    mois_nom        VARCHAR(15) NOT NULL,
    jour            SMALLINT    NOT NULL,
    jour_semaine    SMALLINT    NOT NULL,          -- 1=lundi .. 7=dimanche
    jour_nom        VARCHAR(15) NOT NULL,
    est_weekend     BOOLEAN     NOT NULL
);

-- ---------------------------------------------------------------------
--  RGPD : aucune donnée directement identifiante (nom, num_secu, e-mail...)
CREATE TABLE "DIM_PATIENT" (
    patient_key     SERIAL PRIMARY KEY,
    id_patient      INTEGER     NOT NULL UNIQUE,   -- clé naturelle opérationnelle
    sexe            VARCHAR(10),
    age             SMALLINT,
    tranche_age     VARCHAR(10),
    ville           VARCHAR(100),
    code_postal     VARCHAR(10),
    groupe_sanguin  VARCHAR(3)
);

-- ---------------------------------------------------------------------
CREATE TABLE "DIM_DIAGNOSTIC" (
    diagnostic_key  SERIAL PRIMARY KEY,
    code_diag       VARCHAR(20) NOT NULL UNIQUE,
    libelle_diagnostic VARCHAR(255)
);

-- ---------------------------------------------------------------------
CREATE TABLE "DIM_PROFESSIONNEL" (
    professionnel_key SERIAL PRIMARY KEY,
    identifiant       VARCHAR(30) NOT NULL UNIQUE,
    civilite          VARCHAR(20),
    categorie_professionnelle VARCHAR(100),
    profession        VARCHAR(150),
    code_specialite   VARCHAR(150),
    specialite        VARCHAR(200),
    fonction          VARCHAR(200)
);

-- ---------------------------------------------------------------------
CREATE TABLE "DIM_ETABLISSEMENT" (
    etablissement_key SERIAL PRIMARY KEY,
    finess            VARCHAR(20) NOT NULL UNIQUE,  -- identifiant_organisation
    raison_sociale    VARCHAR(255),
    commune           VARCHAR(100),
    code_postal       VARCHAR(10),
    code_departement  VARCHAR(3),
    region            VARCHAR(60)
);

-- ---------------------------------------------------------------------
--  Grain = région (réponse aux besoins "par région")
CREATE TABLE "DIM_LOCALISATION" (
    localisation_key  SERIAL PRIMARY KEY,
    region            VARCHAR(60) NOT NULL UNIQUE
);

-- ---------------------------------------------------------------------
CREATE TABLE "DIM_MUTUELLE" (
    mutuelle_key      SERIAL PRIMARY KEY,
    id_mut            INTEGER NOT NULL UNIQUE,
    nom_mutuelle      VARCHAR(150)
);
