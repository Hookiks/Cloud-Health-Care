-- =====================================================================
--  CHU Data Warehouse — Tables de faits
-- =====================================================================

-- ---------------------------------------------------------------------
--  FAIT_CONSULTATION : grain = une consultation
CREATE TABLE "FAIT_CONSULTATION" (
    num_consultation  INTEGER,
    patient_key       INTEGER REFERENCES "DIM_PATIENT"(patient_key),
    professionnel_key INTEGER REFERENCES "DIM_PROFESSIONNEL"(professionnel_key),
    etablissement_key INTEGER REFERENCES "DIM_ETABLISSEMENT"(etablissement_key),
    diagnostic_key    INTEGER REFERENCES "DIM_DIAGNOSTIC"(diagnostic_key),
    mutuelle_key      INTEGER REFERENCES "DIM_MUTUELLE"(mutuelle_key),
    date_key          INTEGER REFERENCES "DIM_TEMPS"(date_key),
    annee             SMALLINT,
    duree_minutes     INTEGER,        -- mesure
    nb_consultation   SMALLINT DEFAULT 1
);

-- ---------------------------------------------------------------------
--  FAIT_HOSPITALISATION : grain = une hospitalisation
CREATE TABLE "FAIT_HOSPITALISATION" (
    num_hospitalisation   INTEGER,
    patient_key           INTEGER REFERENCES "DIM_PATIENT"(patient_key),
    etablissement_key     INTEGER REFERENCES "DIM_ETABLISSEMENT"(etablissement_key),
    diagnostic_key        INTEGER REFERENCES "DIM_DIAGNOSTIC"(diagnostic_key),
    date_key              INTEGER REFERENCES "DIM_TEMPS"(date_key),
    annee                 SMALLINT,
    jours_hospitalisation INTEGER,    -- mesure
    nb_hospitalisation    SMALLINT DEFAULT 1
);

-- ---------------------------------------------------------------------
--  FAIT_DECES : grain agrégé = année x région x sexe
CREATE TABLE "FAIT_DECES" (
    annee             SMALLINT,
    localisation_key  INTEGER REFERENCES "DIM_LOCALISATION"(localisation_key),
    sexe              VARCHAR(10),
    nb_deces          INTEGER          -- mesure
);

-- ---------------------------------------------------------------------
--  FAIT_SATISFACTION : grain agrégé = année x région
CREATE TABLE "FAIT_SATISFACTION" (
    annee                 SMALLINT,
    localisation_key      INTEGER REFERENCES "DIM_LOCALISATION"(localisation_key),
    nb_etablissements     INTEGER,
    score_satisfaction    NUMERIC(5,2),   -- mesure : score global moyen e-Satis
    taux_recommandation   NUMERIC(5,2)    -- mesure : taux de recommandation moyen
);
