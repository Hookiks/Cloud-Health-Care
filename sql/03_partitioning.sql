-- =====================================================================
--  CHU Data Warehouse — Partitionnement & buckets (Livrable 2)
--
--  On construit des COPIES partitionnées des tables de faits, peuplées
--  depuis les tables de base, afin de comparer les temps de réponse
--  (avant / après partitionnement) dans scripts/benchmark.py.
-- =====================================================================

-- ---------------------------------------------------------------------
--  1) Partitionnement par PLAGE (RANGE) sur l'année
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS "FAIT_CONSULTATION_PART" CASCADE;
CREATE TABLE "FAIT_CONSULTATION_PART" (
    num_consultation  INTEGER,
    patient_key       INTEGER,
    professionnel_key INTEGER,
    diagnostic_key    INTEGER,
    mutuelle_key      INTEGER,
    date_key          INTEGER,
    annee             SMALLINT NOT NULL,
    duree_minutes     INTEGER,
    nb_consultation   SMALLINT
) PARTITION BY RANGE (annee);

DO $$
DECLARE y INT;
BEGIN
    FOR y IN 2014..2023 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF "FAIT_CONSULTATION_PART" FOR VALUES FROM (%s) TO (%s);',
            'fc_part_' || y, y, y + 1);
    END LOOP;
END $$;
CREATE TABLE fc_part_autres PARTITION OF "FAIT_CONSULTATION_PART" DEFAULT;

INSERT INTO "FAIT_CONSULTATION_PART"
SELECT num_consultation, patient_key, professionnel_key, diagnostic_key,
       mutuelle_key, date_key, annee, duree_minutes, nb_consultation
FROM "FAIT_CONSULTATION";
CREATE INDEX ix_fcp_diag ON "FAIT_CONSULTATION_PART"(diagnostic_key);


DROP TABLE IF EXISTS "FAIT_HOSPITALISATION_PART" CASCADE;
CREATE TABLE "FAIT_HOSPITALISATION_PART" (
    num_hospitalisation   INTEGER,
    patient_key           INTEGER,
    etablissement_key     INTEGER,
    diagnostic_key        INTEGER,
    date_key              INTEGER,
    annee                 SMALLINT NOT NULL,
    jours_hospitalisation INTEGER,
    nb_hospitalisation    SMALLINT
) PARTITION BY RANGE (annee);

DO $$
DECLARE y INT;
BEGIN
    FOR y IN 2014..2023 LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF "FAIT_HOSPITALISATION_PART" FOR VALUES FROM (%s) TO (%s);',
            'fh_part_' || y, y, y + 1);
    END LOOP;
END $$;
CREATE TABLE fh_part_autres PARTITION OF "FAIT_HOSPITALISATION_PART" DEFAULT;

INSERT INTO "FAIT_HOSPITALISATION_PART"
SELECT num_hospitalisation, patient_key, etablissement_key, diagnostic_key,
       date_key, annee, jours_hospitalisation, nb_hospitalisation
FROM "FAIT_HOSPITALISATION";
CREATE INDEX ix_fhp_etab ON "FAIT_HOSPITALISATION_PART"(etablissement_key);


-- ---------------------------------------------------------------------
--  2) Partitionnement par HACHAGE (buckets) sur le patient
--     Répartit uniformément la charge — utile pour le parallélisme.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS "FAIT_CONSULTATION_BUCKETS" CASCADE;
CREATE TABLE "FAIT_CONSULTATION_BUCKETS" (
    num_consultation  INTEGER,
    patient_key       INTEGER,
    professionnel_key INTEGER,
    diagnostic_key    INTEGER,
    mutuelle_key      INTEGER,
    date_key          INTEGER,
    annee             SMALLINT,
    duree_minutes     INTEGER,
    nb_consultation   SMALLINT
) PARTITION BY HASH (patient_key);

CREATE TABLE fc_bucket_0 PARTITION OF "FAIT_CONSULTATION_BUCKETS" FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE fc_bucket_1 PARTITION OF "FAIT_CONSULTATION_BUCKETS" FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE fc_bucket_2 PARTITION OF "FAIT_CONSULTATION_BUCKETS" FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE fc_bucket_3 PARTITION OF "FAIT_CONSULTATION_BUCKETS" FOR VALUES WITH (MODULUS 4, REMAINDER 3);

INSERT INTO "FAIT_CONSULTATION_BUCKETS"
SELECT num_consultation, patient_key, professionnel_key, diagnostic_key,
       mutuelle_key, date_key, annee, duree_minutes, nb_consultation
FROM "FAIT_CONSULTATION";

ANALYZE "FAIT_CONSULTATION_PART";
ANALYZE "FAIT_HOSPITALISATION_PART";
ANALYZE "FAIT_CONSULTATION_BUCKETS";
