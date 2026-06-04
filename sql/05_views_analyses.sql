-- =====================================================================
--  CHU Data Warehouse — Vues d'analyse (prêtes pour Power BI / restitution)
--  Une vue par besoin utilisateur. Les axes (établissement X, période Y,
--  diagnostic X...) restent en colonnes : on filtre dans Power BI (slicers).
-- =====================================================================

-- 1) Taux de consultation par établissement et par période ------------
DROP VIEW IF EXISTS "V_CONSULT_ETABLISSEMENT";
CREATE VIEW "V_CONSULT_ETABLISSEMENT" AS
SELECT t.annee,
       t.trimestre,
       e.finess,
       e.raison_sociale,
       e.region,
       COUNT(*)                                                              AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee), 2) AS taux_pct
FROM "FAIT_CONSULTATION" f
JOIN "DIM_TEMPS" t          ON t.date_key = f.date_key
JOIN "DIM_ETABLISSEMENT" e  ON e.etablissement_key = f.etablissement_key
GROUP BY t.annee, t.trimestre, e.finess, e.raison_sociale, e.region;

-- 2) Taux de consultation par diagnostic et par période ---------------
DROP VIEW IF EXISTS "V_CONSULT_DIAGNOSTIC";
CREATE VIEW "V_CONSULT_DIAGNOSTIC" AS
SELECT t.annee,
       d.code_diag,
       d.libelle_diagnostic,
       COUNT(*)                                                              AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee), 2) AS taux_pct
FROM "FAIT_CONSULTATION" f
JOIN "DIM_TEMPS" t       ON t.date_key = f.date_key
JOIN "DIM_DIAGNOSTIC" d  ON d.diagnostic_key = f.diagnostic_key
GROUP BY t.annee, d.code_diag, d.libelle_diagnostic;

-- 3) Taux global d'hospitalisation par période ------------------------
DROP VIEW IF EXISTS "V_HOSPIT_PERIODE";
CREATE VIEW "V_HOSPIT_PERIODE" AS
SELECT t.annee,
       t.trimestre,
       COUNT(*)                                AS nb_hospitalisations,
       SUM(f.jours_hospitalisation)            AS total_jours,
       ROUND(AVG(f.jours_hospitalisation), 1)  AS duree_moyenne_jours
FROM "FAIT_HOSPITALISATION" f
JOIN "DIM_TEMPS" t ON t.date_key = f.date_key
GROUP BY t.annee, t.trimestre;

-- 4) Taux d'hospitalisation par diagnostic et par période -------------
DROP VIEW IF EXISTS "V_HOSPIT_DIAGNOSTIC";
CREATE VIEW "V_HOSPIT_DIAGNOSTIC" AS
SELECT t.annee,
       d.code_diag,
       d.libelle_diagnostic,
       COUNT(*)                                                              AS nb_hospitalisations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee), 2) AS taux_pct
FROM "FAIT_HOSPITALISATION" f
JOIN "DIM_TEMPS" t       ON t.date_key = f.date_key
JOIN "DIM_DIAGNOSTIC" d  ON d.diagnostic_key = f.diagnostic_key
GROUP BY t.annee, d.code_diag, d.libelle_diagnostic;

-- 5) Taux d'hospitalisation par sexe et par tranche d'âge -------------
DROP VIEW IF EXISTS "V_HOSPIT_SEXE_AGE";
CREATE VIEW "V_HOSPIT_SEXE_AGE" AS
SELECT p.sexe,
       p.tranche_age,
       COUNT(*)                                            AS nb_hospitalisations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS taux_pct
FROM "FAIT_HOSPITALISATION" f
JOIN "DIM_PATIENT" p ON p.patient_key = f.patient_key
GROUP BY p.sexe, p.tranche_age;

-- 6) Taux de consultation par professionnel ---------------------------
DROP VIEW IF EXISTS "V_CONSULT_PROFESSIONNEL";
CREATE VIEW "V_CONSULT_PROFESSIONNEL" AS
SELECT pr.identifiant,
       pr.profession,
       pr.specialite,
       COUNT(*)                                            AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS taux_pct
FROM "FAIT_CONSULTATION" f
JOIN "DIM_PROFESSIONNEL" pr ON pr.professionnel_key = f.professionnel_key
GROUP BY pr.identifiant, pr.profession, pr.specialite;

-- 7) Nombre de décès par région et par année (besoin : 2019) ----------
DROP VIEW IF EXISTS "V_DECES_REGION";
CREATE VIEW "V_DECES_REGION" AS
SELECT f.annee,
       l.region,
       SUM(f.nb_deces) AS nb_deces
FROM "FAIT_DECES" f
JOIN "DIM_LOCALISATION" l ON l.localisation_key = f.localisation_key
GROUP BY f.annee, l.region;

-- 8) Taux global de satisfaction par région (besoin : 2020) -----------
DROP VIEW IF EXISTS "V_SATISFACTION_REGION";
CREATE VIEW "V_SATISFACTION_REGION" AS
SELECT f.annee,
       l.region,
       f.nb_etablissements,
       f.score_satisfaction,
       f.taux_recommandation
FROM "FAIT_SATISFACTION" f
JOIN "DIM_LOCALISATION" l ON l.localisation_key = f.localisation_key;
