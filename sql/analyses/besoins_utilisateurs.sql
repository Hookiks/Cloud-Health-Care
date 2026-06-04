-- =====================================================================
--  CHU Data Warehouse — Requêtes répondant aux 8 besoins utilisateurs
--  (praticiens / chefs d'établissement). "Taux" = part en %.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1) Taux de consultation des patients dans un établissement X sur une
--    période Y. L'établissement est reconstruit via le professionnel
--    (activite_professionnel_sante -> FINESS). Filtrer X via e.raison_sociale
--    / e.finess et la période via t.annee.
-- ---------------------------------------------------------------------
SELECT e.finess,
       e.raison_sociale,
       e.region,
       t.annee,
       COUNT(*)                                                              AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY t.annee), 2) AS taux_pct
FROM "FAIT_CONSULTATION" f
JOIN "DIM_TEMPS" t          ON t.date_key = f.date_key
JOIN "DIM_ETABLISSEMENT" e  ON e.etablissement_key = f.etablissement_key
WHERE t.annee = 2019                       -- exemple de période Y
GROUP BY e.finess, e.raison_sociale, e.region, t.annee
ORDER BY nb_consultations DESC
LIMIT 20;

-- ---------------------------------------------------------------------
-- 2) Taux de consultation par diagnostic sur une période (ex. 2018-2020).
-- ---------------------------------------------------------------------
SELECT d.code_diag,
       d.libelle_diagnostic,
       COUNT(*)                                            AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS taux_pct
FROM "FAIT_CONSULTATION" f
JOIN "DIM_DIAGNOSTIC" d ON d.diagnostic_key = f.diagnostic_key
JOIN "DIM_TEMPS" t      ON t.date_key = f.date_key
WHERE t.annee BETWEEN 2018 AND 2020
GROUP BY d.code_diag, d.libelle_diagnostic
ORDER BY nb_consultations DESC
LIMIT 20;

-- ---------------------------------------------------------------------
-- 3) Taux global d'hospitalisation par période (par année).
-- ---------------------------------------------------------------------
SELECT t.annee,
       COUNT(*)                                            AS nb_hospitalisations,
       SUM(f.jours_hospitalisation)                        AS total_jours,
       ROUND(AVG(f.jours_hospitalisation), 1)              AS duree_moyenne_jours
FROM "FAIT_HOSPITALISATION" f
JOIN "DIM_TEMPS" t ON t.date_key = f.date_key
GROUP BY t.annee
ORDER BY t.annee;

-- ---------------------------------------------------------------------
-- 4) Taux d'hospitalisation par diagnostic sur une période.
-- ---------------------------------------------------------------------
SELECT d.code_diag,
       d.libelle_diagnostic,
       COUNT(*)                                            AS nb_hospitalisations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS taux_pct
FROM "FAIT_HOSPITALISATION" f
JOIN "DIM_DIAGNOSTIC" d ON d.diagnostic_key = f.diagnostic_key
GROUP BY d.code_diag, d.libelle_diagnostic
ORDER BY nb_hospitalisations DESC
LIMIT 20;

-- ---------------------------------------------------------------------
-- 5) Taux d'hospitalisation par sexe et par tranche d'âge.
-- ---------------------------------------------------------------------
SELECT p.sexe,
       p.tranche_age,
       COUNT(*)                                            AS nb_hospitalisations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS taux_pct
FROM "FAIT_HOSPITALISATION" f
JOIN "DIM_PATIENT" p ON p.patient_key = f.patient_key
GROUP BY p.sexe, p.tranche_age
ORDER BY p.sexe, p.tranche_age;

-- ---------------------------------------------------------------------
-- 6) Taux de consultation par professionnel de santé.
-- ---------------------------------------------------------------------
SELECT pr.identifiant,
       pr.profession,
       pr.specialite,
       COUNT(*)                                            AS nb_consultations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS taux_pct
FROM "FAIT_CONSULTATION" f
JOIN "DIM_PROFESSIONNEL" pr ON pr.professionnel_key = f.professionnel_key
GROUP BY pr.identifiant, pr.profession, pr.specialite
ORDER BY nb_consultations DESC
LIMIT 20;

-- ---------------------------------------------------------------------
-- 7) Nombre de décès par région sur l'année 2019.
-- ---------------------------------------------------------------------
SELECT l.region,
       SUM(f.nb_deces)                                     AS nb_deces
FROM "FAIT_DECES" f
JOIN "DIM_LOCALISATION" l ON l.localisation_key = f.localisation_key
WHERE f.annee = 2019
GROUP BY l.region
ORDER BY nb_deces DESC;

-- ---------------------------------------------------------------------
-- 8) Taux global de satisfaction par région sur l'année 2020.
-- ---------------------------------------------------------------------
SELECT l.region,
       f.nb_etablissements,
       f.score_satisfaction,
       f.taux_recommandation
FROM "FAIT_SATISFACTION" f
JOIN "DIM_LOCALISATION" l ON l.localisation_key = f.localisation_key
WHERE f.annee = 2020
ORDER BY f.score_satisfaction DESC NULLS LAST;
