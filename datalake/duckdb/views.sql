-- Vues d'analyse pour chu_gold.duckdb
-- Usage : duckdb chu_gold.duckdb < views.sql

CREATE OR REPLACE VIEW V_HOSPIT_REGION AS
SELECT e.region,
       COUNT(*) AS nb_hospitalisations,
       ROUND(AVG(h.jours_hospitalisation), 1) AS duree_moy_jours
FROM   GOLD_FAIT_HOSPITALISATION h
JOIN   GOLD_DIM_ETABLISSEMENT e ON h.finess = e.finess
GROUP  BY e.region
ORDER  BY nb_hospitalisations DESC;

CREATE OR REPLACE VIEW V_CONSULT_PROFESSION AS
SELECT p.profession, p.code_specialite, COUNT(*) AS nb_consultations,
       ROUND(AVG(c.duree_minutes), 1) AS duree_moy_min
FROM GOLD_FAIT_CONSULTATION c
JOIN GOLD_DIM_PROFESSIONNEL p ON c.identifiant = p.identifiant
GROUP BY p.profession, p.code_specialite
ORDER BY nb_consultations DESC;

CREATE OR REPLACE VIEW V_SATISFACTION_REGION AS
SELECT s.region, l.zone, s.nb_etablissements, s.score_satisfaction, s.taux_recommandation
FROM GOLD_FAIT_SATISFACTION s
JOIN GOLD_DIM_LOCALISATION l ON s.region = l.region
ORDER BY s.score_satisfaction DESC;

CREATE OR REPLACE VIEW V_DECES_REGION AS
SELECT d.annee, d.region, d.sexe, d.nb_deces, l.zone
FROM GOLD_FAIT_DECES d
JOIN GOLD_DIM_LOCALISATION l ON d.region = l.region
ORDER BY d.annee, d.region;

CREATE OR REPLACE VIEW V_DIAGNOSTIC_FREQUENT AS
SELECT dg.libelle_diagnostic, COUNT(*) AS nb_occurrences
FROM GOLD_FAIT_HOSPITALISATION h
JOIN GOLD_DIM_DIAGNOSTIC dg ON h.code_diag = dg.code_diag
GROUP BY dg.libelle_diagnostic
ORDER BY nb_occurrences DESC;

CREATE OR REPLACE VIEW V_HOSPIT_TEMPS AS
SELECT t.annee, t.mois, COUNT(*) AS nb_hospitalisations
FROM GOLD_FAIT_HOSPITALISATION h
JOIN GOLD_DIM_TEMPS t ON h.date_key = t.date_key
GROUP BY t.annee, t.mois
ORDER BY t.annee, t.mois;

CREATE OR REPLACE VIEW V_CONSULT_TEMPS AS
SELECT t.annee, t.mois, COUNT(*) AS nb_consultations
FROM GOLD_FAIT_CONSULTATION c
JOIN GOLD_DIM_TEMPS t ON c.date_key = t.date_key
GROUP BY t.annee, t.mois
ORDER BY t.annee, t.mois;

CREATE OR REPLACE VIEW V_PATIENT_PROFIL AS
SELECT tranche_age, sexe, groupe_sanguin, COUNT(*) AS nb_patients
FROM GOLD_DIM_PATIENT
GROUP BY tranche_age, sexe, groupe_sanguin
ORDER BY tranche_age, sexe;
