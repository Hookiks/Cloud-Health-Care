-- =====================================================================
--  Hive — Exploration du Data Lake (SQL directement sur les fichiers HDFS).
--  Démontre qu'on peut interroger le lac avant tout ETL relationnel.
-- =====================================================================
USE chu_lake;

-- Volumétrie brute du lac
SELECT 'hospitalisations' AS source, COUNT(*) AS n FROM raw_hospitalisations
UNION ALL SELECT 'deces',           COUNT(*) FROM raw_deces
UNION ALL SELECT 'etablissements',  COUNT(*) FROM raw_etablissements;

-- Décès par année directement depuis le lac (équivalent du besoin n°7)
SELECT SUBSTR(date_deces, 1, 4) AS annee, COUNT(*) AS nb_deces
FROM raw_deces
WHERE date_deces RLIKE '^[0-9]{4}'
GROUP BY SUBSTR(date_deces, 1, 4)
ORDER BY annee DESC
LIMIT 10;

-- Top diagnostics d'hospitalisation depuis le lac
SELECT code_diagnostic, COUNT(*) AS nb
FROM raw_hospitalisations
GROUP BY code_diagnostic
ORDER BY nb DESC
LIMIT 10;
