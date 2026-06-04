# =====================================================================
#  Ingestion des sources brutes dans le Data Lake HDFS (Windows / PowerShell)
#  Équivalent de ingest.sh.
# =====================================================================
$ErrorActionPreference = "Stop"
$NN = "namenode"

Write-Host "== Création de l'arborescence du lac =="
docker exec $NN hdfs dfs -mkdir -p /datalake/raw/hospitalisation /datalake/raw/finess /datalake/raw/deces /datalake/raw/satisfaction

Write-Host "== Chargement des fichiers (HDFS put) =="
docker exec $NN hdfs dfs -put -f "/sources/Hospitalisation/Hospitalisations.csv"           /datalake/raw/hospitalisation/
docker exec $NN hdfs dfs -put -f "/sources/Etablissement de SANTE/etablissement_sante.csv" /datalake/raw/finess/
docker exec $NN hdfs dfs -put -f "/sources/DECES EN FRANCE/deces.csv"                        /datalake/raw/deces/
docker exec $NN hdfs dfs -put -f "/sources/Satisfaction/2019/resultats-esatis48h-mco-open-data-2019.csv" /datalake/raw/satisfaction/

Write-Host "== Contenu du lac =="
docker exec $NN hdfs dfs -ls -R /datalake/raw
Write-Host "OK - donnees disponibles dans HDFS (UI : http://localhost:9870)"
