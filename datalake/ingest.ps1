# =====================================================================
#  Ingestion des sources brutes dans le Data Lake HDFS (Windows / PowerShell)
#  NB : Hadoop `hdfs dfs -put` ne gère pas les espaces dans le chemin local.
#       On fait donc `cd '<dossier>' && hdfs dfs -put <fichier>` (le nom passé
#       à hdfs est alors sans espace).
# =====================================================================
$ErrorActionPreference = "Stop"
$NN = "namenode"

Write-Host "== Création de l'arborescence du lac =="
docker exec $NN hdfs dfs -mkdir -p /datalake/raw/hospitalisation /datalake/raw/finess /datalake/raw/deces /datalake/raw/satisfaction

Write-Host "== Chargement des fichiers (HDFS put) =="
docker exec $NN sh -c "cd '/sources/Hospitalisation' && hdfs dfs -put -f Hospitalisations.csv /datalake/raw/hospitalisation/"
docker exec $NN sh -c "cd '/sources/Etablissement de SANTE' && hdfs dfs -put -f etablissement_sante.csv /datalake/raw/finess/"
docker exec $NN sh -c "cd '/sources/Satisfaction/2019' && hdfs dfs -put -f resultats-esatis48h-mco-open-data-2019.csv /datalake/raw/satisfaction/"
Write-Host "  (deces.csv 2 Go : peut prendre plusieurs minutes...)"
docker exec $NN sh -c "cd '/sources/DECES EN FRANCE' && hdfs dfs -put -f deces.csv /datalake/raw/deces/"

Write-Host "== Contenu du lac =="
docker exec $NN hdfs dfs -ls -R -h /datalake/raw
Write-Host "OK - donnees disponibles dans HDFS (UI : http://localhost:9870)"
