# Rapport d'architecture — Pipeline Médaillon CHU
### Cloud Healthcare Unit · Data Lake HDFS + Spark → Data Warehouse PostgreSQL

---

## 1. Vue d'ensemble

Le système décisionnel du groupe **CHU** repose sur une **architecture médaillon**
(*medallion* : Bronze → Silver → Gold) construite sur un **data lake HDFS** et un
moteur de traitement **Apache Spark**, le résultat final étant chargé dans un
**Data Warehouse PostgreSQL** pour la restitution (Power BI).

```
SOURCES                          BRONZE                SILVER                  GOLD                      DWH
(CSV/FTP + BDD opé.)             (brut, HDFS)          (nettoyé+RGPD, Parquet) (constellation, Parquet)  (PostgreSQL)
─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
deces.csv (2 Go) ───┐                                                                               ┌─► GOLD_DIM_*
etablissement.csv ──┤ hdfs put  /datalake/raw  ──►  bronze_to_silver.py  ──►  silver_to_gold.py  ──┤   GOLD_FAIT_HOSPITALISATION
hospitalisations ───┤                          nettoyage + RGPD          constellation (3 faits)   │   GOLD_FAIT_CONSULTATION
e-Satis 2019 ───────┘                          (Spark)                   (Spark)                   │      │
Consultation/Patient (JDBC) ──────────────────────────────────────────────────────────────────────-┤   GOLD_FAIT_DECES
                                                                          add_fait_deces.py ────────┘      │
                                                                                                           ▼
                                                                                                        Power BI
```

Chaque couche a une **responsabilité unique** :

| Couche | Rôle | Format | Emplacement |
|--------|------|--------|-------------|
| **Bronze** | données brutes telles quelles (traçabilité) | CSV | HDFS `/datalake/raw` |
| **Silver** | données nettoyées, typées, **anonymisées (RGPD)** | Parquet | HDFS `/datalake/silver` |
| **Gold** | données modélisées prêtes à l'analyse (constellation) | Parquet + tables | HDFS `/datalake/gold` + PostgreSQL |

### Stack technique
- **HDFS** (Hadoop 3.2.1) : 1 NameNode + 1 DataNode — stockage distribué.
- **Apache Spark 3.1.1** : 1 Master + 1 Worker (20 cœurs) — moteur de traitement.
- **PostgreSQL** : base `Cloud Healthcare Unit` (entrepôt) + base `postgres` (opérationnel).
- **Docker Compose** (images Big Data Europe) : orchestration du cluster.
- Driver **PostgreSQL JDBC 42.5.4** : passerelle Spark ↔ PostgreSQL.

---

## 2. Étape 0 — Sources & Ingestion (Bronze)

### Théorie
L'ingestion consiste à déposer les fichiers sources **bruts, sans transformation**
dans la zone Bronze du lac. Le principe médaillon impose de **conserver la donnée
d'origine intacte** : on ne nettoie rien ici, ce qui garantit la traçabilité et la
capacité à **rejouer** tout le pipeline si la logique de transformation évolue.
HDFS découpe chaque fichier en **blocs de 128 Mo** répartis et répliqués sur les
DataNodes.

### Factuellement (exemple)
Les fichiers locaux sont montés en lecture seule dans le conteneur `namenode`
(`../sources:/sources:ro`) puis copiés dans HDFS :

```bash
docker exec namenode sh -c "cd '/sources/DECES EN FRANCE' \
  && hdfs dfs -put -f deces.csv /datalake/raw/deces/"
```
> ⚠️ On fait `cd` dans le dossier avant le `put` car **Hadoop ne gère pas les
> espaces** dans le chemin local (`DECES EN FRANCE`).

Résultat réel constaté dans le lac (`hdfs dfs -ls -R -h /datalake/raw`) :

| Fichier HDFS | Taille | Lignes |
|---|---|---|
| `/datalake/raw/deces/deces.csv` | **1.9 Go** | 25 088 208 |
| `/datalake/raw/finess/etablissement_sante.csv` | 78.4 Mo | 416 665 |
| `/datalake/raw/hospitalisation/Hospitalisations.csv` | 255 Ko | 2 479 |
| `/datalake/raw/satisfaction/resultats-esatis48h-…-2019.csv` | 214 Ko | 1 152 |

---

## 3. Étape 1 — Bronze → Silver (`bronze_to_silver.py`)

### Théorie
La couche Silver **nettoie et normalise** la donnée : typage correct (entiers,
dates), gestion des valeurs nulles, encodage UTF-8, et surtout **filtrage RGPD**
des données personnelles. Le résultat est écrit en **Parquet** : format **colonne**,
compressé, avec schéma embarqué — idéal pour l'analytique (lecture sélective des
colonnes, prédicats poussés). C'est la *single source of truth* nettoyée.

Particularité du projet : le **fait consultation n'existe pas** dans les fichiers
du lac ; il vit dans la base opérationnelle PostgreSQL. Spark le **capture par
JDBC** dans cette même étape (Bronze opérationnel → Silver), ce qui garde le Gold
cohérent et auto-suffisant.

### Factuellement (exemple)
**a) Nettoyage** — une ligne brute d'hospitalisation :
```
10546;29620;F010000107;S02800;Fracture de l'alveole dentaire, fermees;27/09/2017;17
```
devient, en Silver, une ligne Parquet typée :
| num_hospitalisation | id_patient | finess | code_diag | date_entree (date) | jours_hospitalisation (int) |
|---|---|---|---|---|---|
| 10546 | 29620 | F010000107 | S02800 | 2017-09-27 | 17 |

Code (extrait) :
```python
h = (h.withColumn("num_hospitalisation", F.col("num_hospitalisation").cast("int"))
       .withColumn("date_entree", F.to_date("date_entree", "dd/MM/yyyy")))
```

**b) RGPD** — la fonction `apply_rgpd()` supprime les colonnes identifiantes.
Journalisation réelle de l'exécution :
```
[RGPD] patient: colonnes supprimées -> ['Nom','Prenom','Adresse','EMail','Tel','Num_Secu']
[RGPD] deces:   colonnes supprimées -> ['nom','prenom','numero_acte_deces']
[RGPD] finess:  colonnes supprimées -> ['adresse','email','telecopie','telephone',
                                         'telephone_2','numero_voie','type_voie','voie',...]
[RGPD] professionnel: colonnes supprimées -> ['Nom','Prenom']
```
Exemple : la ligne décès `LANGLET,ANTOINETTE GERMAINE,2,1903-11-11,02383,…` perd
`nom`/`prenom`/`numero_acte`, ne conservant que `sexe=2`, `date_naissance=1903-11-11`,
`code_lieu_deces=02691`, `date_deces=1983-04-11`.

**c) Capture JDBC opérationnelle** :
```python
consult = read_pg(spark, '"Consultation"')   # 1 027 157 lignes
patient = read_pg(spark, '"Patient"')         # 100 000 lignes (puis RGPD)
```

**Résultat réel — 10 zones Silver écrites** :
| Silver | Lignes | Source |
|---|---|---|
| hospitalisations | 2 479 | lac |
| finess | 416 665 | lac |
| satisfaction (2019) | 1 152 | lac |
| satisfaction2020 | 1 150 | lac (xlsx→csv) |
| activite | 1 311 199 | lac (lien praticien→FINESS) |
| deces | **25 088 208** | lac |
| patient | 100 000 | JDBC (RGPD) |
| diagnostic | 15 490 | JDBC |
| professionnel | 1 048 575 | JDBC (RGPD) |
| consultation | 1 027 157 | JDBC |

> **Piège résolu** : le registre des décès contient des dates antérieures à 1582
> (calendrier grégorien proleptique). Spark refuse de les écrire en Parquet par
> défaut → configuration `spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED`.

---

## 4. Étape 2 — Silver → Gold (`silver_to_gold.py`)

### Théorie
La couche Gold **modélise** la donnée pour l'analyse : on construit un schéma
dimensionnel. Ici, un schéma en **constellation** (galaxy schema) = plusieurs
tables de **faits** partageant des **dimensions** communes. Les agrégations et
jointures sont faites en mémoire par Spark, le résultat écrit en Parquet (pour
le lac) **et** chargé dans PostgreSQL (pour la BI).

### Factuellement (exemple)
**Constellation à 4 faits** (2 produits par `silver_to_gold.py` : HOSPITALISATION
et CONSULTATION ; SATISFACTION aussi par `silver_to_gold.py` ; DECES par
`add_fait_deces.py`) :

```
            DIM_TEMPS · DIM_PATIENT · DIM_DIAGNOSTIC · DIM_ETABLISSEMENT
                 \              |              |             /
        ┌─────────●─────────────●──────────────●───────────●──────────┐
        │                                                              │
  FAIT_HOSPITALISATION                                       FAIT_CONSULTATION
        │                                                              │
   (DIM_ETABLISSEMENT)                              DIM_PROFESSIONNEL + DIM_ETABLISSEMENT

  FAIT_DECES        ── grain : année × région × sexe (agrégé, 25 M décès)   [besoin 7]
  FAIT_SATISFACTION ── grain : année × région (agrégé, e-Satis 2020)         [besoin 8]
```

Les faits HOSPITALISATION et CONSULTATION **partagent** `DIM_TEMPS`, `DIM_PATIENT`,
`DIM_DIAGNOSTIC` **et `DIM_ETABLISSEMENT`** (le rattachement consultation→établissement
est reconstruit via `activite_professionnel_sante` → **besoin n°1**). `FAIT_DECES`
et `FAIT_SATISFACTION` sont des faits **agrégés** au grain région, reliés à la
dimension partagée **`DIM_LOCALISATION`** (besoins 7 et 8) — aucun fait n'est isolé.

Exemple de construction d'un fait (extrait) :
```python
fait_consult = consult.select(
    F.col("Num_consultation").alias("num_consultation"),
    F.col("Id_patient").cast("int").alias("patient_id"),
    F.col("Id_prof_sante").alias("identifiant"),
    F.col("Code_diag").alias("code_diag"),
    F.date_format(F.col("Date").cast("date"), "yyyyMMdd").cast("int").alias("date_key"),
    ((fin - deb) / 60).cast("int").alias("duree_minutes"))
```

**Résultat réel — 6 dimensions + 4 faits**, écrits en Parquet (`/datalake/gold/`)
ET en PostgreSQL (`GOLD_*`) :
| Table Gold | Lignes | Job |
|---|---|---|
| GOLD_DIM_PATIENT | 100 000 | `silver_to_gold.py` |
| GOLD_DIM_DIAGNOSTIC | 15 490 | `silver_to_gold.py` |
| GOLD_DIM_ETABLISSEMENT | 416 665 | `silver_to_gold.py` |
| GOLD_DIM_PROFESSIONNEL | 1 048 575 | `silver_to_gold.py` |
| GOLD_DIM_TEMPS | 2 703 | `silver_to_gold.py` |
| GOLD_DIM_LOCALISATION | 19 (région + zone) | `silver_to_gold.py` |
| **GOLD_FAIT_HOSPITALISATION** | 2 479 | `silver_to_gold.py` |
| **GOLD_FAIT_CONSULTATION** (avec `finess`) | 1 027 157 | `silver_to_gold.py` |
| **GOLD_FAIT_SATISFACTION** (→ DIM_LOCALISATION) | 17 (par région, 2020) | `silver_to_gold.py` |
| **GOLD_FAIT_DECES** (→ DIM_LOCALISATION) | 2 962 (agrégé / 25 M décès) | `add_fait_deces.py` |

> **Constellation cohérente** : `DIM_LOCALISATION` (grain région) est **partagée**
> par FAIT_DECES et FAIT_SATISFACTION — la région n'est plus une dimension dégénérée,
> ces deux faits agrégés ne sont donc plus isolés mais participent à la constellation.
> Un **pont** `DIM_ETABLISSEMENT.region → DIM_LOCALISATION.region` relie en plus la
> grappe événementielle (hospit/consult) à la grappe régionale : la dimension région
> filtre alors les **4 faits** → constellation **entièrement connectée**.

> **Besoin n°1 (consultation par établissement)** : la table `Consultation` ne porte
> pas de FINESS. Le lien est reconstruit via `activite_professionnel_sante.csv`
> (`Id_prof_sante` → `identifiant_organisation`), ajoutant la colonne `finess` au
> fait consultation (**~64 % des consultations rattachées**).
>
> **Besoin n°8 (satisfaction par région)** : `FAIT_SATISFACTION` agrège les scores
> e-Satis 2020 par région (score global moyen, taux de recommandation, nb
> d'établissements). Les libellés régionaux du fichier (« PACA », « Ile de France »…)
> sont normalisés vers les libellés canoniques.

**Validation de la constellation** (jointure fait ↔ dimension partagée) :
```sql
SELECT p.sexe, count(*) FROM "GOLD_FAIT_CONSULTATION" f
JOIN "GOLD_DIM_PATIENT" p ON f.patient_id = p.patient_id GROUP BY p.sexe;
--  female : 608 769   |   male : 418 388
```

> **Choix de clés** : on utilise des **clés naturelles** (`patient_id`, `finess`,
> `code_diag`, `date_key`) plutôt que des clés de substitution générées, ce qui
> simplifie le pipeline distribué tout en restant joignable.

---

## 5. Étape 2b — Ajout de FAIT_DECES (`add_fait_deces.py`)

### Théorie
`FAIT_DECES` répond au besoin analytique **n°7 : nombre de décès par région et
par année**. Contrairement aux deux autres faits (grain = 1 événement), il est
**pré-agrégé** en Silver → Gold : le grain est `année × région × sexe`. On réduit
ainsi 25 millions de lignes à ~3 000 agrégats, ce qui est **la seule donnée
utile à l'analyse régionale** et évite de stocker un fait décès à 25 M lignes dans
le DWH relationnel. C'est l'application du principe de **minimisation** :
on ne stocke que le niveau de granularité nécessaire aux requêtes cibles.

La dérivation `code_lieu_deces → département → région` est faite par Spark via
une **table de correspondance broadcastée** (diffusion en mémoire sur les workers),
ce qui évite un shuffle coûteux sur 25 M lignes.

### Factuellement (exemple)
La Silver deces contient (après RGPD) :
```
sexe=2 | date_naissance=1903-11-11 | date_deces=1983-04-11 | code_lieu_deces=02691
```
- `code_lieu_deces=02691` → département `02` → région `Hauts-de-France`.
- Cette ligne s'agrège avec toutes les femmes décédées en 1983 dans les Hauts-de-France.

Code Spark (extrait) :
```python
dept = F.when(
    F.substring("code_lieu_deces", 1, 2) == "97",
    F.substring("code_lieu_deces", 1, 3)          # DOM : 971/972/973/974/976
).otherwise(F.substring("code_lieu_deces", 1, 2)) # métropole : 2 premiers chiffres

agg = (df.withColumn("annee", F.year("date_deces"))
         .withColumn("departement", dept)
         .join(F.broadcast(ref), "departement", "left")
         .groupBy("annee", "region", "sexe")
         .agg(F.count("*").cast("int").alias("nb_deces")))
```

**Résultat factuel — décès par région 2019** (requête sur `GOLD_FAIT_DECES`) :

| Région | Décès 2019 |
|---|---:|
| Île-de-France | 76 988 |
| Auvergne-Rhône-Alpes | 70 406 |
| Nouvelle-Aquitaine | 66 097 |
| Occitanie | 59 781 |
| Hauts-de-France | 55 137 |
| Grand Est | 53 443 |
| Provence-Alpes-Côte d'Azur | 52 579 |
| Bretagne | 35 616 |
| Pays de la Loire | 35 546 |
| Normandie | 34 119 |
| Bourgogne-Franche-Comté | 30 178 |
| Centre-Val de Loire | 26 435 |
| La Réunion | 5 111 |
| Martinique | 3 555 |
| Guadeloupe | 3 415 |
| Guyane | 1 013 |
| Mayotte | 746 |
| Inconnu* | 7 271 |

*Inconnu = code commune non géocodé (décès à l'étranger, codes invalides).*

**Total 2019 : ~617 000 décès** — cohérent avec les statistiques INSEE réelles
(~613 000), la différence s'expliquant par les délais de déclaration inclus dans le
fichier.

Requête SQL directe dans PostgreSQL :
```sql
SELECT region, SUM(nb_deces) AS nb_deces
FROM "GOLD_FAIT_DECES"
WHERE annee = 2019
GROUP BY region
ORDER BY nb_deces DESC;
```

---

## 6. Étape 3 — Chargement DWH & restitution

### Théorie
Le Gold Parquet sert le **traitement Big Data** (Spark/HDFS) ; sa copie chargée en
PostgreSQL via **JDBC** sert la **restitution BI** (connexion native Power BI,
requêtes interactives faibles latences sur des volumes modérés).

### Factuellement
Écriture JDBC depuis Spark (extrait `write_gold`) :
```python
(df.write.format("jdbc")
   .option("url", "jdbc:postgresql://host.docker.internal:5432/Cloud Healthcare Unit")
   .option("dbtable", f'"GOLD_{name}"')
   .mode("overwrite").save())
```
- `host.docker.internal` : depuis le conteneur Spark vers le PostgreSQL de l'hôte.
- Le nom de base **avec espaces** (`Cloud Healthcare Unit`) est accepté littéralement.
- Tables préfixées `GOLD_` : la restitution se branche dessus (Power BI → 8 analyses).

---

## 7. Gouvernance RGPD (transversale)

| Principe | Mise en œuvre |
|---|---|
| **Anonymisation à la source** | Filtrage PII **en Silver**, avant toute persistance analytique. |
| **Minimisation** | Seules les colonnes utiles à l'analyse sont conservées. |
| **Centralisation** | Listes PII = constantes `PII_EXACT` / `PII_CONTAINS` dans le job. |
| **Traçabilité** | Chaque suppression est **journalisée** à l'exécution (`[RGPD] …`). |
| **Champs supprimés** | nom, prénom, n° sécu, e-mail, téléphone, adresse (+ voie). |

La donnée brute (Bronze) reste, elle, à accès restreint : seul le **flux nettoyé**
(Silver/Gold) circule vers l'analytique.

---

## 8. Performance — Benchmark Parquet vs PostgreSQL

Mesure (5 exécutions, moyenne) de requêtes analytiques sur la couche Gold, selon
le moteur de stockage (`benchmark_gold.py`) :

| Requête | Parquet (HDFS/Spark) | PostgreSQL (JDBC) |
|---|---:|---:|
| Consultations par sexe | 1009 ms | **356 ms** |
| Consultations par année | 641 ms | **280 ms** |
| Hospitalisations par région | 401 ms | **195 ms** |
| Scan `count(*)` (1 M lignes) | **72 ms** | 174 ms |

**Interprétation** : à cette volumétrie (~1 M lignes), **PostgreSQL est plus rapide
sur les jointures/agrégations** (l'overhead d'ordonnancement distribué de Spark
domine sur de petits volumes), tandis que **Parquet écrase sur le scan** grâce à son
format colonne (le `count(*)` lit les métadonnées). L'avantage Spark/Parquet
s'inverse et devient décisif sur de **très gros volumes** (10–100× plus), là où le
parallélisme amortit son coût fixe. → graphe `datalake/benchmark/temps_reponse_medaillon.png`.

---

## 9. Comparaison Hive / Spark / Impala — et pourquoi Spark

| Critère | **Hive** | **Impala** | **Spark** (choisi) |
|---|---|---|---|
| Nature | SQL-on-Hadoop (batch) | Moteur SQL MPP interactif | Moteur de calcul distribué généraliste |
| Exécution | MapReduce / Tez (sur disque) | Démons C++ longue durée (MPP, en mémoire) | DAG en mémoire (RDD/DataFrame) |
| Latence | Élevée (batch) | **Très faible** (interactif/BI) | Faible à moyenne |
| Langages | SQL (HQL) uniquement | SQL uniquement | SQL **+ Python/Scala/Java** |
| Transformations complexes | Limitées (SQL) | Limitées (SQL) | **Complètes** (procédural, UDF, ML) |
| Connectivité | Tables Hive | Tables Hive/HDFS | Fichiers, Hive, **JDBC lecture+écriture**, Kafka… |
| Écriture vers SGBD externe | Non (nativement) | Non | **Oui (JDBC)** |
| Cas d'usage idéal | ETL batch SQL massif | Requêtes BI ad-hoc interactives | ETL/ELT, ML, streaming, pipelines |

### Pourquoi **Spark** pour le CHU
Notre pipeline a besoin de **trois choses qu'un moteur purement SQL (Hive/Impala)
ne fait pas, ou mal** :

1. **Des transformations procédurales** — filtrage RGPD colonne par colonne, parsing
   de dates multi-formats, calcul de durée (`heure_fin - heure_debut`), dérivation
   département→région. C'est naturel en PySpark, acrobatique en SQL pur.
2. **Lire *et* écrire vers PostgreSQL via JDBC** — capture des consultations depuis
   la base opérationnelle **et** chargement du Gold dans le DWH. Hive et Impala
   n'écrivent pas nativement vers un SGBD externe ; Spark le fait en une ligne.
3. **Un seul moteur pour tout le médaillon** — Bronze→Silver→Gold dans un seul
   langage (PySpark), réutilisable pour de la ML/streaming plus tard.

**Hive** a été *utilisé en exploration* au début du projet (tables externes
SQL-on-HDFS) et fonctionnait, mais son exécution **MapReduce** affichait
*« Hive-on-MR is deprecated »* et restait lente (~26 s pour agréger les décès) et
**SQL-only**. **Impala** aurait excellé pour la *restitution interactive*, mais
c'est ce rôle que tient déjà **PostgreSQL** côté BI dans notre architecture — ajouter
Impala aurait dupliqué la couche requête sans couvrir nos besoins de transformation.

> **Conclusion** : Spark pour le **traitement** (médaillon), PostgreSQL pour la
> **restitution**. Hive/Impala n'apportaient pas la flexibilité de transformation
> ni la connectivité JDBC dont le pipeline a besoin.

---

## 10. Annexes

### Commandes
```bash
# Cluster
cd datalake && docker compose up -d            # HDFS + Spark
bash ingest.sh                                 # sources -> /datalake/raw

# Pipeline médaillon (le flag --packages fournit le driver JDBC, indispensable)
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.5.4 /app/bronze_to_silver.py
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.5.4 /app/silver_to_gold.py

# Ajout FAIT_DECES (besoin n°7 : décès par région/année)
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.5.4 /app/add_fait_deces.py

# Benchmark
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.5.4 /app/benchmark_gold.py
python datalake/benchmark/plot_benchmark.py
```

### Interfaces
HDFS NameNode `:9870` · DataNode `:9864` · Spark Master `:8080`.

### Arborescence
```
datalake/
├── docker-compose.yml   hadoop.env   ingest.sh / ingest.ps1   README.md
├── spark/   bronze_to_silver.py · silver_to_gold.py · add_fait_deces.py · benchmark_gold.py
└── benchmark/  plot_benchmark.py · temps_reponse_medaillon.png
```
