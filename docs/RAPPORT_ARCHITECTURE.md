# Rapport d'architecture — Entrepôt de données décisionnel CHU
### Cloud Healthcare Unit · Système décisionnel Big Data santé

---

## 1. Contexte et objectifs

Le groupe hospitalier **CHU (Cloud Healthcare Unit)** engage sa transformation
digitale et souhaite exploiter la masse de données générée par ses systèmes de
gestion de soins et ses flux FTP. L'objectif est de construire un **entrepôt de
données décisionnel** permettant d'extraire, stocker, explorer et visualiser les
données médicales selon de multiples axes d'analyse, dans un cadre de **haute
sécurité** et de **conformité RGPD**.

Le système répond à 8 besoins analytiques (praticiens, chefs d'établissement) :
consultations par établissement / diagnostic / professionnel, hospitalisations
globales / par diagnostic / par sexe-âge, décès par région, satisfaction par région.

**Contraintes directrices** : ratio coût-efficacité, sécurité, élasticité,
scalabilité, protection des données personnelles de santé.

---

## 2. Vue d'ensemble de l'architecture

L'architecture est organisée en **couches** (approche *data lake + data warehouse*) :

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SOURCES (hétérogènes)                                                   │
│  • Dump PostgreSQL (soins médico-administratifs)                         │
│  • CSV FINESS (établissements)   • deces.csv (~2 Go, INSEE)              │
│  • e-Satis (satisfaction, CSV/XLSX, FTP)                                 │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │  ingestion
┌───────────────────────────────▼──────────────────────────────────────────┐
│  DATA LAKE — HDFS (zone brute / landing)      [Docker : HDFS+Hive+Spark] │
│  /datalake/raw/{hospitalisation,finess,deces,satisfaction}               │
│   • Hive  : exploration SQL-on-HDFS (schema-on-read)                     │
│   • Spark : traitement distribué + RGPD                                  │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │  ETL Python (pandas) — extract→RGPD→transform→load
┌───────────────────────────────▼──────────────────────────────────────────┐
│  STAGING — PostgreSQL `postgres` (tables opérationnelles restaurées)     │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │  résolution des clés de substitution
┌───────────────────────────────▼──────────────────────────────────────────┐
│  DATA WAREHOUSE — PostgreSQL `Cloud Healthcare Unit`                     │
│  Schéma en ÉTOILE : 7 dimensions + 4 faits                               │
│  Partitionnement (RANGE/HASH) · Index · Vues d'analyse                   │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │  connexion BI
┌───────────────────────────────▼──────────────────────────────────────────┐
│  RESTITUTION — Power BI (8 analyses) · exports Excel · graphiques        │
└──────────────────────────────────────────────────────────────────────────┘
```

**Choix technologiques et justification**

| Couche | Technologie | Justification |
|---|---|---|
| Data lake | **HDFS + Hive + Spark** (Docker) | Stockage distribué des fichiers bruts volumineux ; exploration SQL et traitement parallèle ; démontre la scalabilité « Big Data ». |
| Staging | **PostgreSQL** | Restauration directe du dump opérationnel ; isolation source/cible. |
| Data Warehouse | **PostgreSQL** | SGBD relationnel robuste, gratuit, partitionnement natif ; volumétrie (~25 M lignes) parfaitement gérée sans le coût d'un cluster. |
| ETL | **Python (pandas + SQLAlchemy)** | Lisible, portable, traitement chunké des gros fichiers, filtrage RGPD centralisé. |
| Restitution | **Power BI** | Tableaux de bord interactifs, connexion native PostgreSQL. |

> **Note de positionnement** : un cluster Hadoop n'est pas *indispensable* à cette
> volumétrie ; le data lake HDFS est intégré pour répondre à l'exigence
> d'architecture Big Data (fichiers distribués, scalabilité) et offrir un second
> moteur de traitement (Spark), tandis que PostgreSQL assure le ratio
> coût-efficacité côté entrepôt.

---

## 3. Sources de données

| Source | Format | Volume | Clé / particularité |
|---|---|---|---|
| BDD opérationnelle | dump PostgreSQL custom (`PGDMP`) | 12 tables | Patient, Consultation, Diagnostic, Professionnel_de_sante, Specialites, Mutuelle, Salle… |
| Établissements | `etablissement_sante.csv` (`;`) | 416 k | clé **FINESS** (`identifiant_organisation`) |
| Activité pro. | `activite_professionnel_sante.csv` (`;`) | 1,8 M | lien professionnel → organisation (FINESS) |
| Hospitalisations | `Hospitalisations.csv` (`;`) | 2,5 k | dates `dd/MM/yyyy` |
| Décès | `deces.csv` (`,`) | **~2 Go / 25 M** | registre INSEE, code commune |
| Satisfaction | e-Satis CSV/XLSX | 2014→2020 | colonne `region`, score global |

---

## 4. Couche Data Lake HDFS (`datalake/`)

Le data lake matérialise la **zone brute** distribuée.

- **HDFS** : système de fichiers distribué (1 NameNode + 1 DataNode). Les fichiers
  bruts sont ingérés dans `/datalake/raw/<domaine>/` via `hdfs dfs -put`.
- **Hive** : tables **externes** (schema-on-read) exposant les fichiers HDFS comme
  des tables SQL sans copie — exploration immédiate du lac
  (`datalake/hive/create_external_tables.sql`).
- **Spark** : moteur de traitement distribué. Le job
  `datalake/spark/spark_to_postgres.py` lit HDFS, **applique le RGPD**, agrège et
  écrit dans PostgreSQL via JDBC — illustrant le chemin « lac → entrepôt ».

Stack lancée via `docker compose up -d` (cf. `datalake/README.md`).
Interfaces : HDFS `:9870`, Spark `:8080`, Hive `:10000/:10002`.

---

## 5. Modèle dimensionnel (schéma en étoile)

L'entrepôt suit un **modèle en étoile** : des tables de **faits** (mesures
quantitatives) entourées de **dimensions** (axes d'analyse), reliées par des
**clés de substitution** (`*_key`, SERIAL).

### 7 dimensions
| Dimension | Grain | Attributs clés |
|---|---|---|
| `DIM_TEMPS` | jour | année, trimestre, mois, jour_semaine, week-end |
| `DIM_PATIENT` | patient | sexe, âge, **tranche_age**, ville, code_postal, groupe_sanguin |
| `DIM_DIAGNOSTIC` | code diag. | code_diag, libellé |
| `DIM_PROFESSIONNEL` | praticien | profession, spécialité, catégorie |
| `DIM_ETABLISSEMENT` | FINESS | raison sociale, commune, département, région |
| `DIM_LOCALISATION` | région | région (métropole + DOM) |
| `DIM_MUTUELLE` | mutuelle | nom |

### 4 tables de faits
| Fait | Grain | Mesures |
|---|---|---|
| `FAIT_CONSULTATION` | 1 consultation | nb_consultation, durée (min) |
| `FAIT_HOSPITALISATION` | 1 hospitalisation | nb_hospitalisation, jours |
| `FAIT_DECES` | année × région × sexe (agrégé) | nb_deces |
| `FAIT_SATISFACTION` | année × région (agrégé) | score, taux de recommandation |

**Point de modélisation — consultation ↔ établissement.** La table `Consultation`
ne porte pas de FINESS. Le rattachement à l'établissement est **reconstruit** via
le professionnel : `Consultation.Id_prof_sante` → `activite_professionnel_sante`
(`identifiant` → `identifiant_organisation`) → `DIM_ETABLISSEMENT.finess`
(couverture ~64 %). Cela débloque le besoin n°1.

---

## 6. La pipeline ETL

Le pipeline est orchestré par `ETL/run_pipeline.py` selon la séquence
**Extract → RGPD → Transform → Load**, dimensions **avant** faits (les faits
résolvent les clés de substitution des dimensions).

```
run_pipeline.py
 ├─ (--ddl)      création du schéma : sql/01_dimensions, 02_faits, 04_index
 ├─ DIMENSIONS   dim_temps · dim_localisation · dim_patient · dim_diagnostic
 │               dim_professionnel · dim_mutuelle · dim_etablissement
 ├─ FAITS        fait_consultation · fait_hospitalisation · fait_deces · fait_satisfaction
 └─ (--partition) sql/03_partitioning  + contrôle des volumes
```

**Étapes par job** (modules `ETL/dimensions/*` et `ETL/facts/*`) :
1. **Extract** (`ETL/common/extract.py`) — lecture du staging (SQLAlchemy) ou des
   fichiers (CSV/XLSX). Lecture **tolérante aux encodages** (UTF-8 → latin-1) et
   **chunkée** pour `deces.csv` (blocs de 500 k lignes : le fichier de 2 Go n'est
   jamais entièrement en mémoire).
2. **RGPD** (`ETL/common/rgpd.py`) — suppression des colonnes personnelles *avant*
   toute écriture (cf. §7).
3. **Transform** — normalisation, calcul d'attributs (tranche d'âge, durée,
   département → région via `ETL/common/geo.py`), résolution des clés de
   substitution par jointure avec les dimensions.
4. **Load** (`ETL/common/db.py`) — chargement via **`COPY`** (psycopg2
   `copy_expert`), bien plus rapide que des `INSERT` à l'échelle du million de
   lignes.

**Briques transverses** (`ETL/common/`) : `db` (COPY, DDL, lookups), `extract`
(lecteurs), `rgpd` (filtrage PII), `geo` (référentiel département→région).
La configuration (connexions, chemins) est centralisée dans `chu_config/`
(nommé ainsi pour éviter la collision avec le paquet PyPI `config`).

**Volumes chargés** : DIM_PROFESSIONNEL 1,05 M · DIM_ETABLISSEMENT 416 k ·
FAIT_CONSULTATION 1,03 M · FAIT_DECES 25 M décès agrégés. Pipeline complet ≈ 3-4 min.

---

## 7. RGPD et gouvernance des données

La protection des données de santé est traitée **by design** : le filtrage des
données à caractère personnel (PII) intervient **avant tout chargement** dans
l'entrepôt, jamais après.

**Principe — source unique de vérité.** La liste des colonnes sensibles par source
est centralisée dans `chu_config/pii_fields.py` ; la fonction `filter_pii()`
(`ETL/common/rgpd.py`) l'applique dans **chaque** job concerné et **journalise**
les colonnes retirées (auditabilité).

| Source | Colonnes PII supprimées | Conservé (utile et non identifiant) |
|---|---|---|
| `Patient` → DIM_PATIENT | Nom, Prénom, Adresse, **EMail, Tel, Num_Secu** | sexe, âge, tranche d'âge, ville, code postal, groupe sanguin |
| `deces` → FAIT_DECES | nom, prénom, numéro d'acte | sexe, dates, code géographique (→ région) |
| `Professionnel_de_sante` → DIM_PROFESSIONNEL | Nom, Prénom | identifiant pro, profession, spécialité |
| `Mutuelle` → DIM_MUTUELLE | Adresse | nom |

**Mesures de gouvernance complémentaires**
- **Minimisation** : seules les colonnes utiles à l'analyse sont conservées.
- **Agrégation** : décès et satisfaction sont stockés **agrégés** (par région /
  année), réduisant le risque de ré-identification.
- **Séparation des bases** : staging opérationnel et entrepôt décisionnel séparés.
- **Secrets hors code** : identifiants de connexion dans `.env` (non versionné,
  `.gitignore`).
- **Traçabilité** : chaque suppression PII est loggée à l'exécution.
- **RGPD côté lac** : le job Spark applique le **même** filtrage avant traitement.

---

## 8. Modèle physique et optimisation (Livrable 2)

- **Chargement performant** : `COPY FROM STDIN` pour tous les chargements.
- **Index** (`sql/04_indexes.sql`) : sur chaque clé étrangère des faits et sur les
  colonnes de filtre fréquentes (sexe, tranche d'âge, région, année).
- **Partitionnement** (`sql/03_partitioning.sql`, Livrable 2) :
  - **RANGE par année** sur les faits (élimine les partitions hors période —
    *partition pruning*) ;
  - **HASH (buckets)** sur `patient_key` (répartition uniforme, parallélisme).
- **Mesure de performance** : `scripts/benchmark.py` chronométre des requêtes
  représentatives (tables de base vs partitionnées) et produit un graphe
  (`benchmarks/temps_reponse.png`). *Constat* : à cette volumétrie, l'index sur
  `annee` rivalise avec le pruning ; le gain du partitionnement se révèle surtout
  sur de très gros volumes et pour l'archivage/purge par partition.

---

## 9. Les 8 analyses (besoins utilisateurs)

Chaque besoin est servi par une **vue SQL** (`sql/05_views_analyses.sql`) et un
graphique (`scripts/dashboard.py`) :

| # | Besoin | Vue |
|---|---|---|
| 1 | Consultations par établissement / période | `V_CONSULT_ETABLISSEMENT` |
| 2 | Consultations par diagnostic / période | `V_CONSULT_DIAGNOSTIC` |
| 3 | Hospitalisations globales / période | `V_HOSPIT_PERIODE` |
| 4 | Hospitalisations par diagnostic | `V_HOSPIT_DIAGNOSTIC` |
| 5 | Hospitalisations par sexe et âge | `V_HOSPIT_SEXE_AGE` |
| 6 | Consultations par professionnel | `V_CONSULT_PROFESSIONNEL` |
| 7 | Décès par région (2019) | `V_DECES_REGION` |
| 8 | Satisfaction par région (2020) | `V_SATISFACTION_REGION` |

Le « taux » est calculé par fonctions fenêtre (`SUM(...) OVER (PARTITION BY ...)`).

---

## 10. Restitution (Livrable 3)

- **Power BI** : connexion directe à PostgreSQL sur les 8 vues ; un visuel par
  besoin (histogrammes, courbes, cartes régionales). Guide pas-à-pas dans
  `docs/POWERBI.md`.
- **Export Excel** (`scripts/export_analyses.py` → `benchmarks/analyses_chu.xlsx`) :
  un onglet par analyse, importable dans Power BI ou utilisable en secours.
- **Graphiques PNG** (`scripts/dashboard.py` → `dashboards/`) : supports directs
  pour la soutenance et le rapport.

---

## 11. Préconisations d'outillage

| Besoin | Outil retenu | Alternatives |
|---|---|---|
| Stockage brut distribué | **HDFS** | Amazon S3, Azure Data Lake |
| Exploration du lac | **Hive** | Presto/Trino, Spark SQL |
| Traitement distribué | **Spark** | Flink, Dask |
| Intégration / ETL | **Python (pandas)** | Apache Hop, Talend, dbt |
| Entrepôt | **PostgreSQL** | Snowflake, BigQuery, Redshift |
| Visualisation | **Power BI** | Tableau, Metabase, Superset |
| Conteneurisation | **Docker Compose** | Kubernetes (montée en charge) |

**Sécurité / production** : chiffrement au repos et en transit (TLS), gestion des
secrets (Vault), contrôle d'accès par rôles, journalisation des accès, pseudonymisation.

---

## 12. Mise en œuvre — récapitulatif

```bash
# Entrepôt (PostgreSQL)
pip install -r requirements.txt && cp .env.example .env   # renseigner le mot de passe
python -m ETL.run_pipeline --ddl --partition              # ETL complet
psql -d "Cloud Healthcare Unit" -f sql/05_views_analyses.sql
python -m scripts.dashboard                               # graphiques

# Data lake (Docker)
cd datalake && docker compose up -d && bash ingest.sh     # HDFS + ingestion
```

**Arborescence**
```
chu_config/   configuration + référentiel RGPD
ETL/          common/ · dimensions/ · facts/ · run_pipeline.py
sql/          01..05 (DDL, partitionnement, index, vues)
scripts/      benchmark · export_analyses · dashboard · restore_dump
datalake/     docker-compose · hive/ · spark/ · ingest
docs/         RAPPORT_ARCHITECTURE.md · POWERBI.md
```
