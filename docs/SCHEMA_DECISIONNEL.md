# Schéma décisionnel — CHU (couche GOLD)

Schéma en **constellation** (galaxy schema) : **4 tables de faits + 6 dimensions**
partageant des dimensions communes — **aucun fait isolé**. Tables réellement
présentes dans le Data Warehouse PostgreSQL (`Cloud Healthcare Unit`) et dans HDFS
`/datalake/gold`. Les 4 faits couvrent les **8 besoins utilisateurs**.

## Diagramme

```
                              GOLD_DIM_TEMPS                 GOLD_DIM_DIAGNOSTIC
                              date_key (PK)                  code_diag (PK)
                              annee, trimestre, mois         libelle_diagnostic
                                     ▲                              ▲
                       date_key ─────┼──────────────┬───────────────┘ code_diag
                                     │              │
        GOLD_DIM_ETABLISSEMENT       │              │       GOLD_DIM_PROFESSIONNEL
        finess (PK)                  │              │       identifiant (PK)
        raison_sociale, commune,     │              │       profession, specialite,
        code_postal, dept, region    │              │       categorie_pro
              ▲        ▲             │              │              ▲
        finess│        │ finess      │              │   identifiant │
              │        └──────────┐  │              │   ┌───────────┘
   ┌──────────┴──────────┐    ┌───┴──┴──────────────┴───┴───┐
   │ GOLD_FAIT_          │    │ GOLD_FAIT_                   │
   │ HOSPITALISATION     │    │ CONSULTATION                │
   │ num_hospitalisation │    │ num_consultation            │
   │ patient_id    (FK)  │    │ patient_id      (FK)        │
   │ finess        (FK)  │    │ identifiant     (FK)        │
   │ code_diag     (FK)  │    │ finess          (FK) ← n°1  │
   │ date_key      (FK)  │    │ code_diag       (FK)        │
   │ ·jours_hospit       │    │ date_key        (FK)        │
   │ ·nb_hospitalisation │    │ ·duree_minutes ·nb_consult  │
   └──────────┬──────────┘    └──────────────┬──────────────┘
              │ patient_id                   │ patient_id
              └───────────────┬──────────────┘
                              ▼
                       GOLD_DIM_PATIENT
                       patient_id (PK)
                       sexe, age, tranche_age,
                       ville, code_postal, groupe_sanguin


                       GOLD_DIM_LOCALISATION
                       region (PK) · zone (Métropole/DOM)
                              ▲              ▲
                       region │              │ region
              ┌───────────────┘              └───────────────┐
   ┌──────────┴──────────────┐                  ┌────────────┴────────────┐
   │ GOLD_FAIT_DECES         │                  │ GOLD_FAIT_SATISFACTION   │
   │ (agrégé · besoin n°7)   │                  │ (agrégé · besoin n°8)    │
   │ region   (FK)           │                  │ region   (FK)            │
   │ annee                   │                  │ annee                    │
   │ sexe                    │                  │ ·nb_etablissements       │
   │ ·nb_deces (mesure)      │                  │ ·score_satisfaction      │
   │  ← 25 M décès → 2 962   │                  │ ·taux_recommandation     │
   └─────────────────────────┘                  └──────────────────────────┘
        (← e-Satis 2020 pour la satisfaction)
```

## Les 4 tables de faits

| Fait | Grain | Mesures | Dimensions reliées |
|------|-------|---------|--------------------|
| **GOLD_FAIT_HOSPITALISATION** | 1 hospitalisation | jours_hospitalisation, nb_hospitalisation | TEMPS, PATIENT, DIAGNOSTIC, ÉTABLISSEMENT |
| **GOLD_FAIT_CONSULTATION** | 1 consultation | duree_minutes, nb_consultation | TEMPS, PATIENT, DIAGNOSTIC, PROFESSIONNEL, **ÉTABLISSEMENT** |
| **GOLD_FAIT_DECES** | année × région × sexe (agrégé) | nb_deces | **LOCALISATION**, TEMPS (année) |
| **GOLD_FAIT_SATISFACTION** | année × région (agrégé) | nb_etablissements, score_satisfaction, taux_recommandation | **LOCALISATION**, TEMPS (année) |

## Les 6 dimensions (clés naturelles)

| Dimension | Clé (PK) | Attributs |
|-----------|----------|-----------|
| GOLD_DIM_TEMPS | date_key | date_complete, annee, trimestre, mois, jour |
| GOLD_DIM_PATIENT | patient_id | sexe, age, tranche_age, ville, code_postal, groupe_sanguin |
| GOLD_DIM_DIAGNOSTIC | code_diag | libelle_diagnostic |
| GOLD_DIM_ETABLISSEMENT | finess | raison_sociale, commune, code_postal, departement, region |
| GOLD_DIM_PROFESSIONNEL | identifiant | civilite, profession, categorie_professionnelle, code_specialite |
| GOLD_DIM_LOCALISATION | region | zone (Métropole / DOM) |

> **Constellation (galaxy schema)** — chaque fait partage ≥ 1 table de dimension :
> - `DIM_TEMPS`, `DIM_PATIENT`, `DIM_DIAGNOSTIC`, `DIM_ETABLISSEMENT` sont **partagés**
>   par HOSPITALISATION et CONSULTATION ; `DIM_PROFESSIONNEL` est propre à la consultation.
> - `DIM_LOCALISATION` est **partagé** par DECES et SATISFACTION (la région n'est plus
>   une dimension dégénérée → ces faits ne sont plus isolés).
> - **Pont** `DIM_ETABLISSEMENT.region → DIM_LOCALISATION.region` (relation dim-à-dim) :
>   relie la grappe événementielle (hospit/consult) à la grappe régionale →
>   **constellation entièrement connectée**, la région filtre les 4 faits.
>
> Les jointures se font par **clés naturelles** (pas de clé de substitution).

## Relations (matrice complète)

| Dimension (PK) | → Cible | Clé | Cardinalité |
|---|---|---|---|
| DIM_PATIENT (`patient_id`) | FAIT_HOSPITALISATION, FAIT_CONSULTATION | patient_id | 1 → N |
| DIM_ETABLISSEMENT (`finess`) | FAIT_HOSPITALISATION, FAIT_CONSULTATION | finess | 1 → N |
| DIM_DIAGNOSTIC (`code_diag`) | FAIT_HOSPITALISATION, FAIT_CONSULTATION | code_diag | 1 → N |
| DIM_TEMPS (`date_key`) | FAIT_HOSPITALISATION, FAIT_CONSULTATION | date_key | 1 → N |
| DIM_PROFESSIONNEL (`identifiant`) | FAIT_CONSULTATION | identifiant | 1 → N |
| DIM_LOCALISATION (`region`) | FAIT_DECES, FAIT_SATISFACTION | region | 1 → N |
| **DIM_LOCALISATION (`region`)** | **DIM_ETABLISSEMENT** (pont) | region | 1 → N |

> Power BI : pour activer le pont, créer la relation
> `DIM_ETABLISSEMENT[region]  →  DIM_LOCALISATION[region]` (plusieurs établissements
> par région). `annee` (sur DECES/SATISFACTION) reste un attribut de filtre, pas une FK.

## Couverture des besoins utilisateurs

| # | Besoin | Couvert par |
|---|--------|-------------|
| 1 | Consultations par établissement / période | FAIT_CONSULTATION × DIM_ETABLISSEMENT (lien via `finess`) |
| 2 | Consultations par diagnostic | FAIT_CONSULTATION × DIM_DIAGNOSTIC |
| 3 | Hospitalisations globales / période | FAIT_HOSPITALISATION × DIM_TEMPS |
| 4 | Hospitalisations par diagnostic | FAIT_HOSPITALISATION × DIM_DIAGNOSTIC |
| 5 | Hospitalisations par sexe / âge | FAIT_HOSPITALISATION × DIM_PATIENT |
| 6 | Consultations par professionnel | FAIT_CONSULTATION × DIM_PROFESSIONNEL |
| 7 | Décès par région (2019) | FAIT_DECES × DIM_LOCALISATION |
| 8 | Satisfaction par région (2020) | FAIT_SATISFACTION × DIM_LOCALISATION |

→ **Les 4 faits couvrent les 8 besoins (8/8).** Les faits HOSPITALISATION et
CONSULTATION sont au grain événement (détaillés) ; DECES et SATISFACTION sont
**agrégés** (grain région) et reliés à `DIM_LOCALISATION` → **vraie constellation,
aucun fait isolé**.
