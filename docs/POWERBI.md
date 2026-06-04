# Guide Power BI — Restitution CHU (Livrable 3)

Ce guide explique comment brancher **Power BI Desktop** sur l'entrepôt
`Cloud Healthcare Unit` et construire le tableau de bord des 8 analyses.

> Un fichier `.pbix` ne peut pas être généré par script (format binaire propriétaire).
> On fournit donc : les **8 vues SQL** prêtes à l'emploi, un **export Excel**
> (`benchmarks/analyses_chu.xlsx`) et ce guide de construction.

---

## Option A — Connexion directe à PostgreSQL (recommandée)

1. **Accueil → Obtenir les données → Base de données PostgreSQL**.
2. Serveur : `localhost:5432` · Base de données : `Cloud Healthcare Unit`.
3. Mode : **Importer** (plus fluide pour la démo).
4. Identifiants : utilisateur `postgres` / mot de passe (votre `.env`).
5. Dans le navigateur, cocher les **8 vues** :
   `V_CONSULT_ETABLISSEMENT`, `V_CONSULT_DIAGNOSTIC`, `V_HOSPIT_PERIODE`,
   `V_HOSPIT_DIAGNOSTIC`, `V_HOSPIT_SEXE_AGE`, `V_CONSULT_PROFESSIONNEL`,
   `V_DECES_REGION`, `V_SATISFACTION_REGION`.
   *(Pré-requis : avoir exécuté `sql/05_views_analyses.sql`.)*
6. **Charger**.

> Pour un modèle en étoile complet (slicers transverses), importer plutôt les
> tables `DIM_*` et `FAIT_*` : Power BI détecte les relations via les clés
> `*_key` (sinon les créer manuellement dans la vue *Modèle*).

## Option B — Sans base : import du classeur Excel

**Obtenir les données → Excel** → `benchmarks/analyses_chu.xlsx`
(un onglet par analyse). Idéal en secours si la connexion BDD n'est pas dispo.

---

## Visuels suggérés (1 page = 1 besoin)

| # | Besoin | Vue | Visuel Power BI | Filtres (slicers) |
|---|--------|-----|-----------------|-------------------|
| 1 | Consultation par établissement / période | `V_CONSULT_ETABLISSEMENT` | Histogramme `nb_consultations` par `raison_sociale` | `raison_sociale` (X), `annee` (Y) |
| 2 | Consultation par diagnostic / période | `V_CONSULT_DIAGNOSTIC` | Barres Top N par `libelle_diagnostic` | `code_diag` (X), `annee` (Y) |
| 3 | Hospitalisation globale / période | `V_HOSPIT_PERIODE` | Courbe `nb_hospitalisations` par `annee` | `annee` (Y) |
| 4 | Hospitalisation par diagnostic | `V_HOSPIT_DIAGNOSTIC` | Barres `taux_pct` par diagnostic | `annee` |
| 5 | Hospitalisation par sexe / âge | `V_HOSPIT_SEXE_AGE` | Barres empilées `nb` par `tranche_age`, légende `sexe` | — |
| 6 | Consultation par professionnel | `V_CONSULT_PROFESSIONNEL` | Tableau / barres Top praticiens | `specialite` |
| 7 | Décès par région (2019) | `V_DECES_REGION` | **Carte / Treemap** `nb_deces` par `region` | `annee = 2019` |
| 8 | Satisfaction par région (2020) | `V_SATISFACTION_REGION` | Carte choroplèthe `score_satisfaction` | `annee = 2020` |

### Astuces
- **Carte régionale** : champ `region` typé *Localisation* → Power BI géocode.
- **Mesure "Taux"** : utiliser la colonne `taux_pct` des vues, ou recréer en DAX
  `Taux % = DIVIDE([Nb], CALCULATE([Nb], ALL(...))) * 100`.
- Ajouter un **slicer `annee`** commun à toutes les pages (synchronisé) pour
  répondre à « sur une période Y ».

---

## Régénérer les supports
```bash
psql -d "Cloud Healthcare Unit" -f sql/05_views_analyses.sql   # (re)créer les vues
python -m scripts.export_analyses                              # -> benchmarks/analyses_chu.xlsx
```
