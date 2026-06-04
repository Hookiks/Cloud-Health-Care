"""FAIT_SATISFACTION — résultats e-Satis 48h MCO 2020.

Le fichier XLSX porte déjà la colonne `region`. Grain final : année x région
(score global moyen et taux de recommandation moyen par région).
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import copy_dataframe, read_lookup
from ETL.common.extract import read_satisfaction_2020

# Normalisation des libellés région du fichier vers ceux de DIM_LOCALISATION
_ALIAS = {
    "Auvergne-Rhone-Alpes": "Auvergne-Rhône-Alpes",
    "Bourgogne-Franche-Comte": "Bourgogne-Franche-Comté",
    "Ile-de-France": "Île-de-France",
    "Ile de France": "Île-de-France",
    "Hauts de France": "Hauts-de-France",
    "Nouvelle Aquitaine": "Nouvelle-Aquitaine",
    "Provence-Alpes-Cote d'Azur": "Provence-Alpes-Côte d'Azur",
    "PACA": "Provence-Alpes-Côte d'Azur",
    "La Reunion": "La Réunion",
    "Ocean Indien": "La Réunion",
    "Océan Indien": "La Réunion",
}


def build() -> pd.DataFrame:
    df = read_satisfaction_2020()
    df["region"] = df["region"].astype(str).str.strip().replace(_ALIAS)
    df["score"] = pd.to_numeric(df["score_all_rea_ajust"], errors="coerce")
    df["reco"] = pd.to_numeric(df["taux_reco_brut"], errors="coerce")

    agg = (df.groupby("region")
             .agg(nb_etablissements=("finess", "nunique"),
                  score_satisfaction=("score", "mean"),
                  taux_recommandation=("reco", "mean"))
             .reset_index())
    agg["annee"] = 2020
    agg["score_satisfaction"] = agg["score_satisfaction"].round(2)
    agg["taux_recommandation"] = agg["taux_recommandation"].round(2)

    loc = read_lookup(settings.dw_engine(),
                      'SELECT region, localisation_key FROM "DIM_LOCALISATION"')
    agg = agg.merge(loc, on="region", how="left")
    return agg[["annee", "localisation_key", "nb_etablissements",
                "score_satisfaction", "taux_recommandation"]]


def run() -> None:
    print("[FAIT_SATISFACTION]")
    copy_dataframe(settings.dw_engine(), build(), '"FAIT_SATISFACTION"')


if __name__ == "__main__":
    run()
