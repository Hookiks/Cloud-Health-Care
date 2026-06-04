"""DIM_TEMPS — dimension temps générée par programme.

Aucune source externe : on génère un calendrier complet sur la plage
configurée (DIM_TEMPS_START..DIM_TEMPS_END).
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import load_dataframe

MOIS = ["janvier", "février", "mars", "avril", "mai", "juin",
        "juillet", "août", "septembre", "octobre", "novembre", "décembre"]
JOURS = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]


def build() -> pd.DataFrame:
    dates = pd.date_range(settings.DIM_TEMPS_START, settings.DIM_TEMPS_END, freq="D")
    df = pd.DataFrame({"date_complete": dates})
    df["date_key"] = df["date_complete"].dt.strftime("%Y%m%d").astype(int)
    df["annee"] = df["date_complete"].dt.year
    df["trimestre"] = df["date_complete"].dt.quarter
    df["mois"] = df["date_complete"].dt.month
    df["mois_nom"] = df["mois"].map(lambda m: MOIS[m - 1])
    df["jour"] = df["date_complete"].dt.day
    df["jour_semaine"] = df["date_complete"].dt.weekday + 1   # 1=lundi
    df["jour_nom"] = df["date_complete"].dt.weekday.map(lambda d: JOURS[d])
    df["est_weekend"] = df["jour_semaine"] >= 6
    df["date_complete"] = df["date_complete"].dt.date
    return df[["date_key", "date_complete", "annee", "trimestre", "mois", "mois_nom",
               "jour", "jour_semaine", "jour_nom", "est_weekend"]]


def run() -> None:
    print("[DIM_TEMPS]")
    engine = settings.dw_engine()
    load_dataframe(engine, build(), "DIM_TEMPS")


if __name__ == "__main__":
    run()
