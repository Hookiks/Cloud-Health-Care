"""DIM_PATIENT — depuis la table opérationnelle Patient (staging).

RGPD : les colonnes directement identifiantes (nom, prénom, adresse, e-mail,
téléphone, n° sécu) sont retirées AVANT chargement (cf. ETL/common/rgpd.py).
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import load_dataframe
from ETL.common.extract import read_table
from ETL.common.rgpd import filter_pii

_BORNES = [0, 18, 30, 45, 60, 75, 200]
_LIBELLES = ["0-17", "18-29", "30-44", "45-59", "60-74", "75+"]


def _tranche(age: pd.Series) -> pd.Series:
    return pd.cut(pd.to_numeric(age, errors="coerce"), bins=_BORNES,
                 labels=_LIBELLES, right=False)


def build(src_engine) -> pd.DataFrame:
    df = read_table(src_engine, "Patient")
    df = filter_pii(df, "patient")           # <-- conformité RGPD
    out = pd.DataFrame({
        "id_patient": pd.to_numeric(df["Id_patient"], errors="coerce").astype("Int64"),
        "sexe": df["Sexe"],
        "age": pd.to_numeric(df["Age"], errors="coerce").astype("Int64"),
        "ville": df["Ville"],
        "code_postal": df["Code_postal"],
        "groupe_sanguin": df["Groupe_sanguin"],
    })
    out["tranche_age"] = _tranche(out["age"]).astype("object")
    out = out.dropna(subset=["id_patient"]).drop_duplicates(subset=["id_patient"])
    return out[["id_patient", "sexe", "age", "tranche_age", "ville",
                "code_postal", "groupe_sanguin"]]


def run() -> None:
    print("[DIM_PATIENT]")
    load_dataframe(settings.dw_engine(), build(settings.source_engine()), "DIM_PATIENT")


if __name__ == "__main__":
    run()
