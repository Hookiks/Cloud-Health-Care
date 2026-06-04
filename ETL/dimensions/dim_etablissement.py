"""DIM_ETABLISSEMENT — référentiel FINESS des établissements de santé.

Clé naturelle : identifiant_organisation (= identifiant_organisation des
hospitalisations). Le département et la région sont dérivés du code postal.
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import load_dataframe
from ETL.common.extract import read_etablissements
from ETL.common.geo import code_to_departement, departement_to_region


def build() -> pd.DataFrame:
    df = read_etablissements()
    dept = df["code_postal"].map(code_to_departement)
    out = pd.DataFrame({
        "finess": df["identifiant_organisation"].astype(str).str.strip(),
        "raison_sociale": df.get("raison_sociale_site"),
        "commune": df.get("commune"),
        "code_postal": df.get("code_postal"),
        "code_departement": dept,
        "region": dept.map(departement_to_region),
    })
    out = out[out["finess"].notna() & (out["finess"] != "") & (out["finess"] != "nan")]
    out = out.drop_duplicates(subset=["finess"])
    return out


def run() -> None:
    print("[DIM_ETABLISSEMENT]")
    load_dataframe(settings.dw_engine(), build(), "DIM_ETABLISSEMENT")


if __name__ == "__main__":
    run()
