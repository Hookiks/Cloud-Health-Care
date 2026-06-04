"""DIM_PROFESSIONNEL — Professionnel_de_sante joint à Specialites.

RGPD : nom / prénom du praticien retirés ; on conserve l'identifiant
professionnel, la catégorie et la spécialité.
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import load_dataframe
from ETL.common.extract import read_table
from ETL.common.rgpd import filter_pii


def build(src_engine) -> pd.DataFrame:
    prof = read_table(src_engine, "Professionnel_de_sante")
    prof = filter_pii(prof, "professionnel")
    spec = read_table(src_engine, "Specialites")

    df = prof.merge(spec, on="Code_specialite", how="left")
    out = pd.DataFrame({
        "identifiant": df["Identifiant"].astype(str).str.strip(),
        "civilite": df.get("Civilite"),
        "categorie_professionnelle": df.get("Categorie_professionnelle"),
        "profession": df.get("Profession"),
        "code_specialite": df.get("Code_specialite"),
        "specialite": df.get("Specialite"),
        "fonction": df.get("Fonction"),
    })
    out = out.dropna(subset=["identifiant"]).drop_duplicates(subset=["identifiant"])
    return out


def run() -> None:
    print("[DIM_PROFESSIONNEL]")
    load_dataframe(settings.dw_engine(), build(settings.source_engine()), "DIM_PROFESSIONNEL")


if __name__ == "__main__":
    run()
