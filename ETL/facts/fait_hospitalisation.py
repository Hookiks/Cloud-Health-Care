"""FAIT_HOSPITALISATION — depuis Hospitalisations.csv.

Grain : une ligne par hospitalisation ; mesure = jours d'hospitalisation.
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import copy_dataframe, read_lookup
from ETL.common.extract import read_hospitalisations


def build(dw_engine) -> pd.DataFrame:
    df = read_hospitalisations()

    patient = read_lookup(dw_engine, 'SELECT id_patient, patient_key FROM "DIM_PATIENT"')
    etab = read_lookup(dw_engine, 'SELECT finess, etablissement_key FROM "DIM_ETABLISSEMENT"')
    diag = read_lookup(dw_engine, 'SELECT code_diag, diagnostic_key FROM "DIM_DIAGNOSTIC"')

    df["Id_patient"] = pd.to_numeric(df["Id_patient"], errors="coerce")
    df["identifiant_organisation"] = df["identifiant_organisation"].astype(str).str.strip()
    df["Code_diagnostic"] = df["Code_diagnostic"].astype(str).str.strip()

    df = df.merge(patient, left_on="Id_patient", right_on="id_patient", how="left")
    df = df.merge(etab, left_on="identifiant_organisation", right_on="finess", how="left")
    df = df.merge(diag, left_on="Code_diagnostic", right_on="code_diag", how="left")

    dt = pd.to_datetime(df["Date_Entree"], format="%d/%m/%Y", errors="coerce")

    out = pd.DataFrame({
        "num_hospitalisation": pd.to_numeric(df["Num_Hospitalisation"], errors="coerce").astype("Int64"),
        "patient_key": df["patient_key"].astype("Int64"),
        "etablissement_key": df["etablissement_key"].astype("Int64"),
        "diagnostic_key": df["diagnostic_key"].astype("Int64"),
        "date_key": dt.dt.strftime("%Y%m%d").astype("Int64"),
        "annee": dt.dt.year.astype("Int64"),
        "jours_hospitalisation": pd.to_numeric(df["Jour_Hospitalisation"], errors="coerce").astype("Int64"),
        "nb_hospitalisation": 1,
    })
    return out


def run() -> None:
    print("[FAIT_HOSPITALISATION]")
    copy_dataframe(settings.dw_engine(), build(settings.dw_engine()), '"FAIT_HOSPITALISATION"')


if __name__ == "__main__":
    run()
