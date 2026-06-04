"""FAIT_CONSULTATION — depuis la table Consultation (staging).

Résout les clés de substitution des dimensions puis charge via COPY.
Grain : une ligne par consultation.
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import copy_dataframe, read_lookup
from ETL.common.extract import read_csv_smart, read_table


def _prof_to_etablissement(dw_engine) -> pd.DataFrame:
    """Construit la correspondance professionnel -> etablissement_key via
    activite_professionnel_sante.csv (identifiant -> identifiant_organisation)
    puis DIM_ETABLISSEMENT (finess -> etablissement_key).

    Permet de rattacher une consultation à un établissement (besoin n°1),
    le rattachement n'existant pas directement dans la table Consultation.
    """
    act = read_csv_smart(settings.PATH_ACTIVITE_PRO, sep=";")
    act = act[act["identifiant_organisation"].notna()].copy()
    act["identifiant"] = act["identifiant"].astype(str).str.strip()
    act["finess"] = act["identifiant_organisation"].astype(str).str.strip()
    act = act.drop_duplicates(subset=["identifiant"])          # 1 organisation/pro

    etab = read_lookup(dw_engine, 'SELECT finess, etablissement_key FROM "DIM_ETABLISSEMENT"')
    link = act.merge(etab, on="finess", how="inner")
    return link[["identifiant", "etablissement_key"]]


def build(src_engine, dw_engine) -> pd.DataFrame:
    df = read_table(src_engine, "Consultation")

    # Correspondances clé naturelle -> clé de substitution
    patient = read_lookup(dw_engine, 'SELECT id_patient, patient_key FROM "DIM_PATIENT"')
    prof = read_lookup(dw_engine, 'SELECT identifiant, professionnel_key FROM "DIM_PROFESSIONNEL"')
    diag = read_lookup(dw_engine, 'SELECT code_diag, diagnostic_key FROM "DIM_DIAGNOSTIC"')
    mut = read_lookup(dw_engine, 'SELECT id_mut, mutuelle_key FROM "DIM_MUTUELLE"')
    etab_link = _prof_to_etablissement(dw_engine)

    df["Id_patient"] = pd.to_numeric(df["Id_patient"], errors="coerce")
    df["Id_mut"] = pd.to_numeric(df["Id_mut"], errors="coerce")
    df["Id_prof_sante"] = df["Id_prof_sante"].astype(str).str.strip()
    df["Code_diag"] = df["Code_diag"].astype(str).str.strip()

    df = df.merge(patient, left_on="Id_patient", right_on="id_patient", how="left")
    df = df.merge(prof, left_on="Id_prof_sante", right_on="identifiant", how="left")
    df = df.merge(diag, left_on="Code_diag", right_on="code_diag", how="left")
    df = df.merge(mut, left_on="Id_mut", right_on="id_mut", how="left")
    df = df.merge(etab_link, left_on="Id_prof_sante", right_on="identifiant", how="left")

    dt = pd.to_datetime(df["Date"], errors="coerce")
    debut = pd.to_datetime(df["Heure_debut"].astype(str), format="%H:%M:%S", errors="coerce")
    fin = pd.to_datetime(df["Heure_fin"].astype(str), format="%H:%M:%S", errors="coerce")

    out = pd.DataFrame({
        "num_consultation": pd.to_numeric(df["Num_consultation"], errors="coerce").astype("Int64"),
        "patient_key": df["patient_key"].astype("Int64"),
        "professionnel_key": df["professionnel_key"].astype("Int64"),
        "etablissement_key": df["etablissement_key"].astype("Int64"),
        "diagnostic_key": df["diagnostic_key"].astype("Int64"),
        "mutuelle_key": df["mutuelle_key"].astype("Int64"),
        "date_key": dt.dt.strftime("%Y%m%d").astype("Int64"),
        "annee": dt.dt.year.astype("Int64"),
        "duree_minutes": ((fin - debut).dt.total_seconds() / 60).round().astype("Int64"),
        "nb_consultation": 1,
    })
    return out


def run() -> None:
    print("[FAIT_CONSULTATION]")
    copy_dataframe(settings.dw_engine(), build(settings.source_engine(), settings.dw_engine()),
                   '"FAIT_CONSULTATION"')


if __name__ == "__main__":
    run()
