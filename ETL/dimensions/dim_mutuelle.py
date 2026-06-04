"""DIM_MUTUELLE — table Mutuelle (staging). RGPD : adresse retirée."""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import load_dataframe
from ETL.common.extract import read_table
from ETL.common.rgpd import filter_pii


def build(src_engine) -> pd.DataFrame:
    df = read_table(src_engine, "Mutuelle")
    df = filter_pii(df, "mutuelle")
    out = pd.DataFrame({
        "id_mut": pd.to_numeric(df["Id_Mut"], errors="coerce").astype("Int64"),
        "nom_mutuelle": df["Nom"],
    })
    out = out.dropna(subset=["id_mut"]).drop_duplicates(subset=["id_mut"])
    return out


def run() -> None:
    print("[DIM_MUTUELLE]")
    load_dataframe(settings.dw_engine(), build(settings.source_engine()), "DIM_MUTUELLE")


if __name__ == "__main__":
    run()
