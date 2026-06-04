"""DIM_DIAGNOSTIC — table Diagnostic (staging) enrichie des codes
diagnostics présents dans le fichier des hospitalisations (afin qu'aucune
clé de fait ne soit orpheline)."""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import load_dataframe
from ETL.common.extract import read_hospitalisations, read_table


def build(src_engine) -> pd.DataFrame:
    diag = read_table(src_engine, "Diagnostic").rename(
        columns={"Code_diag": "code_diag", "Diagnostic": "libelle_diagnostic"})

    # Codes présents dans les hospitalisations mais éventuellement absents du référentiel
    hosp = read_hospitalisations()
    extra = pd.DataFrame({
        "code_diag": hosp["Code_diagnostic"],
        "libelle_diagnostic": hosp["Suite_diagnostic_consultation"],
    })

    out = pd.concat([diag, extra], ignore_index=True)
    out = out.dropna(subset=["code_diag"])
    out["code_diag"] = out["code_diag"].astype(str).str.strip()
    # Garde le premier libellé non nul par code
    out = (out.sort_values("libelle_diagnostic")
              .drop_duplicates(subset=["code_diag"], keep="first"))
    return out[["code_diag", "libelle_diagnostic"]]


def run() -> None:
    print("[DIM_DIAGNOSTIC]")
    load_dataframe(settings.dw_engine(), build(settings.source_engine()), "DIM_DIAGNOSTIC")


if __name__ == "__main__":
    run()
