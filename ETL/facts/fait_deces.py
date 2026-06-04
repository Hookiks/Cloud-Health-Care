"""FAIT_DECES — registre INSEE des décès (~2 Go), lu par blocs.

Le fichier n'est jamais chargé entièrement en mémoire : chaque bloc est
filtré (RGPD : nom/prénom retirés), enrichi (région via code commune INSEE)
puis agrégé. Grain final : année x région x sexe.
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import copy_dataframe, read_lookup
from ETL.common.extract import read_deces_chunks
from ETL.common.geo import code_to_region
from ETL.common.rgpd import filter_pii

_SEXE = {"1": "Homme", "2": "Femme"}


def build() -> pd.DataFrame:
    cumul: dict[tuple, int] = {}
    n_blocs = 0
    for chunk in read_deces_chunks():
        chunk = filter_pii(chunk, "deces")        # <-- conformité RGPD
        annee = pd.to_datetime(chunk["date_deces"], errors="coerce").dt.year
        region = chunk["code_lieu_deces"].map(code_to_region)
        sexe = chunk["sexe"].map(_SEXE).fillna("Inconnu")

        g = pd.DataFrame({"annee": annee, "region": region, "sexe": sexe}).dropna(subset=["annee"])
        for (a, r, s), nb in g.groupby(["annee", "region", "sexe"]).size().items():
            cumul[(int(a), r, s)] = cumul.get((int(a), r, s), 0) + int(nb)
        n_blocs += 1
        print(f"  [DECES] bloc {n_blocs} traité ({len(chunk)} lignes)")

    agg = pd.DataFrame(
        [(a, r, s, n) for (a, r, s), n in cumul.items()],
        columns=["annee", "region", "sexe", "nb_deces"],
    )

    loc = read_lookup(settings.dw_engine(),
                      'SELECT region, localisation_key FROM "DIM_LOCALISATION"')
    agg = agg.merge(loc, on="region", how="left")
    return agg[["annee", "localisation_key", "sexe", "nb_deces"]]


def run() -> None:
    print("[FAIT_DECES]")
    copy_dataframe(settings.dw_engine(), build(), '"FAIT_DECES"')


if __name__ == "__main__":
    run()
