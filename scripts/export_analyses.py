"""Exporte les 8 vues d'analyse vers un classeur Excel (un onglet par besoin).

Sert de support direct pour la restitution (Power BI peut importer ce .xlsx,
ou l'on peut y construire des graphiques sans connexion à PostgreSQL).
"""
from __future__ import annotations

import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chu_config import settings

# (onglet, vue, tri par défaut)
EXPORTS = [
    ("1_Consult_etablissement", '"V_CONSULT_ETABLISSEMENT"', "nb_consultations"),
    ("2_Consult_diagnostic", '"V_CONSULT_DIAGNOSTIC"', "nb_consultations"),
    ("3_Hospit_periode", '"V_HOSPIT_PERIODE"', None),
    ("4_Hospit_diagnostic", '"V_HOSPIT_DIAGNOSTIC"', "nb_hospitalisations"),
    ("5_Hospit_sexe_age", '"V_HOSPIT_SEXE_AGE"', None),
    ("6_Consult_professionnel", '"V_CONSULT_PROFESSIONNEL"', "nb_consultations"),
    ("7_Deces_region", '"V_DECES_REGION"', "nb_deces"),
    ("8_Satisfaction_region", '"V_SATISFACTION_REGION"', "score_satisfaction"),
]


def main() -> None:
    engine = settings.dw_engine()
    settings.BENCH_DIR.mkdir(exist_ok=True)
    out = settings.BENCH_DIR / "analyses_chu.xlsx"

    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for sheet, view, order in EXPORTS:
            df = pd.read_sql(f"SELECT * FROM {view}", engine)
            if order and order in df.columns:
                df = df.sort_values(order, ascending=False)
            df.to_excel(writer, sheet_name=sheet, index=False)
            print(f"  [XLSX] {sheet:<26} {len(df):>7} lignes")

    print(f"\nClasseur généré : {out}")


if __name__ == "__main__":
    main()
