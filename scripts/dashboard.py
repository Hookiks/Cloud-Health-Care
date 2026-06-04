"""Génère des graphiques de restitution (PNG) pour les 8 besoins utilisateurs.

Support immédiat pour la soutenance / le rapport, en complément du tableau de
bord Power BI. Sorties dans dashboards/.
"""
from __future__ import annotations

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chu_config import settings

OUT = settings.ROOT / "dashboards"


def _read(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, settings.dw_engine())


def main() -> None:
    OUT.mkdir(exist_ok=True)
    plt.rcParams.update({"figure.autolayout": True, "axes.titlesize": 11})

    # 1) Consultations par établissement (Top 10, 2019)
    d = _read('SELECT raison_sociale, SUM(nb_consultations) nb FROM "V_CONSULT_ETABLISSEMENT"'
              " WHERE annee=2019 GROUP BY raison_sociale ORDER BY nb DESC LIMIT 10")
    _barh(d, "raison_sociale", "nb", "1. Consultations par établissement (Top 10, 2019)", "01_consult_etablissement.png")

    # 2) Consultations par diagnostic (Top 10)
    d = _read('SELECT libelle_diagnostic, SUM(nb_consultations) nb FROM "V_CONSULT_DIAGNOSTIC"'
              " GROUP BY libelle_diagnostic ORDER BY nb DESC LIMIT 10")
    _barh(d, "libelle_diagnostic", "nb", "2. Consultations par diagnostic (Top 10)", "02_consult_diagnostic.png")

    # 3) Hospitalisations par année
    d = _read('SELECT annee, SUM(nb_hospitalisations) nb FROM "V_HOSPIT_PERIODE" GROUP BY annee ORDER BY annee')
    _line(d, "annee", "nb", "3. Hospitalisations par année", "03_hospit_periode.png")

    # 4) Hospitalisations par diagnostic (Top 10)
    d = _read('SELECT libelle_diagnostic, SUM(nb_hospitalisations) nb FROM "V_HOSPIT_DIAGNOSTIC"'
              " GROUP BY libelle_diagnostic ORDER BY nb DESC LIMIT 10")
    _barh(d, "libelle_diagnostic", "nb", "4. Hospitalisations par diagnostic (Top 10)", "04_hospit_diagnostic.png")

    # 5) Hospitalisations par sexe et tranche d'âge
    d = _read('SELECT sexe, tranche_age, nb_hospitalisations FROM "V_HOSPIT_SEXE_AGE"'
              " WHERE tranche_age IS NOT NULL ORDER BY tranche_age")
    piv = d.pivot_table(index="tranche_age", columns="sexe", values="nb_hospitalisations", aggfunc="sum")
    piv.plot(kind="bar", figsize=(8, 5))
    plt.title("5. Hospitalisations par sexe et tranche d'âge")
    plt.ylabel("Nombre"); plt.xlabel("Tranche d'âge"); plt.xticks(rotation=0)
    plt.savefig(OUT / "05_hospit_sexe_age.png", dpi=120); plt.close()

    # 6) Consultations par professionnel (Top 10)
    d = _read('SELECT identifiant, nb_consultations nb FROM "V_CONSULT_PROFESSIONNEL"'
              " ORDER BY nb DESC LIMIT 10")
    _barh(d, "identifiant", "nb", "6. Consultations par professionnel (Top 10)", "06_consult_professionnel.png")

    # 7) Décès par région en 2019
    d = _read('SELECT region, nb_deces nb FROM "V_DECES_REGION" WHERE annee=2019 ORDER BY nb DESC LIMIT 15')
    _barh(d, "region", "nb", "7. Décès par région (2019)", "07_deces_region_2019.png")

    # 8) Satisfaction par région en 2020
    d = _read('SELECT region, score_satisfaction s FROM "V_SATISFACTION_REGION"'
              " WHERE annee=2020 AND score_satisfaction IS NOT NULL ORDER BY s DESC")
    _barh(d, "region", "s", "8. Score de satisfaction par région (2020)", "08_satisfaction_region_2020.png")

    print(f"\n8 graphiques générés dans : {OUT}")


def _barh(d, col, val, title, fname):
    d = d.iloc[::-1]
    plt.figure(figsize=(9, 5))
    plt.barh(d[col].astype(str).str.slice(0, 40), d[val])
    plt.title(title); plt.xlabel("Nombre")
    plt.savefig(OUT / fname, dpi=120); plt.close()
    print(f"  [PNG] {fname}")


def _line(d, col, val, title, fname):
    plt.figure(figsize=(9, 5))
    plt.plot(d[col], d[val], marker="o")
    plt.title(title); plt.xlabel(col); plt.ylabel("Nombre"); plt.grid(True, alpha=0.3)
    plt.savefig(OUT / fname, dpi=120); plt.close()
    print(f"  [PNG] {fname}")


if __name__ == "__main__":
    main()
