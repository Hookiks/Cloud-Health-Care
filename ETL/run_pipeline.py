"""Orchestrateur ETL du CHU Data Warehouse.

Ordre : (DDL) -> dimensions -> faits. Les dimensions doivent être chargées
avant les faits (résolution des clés de substitution).

Exemples :
    python -m ETL.run_pipeline --ddl            # recrée le schéma puis charge tout
    python -m ETL.run_pipeline --skip-deces     # saute le registre des décès (2 Go)
    python -m ETL.run_pipeline --partition       # construit aussi les tables partitionnées
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Permet l'exécution directe (python ETL/run_pipeline.py) en plus de `-m` :
# on garantit que la racine du projet est en tête de sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chu_config import settings
from ETL.common.db import run_sql_file, table_count
from ETL.dimensions import (dim_diagnostic, dim_etablissement, dim_localisation,
                            dim_mutuelle, dim_patient, dim_professionnel, dim_temps)
from ETL.facts import (fait_consultation, fait_deces, fait_hospitalisation,
                       fait_satisfaction)

DIMENSIONS = [
    ("DIM_TEMPS", dim_temps),
    ("DIM_LOCALISATION", dim_localisation),
    ("DIM_PATIENT", dim_patient),
    ("DIM_DIAGNOSTIC", dim_diagnostic),
    ("DIM_PROFESSIONNEL", dim_professionnel),
    ("DIM_MUTUELLE", dim_mutuelle),
    ("DIM_ETABLISSEMENT", dim_etablissement),
]
FAITS = [
    ("FAIT_CONSULTATION", fait_consultation),
    ("FAIT_HOSPITALISATION", fait_hospitalisation),
    ("FAIT_DECES", fait_deces),
    ("FAIT_SATISFACTION", fait_satisfaction),
]


def main() -> None:
    ap = argparse.ArgumentParser(description="ETL CHU Data Warehouse")
    ap.add_argument("--ddl", action="store_true", help="(re)crée dimensions, faits, index")
    ap.add_argument("--partition", action="store_true", help="construit les tables partitionnées (L2)")
    ap.add_argument("--skip-deces", action="store_true", help="ignore le registre des décès (2 Go)")
    args = ap.parse_args()

    dw = settings.dw_engine()
    t0 = time.perf_counter()

    if args.ddl:
        print("=== Création du schéma (DDL) ===")
        run_sql_file(dw, settings.SQL_DIR / "01_create_dimensions.sql")
        run_sql_file(dw, settings.SQL_DIR / "02_create_facts.sql")
        run_sql_file(dw, settings.SQL_DIR / "04_indexes.sql")

    print("=== Dimensions ===")
    for _, mod in DIMENSIONS:
        mod.run()

    print("=== Faits ===")
    for name, mod in FAITS:
        if args.skip_deces and name == "FAIT_DECES":
            print("[FAIT_DECES] ignoré (--skip-deces)")
            continue
        mod.run()

    if args.partition:
        print("=== Partitionnement (Livrable 2) ===")
        run_sql_file(dw, settings.SQL_DIR / "03_partitioning.sql")

    print("\n=== Contrôle des volumes ===")
    for name, _ in DIMENSIONS + FAITS:
        try:
            print(f"  {name:<24} {table_count(dw, f'\"{name}\"'):>10} lignes")
        except Exception as exc:  # table absente / non chargée
            print(f"  {name:<24} (indisponible : {exc})")

    print(f"\nPipeline terminé en {time.perf_counter() - t0:.1f} s.")


if __name__ == "__main__":
    main()
