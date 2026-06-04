"""Mesure des temps de réponse (Livrable 2).

Compare l'exécution de requêtes représentatives sur les tables de faits
NON partitionnées vs PARTITIONNÉES (par année), puis génère un graphe.

Prérequis : avoir lancé `python -m ETL.run_pipeline --partition`.
"""
from __future__ import annotations

import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import text

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chu_config import settings

# (label, requête table de base, requête table partitionnée)
CAS = [
    (
        "Consultations 2019",
        'SELECT count(*) FROM "FAIT_CONSULTATION" WHERE annee = 2019',
        'SELECT count(*) FROM "FAIT_CONSULTATION_PART" WHERE annee = 2019',
    ),
    (
        "Consult. 2018-2020",
        'SELECT count(*) FROM "FAIT_CONSULTATION" WHERE annee BETWEEN 2018 AND 2020',
        'SELECT count(*) FROM "FAIT_CONSULTATION_PART" WHERE annee BETWEEN 2018 AND 2020',
    ),
    (
        "Hospit. 2020",
        'SELECT count(*) FROM "FAIT_HOSPITALISATION" WHERE annee = 2020',
        'SELECT count(*) FROM "FAIT_HOSPITALISATION_PART" WHERE annee = 2020',
    ),
]

REPETITIONS = 5


def _temps_moyen(engine, sql: str) -> float:
    with engine.connect() as conn:
        conn.execute(text(sql))  # warm-up
        debut = time.perf_counter()
        for _ in range(REPETITIONS):
            conn.execute(text(sql))
        return (time.perf_counter() - debut) / REPETITIONS * 1000  # ms


def main() -> None:
    engine = settings.dw_engine()
    labels, base_ms, part_ms = [], [], []

    print(f"{'Cas':<22}{'Base (ms)':>12}{'Partition (ms)':>16}")
    for label, sql_base, sql_part in CAS:
        tb = _temps_moyen(engine, sql_base)
        tp = _temps_moyen(engine, sql_part)
        labels.append(label)
        base_ms.append(tb)
        part_ms.append(tp)
        print(f"{label:<22}{tb:>12.2f}{tp:>16.2f}")

    x = range(len(labels))
    plt.figure(figsize=(9, 5))
    plt.bar([i - 0.2 for i in x], base_ms, width=0.4, label="Non partitionné")
    plt.bar([i + 0.2 for i in x], part_ms, width=0.4, label="Partitionné (année)")
    plt.xticks(list(x), labels, rotation=15)
    plt.ylabel("Temps moyen (ms)")
    plt.title(f"Temps de réponse — moyenne sur {REPETITIONS} exécutions")
    plt.legend()
    plt.tight_layout()

    settings.BENCH_DIR.mkdir(exist_ok=True)
    out = settings.BENCH_DIR / "temps_reponse.png"
    plt.savefig(out, dpi=120)
    print(f"\nGraphe enregistré : {out}")


if __name__ == "__main__":
    main()
