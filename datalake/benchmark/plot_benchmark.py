"""Trace le graphe des temps de réponse Parquet vs PostgreSQL.

Lit le CSV produit par datalake/spark/benchmark_gold.py et génère un PNG.
Exécution (hôte) : python datalake/benchmark/plot_benchmark.py
Dépendances hôte : pandas, matplotlib.
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).resolve().parent
CSV = HERE.parent / "spark" / "benchmark_results.csv"
OUT = HERE / "temps_reponse_medaillon.png"


def main() -> None:
    df = pd.read_csv(CSV)
    x = range(len(df))
    plt.figure(figsize=(9, 5))
    plt.bar([i - 0.2 for i in x], df["parquet_ms"], width=0.4, label="Parquet (HDFS/Spark)")
    plt.bar([i + 0.2 for i in x], df["postgres_ms"], width=0.4, label="PostgreSQL (JDBC)")
    plt.xticks(list(x), df["requete"], rotation=15)
    plt.ylabel("Temps moyen (ms)")
    plt.title("GOLD — Temps de réponse : Parquet vs PostgreSQL")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT, dpi=120)
    print(f"Graphe enregistré : {OUT}")


if __name__ == "__main__":
    main()
