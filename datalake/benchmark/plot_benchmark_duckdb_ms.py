"""
Tableau des performances DuckDB (lecture de benchmark_results.csv), en ms.

Usage : python plot_benchmark_duckdb_ms.py
Sortie : performances_duckdb_ms.png
"""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

HERE = Path(__file__).resolve().parent
CSV = HERE / "benchmark_results.csv"
OUT = HERE / "performances_duckdb_ms.png"


def main() -> None:
    df = pd.read_csv(CSV)

    col_labels = ["Requête", "Min (ms)", "Moyenne (ms)", "Max (ms)", "Lignes"]
    cell_text = []
    for _, row in df.iterrows():
        cell_text.append([
            row["requete"],
            f"{row['min_us'] / 1000:,.3f}",
            f"{row['moy_us'] / 1000:,.3f}",
            f"{row['max_us'] / 1000:,.3f}",
            f"{int(row['n_rows'])}",
        ])

    fig, ax = plt.subplots(figsize=(11, 0.5 * len(df) + 1.5))
    ax.axis("off")

    table = ax.table(
        cellText=cell_text,
        colLabels=col_labels,
        cellLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.auto_set_column_width(col=list(range(len(col_labels))))

    # Style en-tête
    for j in range(len(col_labels)):
        cell = table[0, j]
        cell.set_facecolor("#2c3e50")
        cell.set_text_props(color="white", weight="bold")

    # Alignement à gauche pour la colonne "Requête"
    for i in range(1, len(df) + 1):
        table[i, 0].set_text_props(ha="left")
        table[i, 0].PAD = 0.02
        if i % 2 == 0:
            for j in range(len(col_labels)):
                table[i, j].set_facecolor("#f2f2f2")

    table.scale(1, 1.6)

    plt.title("Performances DuckDB Parquet partitionné (8 requêtes, moyenne sur 5 exécutions)",
              fontsize=11, fontweight="bold", pad=20)
    plt.tight_layout()
    plt.savefig(OUT, dpi=150, bbox_inches="tight")
    print(f"Tableau enregistré : {OUT}")


if __name__ == "__main__":
    main()
