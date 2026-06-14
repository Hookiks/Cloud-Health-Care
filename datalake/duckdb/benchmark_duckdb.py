"""
Benchmark de performance — architecture DuckDB + Parquet partitionné (HDFS).

Mesure le temps d'exécution de requêtes représentatives des 8 besoins
utilisateurs, sur la base chu_gold.duckdb. Pas de comparaison avec
l'ancienne architecture : uniquement les performances factuelles de
la version actuelle.

Usage : python benchmark_duckdb.py
Sortie : affichage console + benchmark_results.csv
"""

import csv
import io
import sys
import time
from pathlib import Path

import duckdb

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = Path(__file__).resolve().parent
DB = str(HERE / "chu_gold.duckdb")
CSV_OUT = HERE / "benchmark_results.csv"

N_RUNS = 5  # nombre d'exécutions par requête (on garde la moyenne)

QUERIES = {
    "Q1 - Hospit. par region (jointure)": """
        SELECT e.region, COUNT(*) AS nb_hospit,
               ROUND(AVG(h.jours_hospitalisation), 1) AS duree_moy_j
        FROM GOLD_FAIT_HOSPITALISATION h
        JOIN GOLD_DIM_ETABLISSEMENT e ON h.finess = e.finess
        GROUP BY e.region ORDER BY nb_hospit DESC
    """,
    "Q2 - Consultations par annee (scan partitionne)": """
        SELECT annee, COUNT(*) AS nb_consultations,
               ROUND(AVG(duree_minutes), 1) AS duree_moy_min
        FROM GOLD_FAIT_CONSULTATION
        GROUP BY annee ORDER BY annee
    """,
    "Q3 - Consultations 1 annee (partition pruning)": """
        SELECT COUNT(*) FROM GOLD_FAIT_CONSULTATION WHERE annee = 2019
    """,
    "Q4 - Consultations par profession (jointure 1M lignes)": """
        SELECT p.profession, p.code_specialite, COUNT(*) AS nb_consultations,
               ROUND(AVG(c.duree_minutes), 1) AS duree_moy_min
        FROM GOLD_FAIT_CONSULTATION c
        JOIN GOLD_DIM_PROFESSIONNEL p ON c.identifiant = p.identifiant
        GROUP BY p.profession, p.code_specialite
        ORDER BY nb_consultations DESC
    """,
    "Q5 - Diagnostics les plus frequents": """
        SELECT dg.libelle_diagnostic, COUNT(*) AS nb
        FROM GOLD_FAIT_HOSPITALISATION h
        JOIN GOLD_DIM_DIAGNOSTIC dg ON h.code_diag = dg.code_diag
        GROUP BY dg.libelle_diagnostic ORDER BY nb DESC LIMIT 10
    """,
    "Q6 - Profil patients (groupby simple, 100k lignes)": """
        SELECT tranche_age, sexe, COUNT(*) AS nb
        FROM GOLD_DIM_PATIENT
        GROUP BY tranche_age, sexe ORDER BY tranche_age, sexe
    """,
    "Q7 - Satisfaction par region": """
        SELECT region, score_satisfaction, taux_recommandation
        FROM GOLD_FAIT_SATISFACTION ORDER BY score_satisfaction DESC
    """,
    "Q8 - Deces par region/annee/sexe (jointure)": """
        SELECT d.annee, d.region, d.sexe, d.nb_deces, l.zone
        FROM GOLD_FAIT_DECES d
        JOIN GOLD_DIM_LOCALISATION l ON d.region = l.region
        ORDER BY d.annee, d.region
    """,
}


def time_query(con, sql: str, n: int = N_RUNS):
    times = []
    n_rows = None
    for _ in range(n):
        t0 = time.perf_counter()
        res = con.execute(sql).fetchall()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1_000_000)  # µs
        n_rows = len(res)
    times.sort()
    return {
        "min_us": times[0],
        "moy_us": sum(times) / len(times),
        "max_us": times[-1],
        "n_rows": n_rows,
    }


def main():
    con = duckdb.connect(DB, read_only=True)

    print(f"Base : {DB}")
    print(f"{N_RUNS} exécutions par requête (min / moyenne / max)\n")

    results = []
    header = f"{'Requête':<48} {'min (µs)':>12} {'moy (µs)':>12} {'max (µs)':>12} {'lignes':>8}"
    print(header)
    print("-" * len(header))

    for name, sql in QUERIES.items():
        stats = time_query(con, sql)
        print(f"{name:<48} {stats['min_us']:>12.1f} {stats['moy_us']:>12.1f} "
              f"{stats['max_us']:>12.1f} {stats['n_rows']:>8}")
        results.append({"requete": name, **stats})

    con.close()

    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["requete", "min_us", "moy_us", "max_us", "n_rows"])
        writer.writeheader()
        writer.writerows(results)

    print(f"\nRésultats enregistrés -> {CSV_OUT}")


if __name__ == "__main__":
    main()
