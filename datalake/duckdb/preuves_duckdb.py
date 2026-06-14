"""
Démonstration / preuves : stockage Parquet partitionné + DuckDB.

À lancer en soutenance :  python preuves_duckdb.py

Produit 5 preuves :
  1. Couche stockage HDFS = Parquet partitionné par annee (format Hive).
  2. Les 10 tables GOLD sont chargées dans DuckDB (DWH colonnaire OLAP).
  3. La colonne de partition "annee" est conservée et interrogeable.
  4. Partition pruning natif DuckDB : 1 fichier lu sur 9 quand on filtre une année.
  5. La chaîne fonctionne : une requête métier (jointure) renvoie un résultat.
"""

import io
import subprocess
import sys
from pathlib import Path

import duckdb

# Sortie UTF-8 (console Windows)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = Path(__file__).resolve().parent
DB = str(HERE / "chu_gold.duckdb")
EXPORT_FAIT = HERE / "gold_export" / "FAIT_CONSULTATION"

CONTAINER = "namenode"
HDFS_FAIT = "/datalake/gold/FAIT_CONSULTATION"


def titre(n, txt):
    print(f"\n{'='*72}\n  PREUVE {n} — {txt}\n{'='*72}")


def hdfs(path):
    out = subprocess.run(
        ["docker", "exec", CONTAINER, "hdfs", "dfs", "-ls", path],
        capture_output=True, text=True, encoding="utf-8")
    return out.stdout


def main():
    con = duckdb.connect(DB)

    # ---- 1. Stockage Parquet partitionné sur HDFS --------------------
    titre(1, "Couche stockage HDFS = Parquet partitionné par annee (format Hive)")
    print("  $ hdfs dfs -ls /datalake/gold/FAIT_CONSULTATION")
    folders = [l.split()[-1].split("/")[-1]
               for l in hdfs(HDFS_FAIT).splitlines() if "annee=" in l]
    print(f"  Partitions : {folders}")
    print("\n  $ hdfs dfs -ls /datalake/gold/FAIT_CONSULTATION/annee=2019")
    for l in hdfs(f"{HDFS_FAIT}/annee=2019").splitlines():
        if ".parquet" in l:
            parts = l.split()
            print(f"    fichier Parquet : {parts[-1].split('/')[-1]}  ({parts[4]} octets)")

    # ---- 2. Tables dans DuckDB ---------------------------------------
    titre(2, "Les tables GOLD vivent dans DuckDB (DWH colonnaire OLAP)")
    rows = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name LIKE 'GOLD_%' ORDER BY table_name
    """).fetchall()
    for (name,) in rows:
        n = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        print(f"  {name:<28} {n:>9} lignes")

    # ---- 3. Colonne de partition conservée ---------------------------
    titre(3, "Partitionnement par 'annee' — colonne conservée et interrogeable")
    df = con.execute("""
        SELECT annee, COUNT(*) AS nb_consultations
        FROM GOLD_FAIT_CONSULTATION GROUP BY annee ORDER BY annee
    """).fetchall()
    for annee, nb in df:
        print(f"  annee={annee}  ->  {nb:>8} consultations")

    # ---- 4. Partition pruning natif DuckDB ---------------------------
    titre(4, "Partition pruning natif DuckDB — seule la partition utile est lue")
    pattern = (EXPORT_FAIT / "**" / "*.parquet").as_posix()
    plan = con.execute(f"""
        EXPLAIN ANALYZE
        SELECT COUNT(*) FROM read_parquet('{pattern}', hive_partitioning=true)
        WHERE annee = 2019
    """).fetchall()[0][1]
    for line in plan.splitlines():
        s = line.strip("│ ")
        if "Scanning Files" in s or "Total Files Read" in s or "File Filters" in s:
            print(f"  >>> {s}")
    print("  (DuckDB saute les autres partitions grâce au filtre annee = 2019)")

    # ---- 5. La chaîne fonctionne (requête métier) --------------------
    titre(5, "La chaîne fonctionne : hospitalisations par région (jointure)")
    res = con.execute("""
        SELECT e.region, COUNT(*) AS nb_hospit,
               ROUND(AVG(h.jours_hospitalisation), 1) AS duree_moy_j
        FROM GOLD_FAIT_HOSPITALISATION h
        JOIN GOLD_DIM_ETABLISSEMENT e ON h.finess = e.finess
        GROUP BY e.region ORDER BY nb_hospit DESC LIMIT 5
    """).fetchall()
    for region, nb, duree in res:
        print(f"  {region:<28} {nb:>5} hospit.  durée moy {duree} j")

    con.close()
    print("\nToutes les preuves ont été produites.\n")


if __name__ == "__main__":
    main()
