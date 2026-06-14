"""
Charge les tables GOLD (Parquet sur HDFS) dans un fichier DuckDB unique.

Le Parquet est lu nativement par DuckDB (read_parquet), avec hive_partitioning
pour restituer la colonne de partition "annee" des tables de faits.

Étapes :
  1. Exporte chaque dossier Parquet GOLD de HDFS vers le conteneur namenode
     (hdfs dfs -copyToLocal), puis vers le système de fichiers local
     (docker cp).
  2. Charge chaque dossier Parquet dans une table DuckDB du fichier
     chu_gold.duckdb (CREATE OR REPLACE TABLE ... AS SELECT * FROM read_parquet).

Prérequis : Docker actif (conteneur "namenode"), paquet Python "duckdb" installé.

Usage : python load_to_duckdb.py
"""

import shutil
import subprocess
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
EXPORT_DIR = HERE / "gold_export"
DB_PATH = HERE / "chu_gold.duckdb"

CONTAINER = "namenode"
HDFS_GOLD = "/datalake/gold"
CONTAINER_TMP = "/tmp/gold_export"

TABLES = [
    "DIM_PATIENT",
    "DIM_DIAGNOSTIC",
    "DIM_ETABLISSEMENT",
    "DIM_PROFESSIONNEL",
    "DIM_TEMPS",
    "DIM_LOCALISATION",
    "FAIT_HOSPITALISATION",
    "FAIT_CONSULTATION",
    "FAIT_SATISFACTION",
    "FAIT_DECES",
]

# Tables écrites par Spark avec partitionBy("annee") -> dossiers annee=YYYY/
PARTITIONED_TABLES = {"FAIT_HOSPITALISATION", "FAIT_CONSULTATION", "FAIT_DECES"}


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def export_table(name: str) -> Path:
    """Copie le dossier Parquet HDFS GOLD/<name> -> EXPORT_DIR/<name> (local)."""
    container_path = f"{CONTAINER_TMP}/{name}"
    local_path = EXPORT_DIR / name

    if local_path.exists():
        shutil.rmtree(local_path)

    run(["docker", "exec", CONTAINER, "rm", "-rf", container_path])
    run(["docker", "exec", CONTAINER, "mkdir", "-p", CONTAINER_TMP])
    run(["docker", "exec", CONTAINER, "hdfs", "dfs", "-copyToLocal",
         f"{HDFS_GOLD}/{name}", container_path])
    run(["docker", "cp", f"{CONTAINER}:{container_path}", str(local_path)])

    return local_path


def load_table(con: duckdb.DuckDBPyConnection, name: str, local_path: Path) -> None:
    if name in PARTITIONED_TABLES:
        # Parquet partitionné Hive (annee=YYYY/) -> hive_partitioning restitue "annee"
        pattern = str(local_path / "**" / "*.parquet").replace("\\", "/")
        source = f"read_parquet('{pattern}', hive_partitioning=true)"
    else:
        pattern = str(local_path / "*.parquet").replace("\\", "/")
        source = f"read_parquet('{pattern}')"
    con.execute(f'CREATE OR REPLACE TABLE "GOLD_{name}" AS SELECT * FROM {source}')
    n = con.execute(f'SELECT COUNT(*) FROM "GOLD_{name}"').fetchone()[0]
    print(f"  [DUCKDB] GOLD_{name:<22} {n:>9} lignes")


def main() -> None:
    EXPORT_DIR.mkdir(exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    for table in TABLES:
        print(f"Export {table} ...")
        local_path = export_table(table)
        load_table(con, table, local_path)

    con.close()
    print(f"\nTerminé -> {DB_PATH}")


if __name__ == "__main__":
    main()
