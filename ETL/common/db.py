"""Briques d'accès base de données pour l'ETL.

Fournit :
- run_sql_file  : exécution d'un script .sql (DDL),
- load_dataframe : chargement d'un DataFrame (insert classique),
- copy_dataframe : chargement performant via COPY (gros volumes),
- table_count    : comptage de contrôle.
"""
from __future__ import annotations

import csv
import io
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def run_sql_file(engine: Engine, path: str | Path) -> None:
    """Exécute un fichier SQL complet (plusieurs instructions séparées par ';')."""
    sql = Path(path).read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
    print(f"  [SQL] exécuté : {Path(path).name}")


def _copy(engine: Engine, df: pd.DataFrame, qualified_table: str) -> int:
    """Chargement performant via COPY FROM STDIN (psycopg2 copy_expert).

    `qualified_table` doit être un identifiant prêt à l'emploi (déjà entre
    guillemets si nécessaire). Les colonnes du DataFrame doivent correspondre
    aux colonnes cibles ; les colonnes SERIAL absentes sont auto-générées.
    """
    if df.empty:
        print(f"  [COPY]        0 ligne  -> {qualified_table} (vide, ignoré)")
        return 0

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N", quoting=csv.QUOTE_MINIMAL)
    buf.seek(0)

    cols = ", ".join(f'"{c}"' for c in df.columns)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY {qualified_table} ({cols}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')",
                buf,
            )
        raw.commit()
    finally:
        raw.close()
    print(f"  [COPY] {len(df):>8} lignes -> {qualified_table}")
    return len(df)


def load_dataframe(engine: Engine, df: pd.DataFrame, table: str) -> int:
    """Charge un DataFrame dans une table (par nom non quoté) via COPY."""
    return _copy(engine, df, f'"{table}"')


def copy_dataframe(engine: Engine, df: pd.DataFrame, table: str) -> int:
    """Charge un DataFrame via COPY (le nom de table peut être déjà quoté)."""
    return _copy(engine, df, table)


def table_count(engine: Engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def read_lookup(engine: Engine, sql: str) -> pd.DataFrame:
    """Lit une table de correspondance (clé naturelle -> clé de substitution)."""
    return pd.read_sql(text(sql), engine)
