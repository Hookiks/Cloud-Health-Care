"""Configuration centrale du projet CHU Data Warehouse.

Charge les variables d'environnement (.env) et expose :
- les chemins vers les fichiers sources,
- les fabriques d'engine SQLAlchemy (entrepôt cible + staging),
- les réglages ETL (chunk size, plage DIM_TEMPS).
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL

# Racine du projet (ce fichier est dans <racine>/config/)
ROOT = Path(__file__).resolve().parents[1]

load_dotenv(ROOT / ".env")

# --------------------------------------------------------------------------- #
# Chemins des sources
# --------------------------------------------------------------------------- #
SOURCES = ROOT / "sources"
SQL_DIR = ROOT / "sql"
BENCH_DIR = ROOT / "benchmarks"

PATH_DUMP = SOURCES / "BDD PostgreSQL" / "DATA2023"
PATH_HOSPITALISATIONS = SOURCES / "Hospitalisation" / "Hospitalisations.csv"
PATH_ETABLISSEMENTS = SOURCES / "Etablissement de SANTE" / "etablissement_sante.csv"
PATH_ACTIVITE_PRO = SOURCES / "Etablissement de SANTE" / "activite_professionnel_sante.csv"
PATH_DECES = SOURCES / "DECES EN FRANCE" / "deces.csv"
PATH_SATISFACTION_2020 = (
    SOURCES / "Satisfaction" / "2020" / "resultats-esatis48h-mco-open-data-2020.xlsx"
)


# --------------------------------------------------------------------------- #
# Réglages ETL
# --------------------------------------------------------------------------- #
DECES_CHUNKSIZE = int(os.getenv("DECES_CHUNKSIZE", "500000"))
DIM_TEMPS_START = os.getenv("DIM_TEMPS_START", "2000-01-01")
DIM_TEMPS_END = os.getenv("DIM_TEMPS_END", "2025-12-31")


# --------------------------------------------------------------------------- #
# Connexions base de données
# --------------------------------------------------------------------------- #
def _url(prefix: str) -> URL:
    # URL.create encode proprement les caractères spéciaux (espaces dans le nom
    # de base « Cloud Healthcare Unit », caractères du mot de passe, etc.).
    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv(f"{prefix}_USER", "postgres"),
        password=os.getenv(f"{prefix}_PASSWORD", "postgres"),
        host=os.getenv(f"{prefix}_HOST", "localhost"),
        port=int(os.getenv(f"{prefix}_PORT", "5432")),
        database=os.getenv(f"{prefix}_NAME"),
    )


def dw_engine() -> Engine:
    """Engine vers l'entrepôt de données cible (chu_dw)."""
    return create_engine(_url("DW"), future=True)


def source_engine() -> Engine:
    """Engine vers la base de staging restaurée depuis le dump (chu_source)."""
    return create_engine(_url("SRC"), future=True)


def pg_bin(tool: str) -> str:
    """Chemin complet d'un binaire PostgreSQL (pg_restore, psql...)."""
    pg = os.getenv("PG_BIN", "").strip()
    return str(Path(pg) / tool) if pg else tool
