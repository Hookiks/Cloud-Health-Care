"""Extraction des sources hétérogènes (staging PostgreSQL + CSV/XLSX).

Centralise la lecture tolérante aux encodages/séparateurs mixtes et la
lecture chunkée du registre des décès (~2 Go).
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from chu_config import settings


def read_table(engine: Engine, table: str, schema: str = "public") -> pd.DataFrame:
    """Lit intégralement une table du staging opérationnel."""
    return pd.read_sql(text(f'SELECT * FROM {schema}."{table}"'), engine)


def read_csv_smart(
    path: str | Path,
    sep: str = ";",
    encodings: tuple[str, ...] = ("utf-8", "latin-1", "cp1252"),
    **kwargs,
) -> pd.DataFrame:
    """Lecture CSV tolérante : essaie plusieurs encodages jusqu'au premier succès."""
    last_err: Exception | None = None
    for enc in encodings:
        try:
            return pd.read_csv(path, sep=sep, encoding=enc, dtype=str, **kwargs)
        except (UnicodeDecodeError, UnicodeError) as exc:
            last_err = exc
    raise last_err  # type: ignore[misc]


def read_hospitalisations() -> pd.DataFrame:
    return read_csv_smart(settings.PATH_HOSPITALISATIONS, sep=";")


def read_etablissements() -> pd.DataFrame:
    return read_csv_smart(settings.PATH_ETABLISSEMENTS, sep=";")


def read_satisfaction_2020() -> pd.DataFrame:
    """Résultats e-Satis 48h MCO 2020 (XLSX, contient déjà la colonne `region`)."""
    return pd.read_excel(settings.PATH_SATISFACTION_2020, dtype={"finess": str, "finess_geo": str})


def read_deces_chunks(chunksize: int | None = None) -> Iterator[pd.DataFrame]:
    """Itère le registre des décès par blocs (séparateur ',', encodage UTF-8/latin-1)."""
    size = chunksize or settings.DECES_CHUNKSIZE
    for enc in ("utf-8", "latin-1"):
        try:
            yield from pd.read_csv(
                settings.PATH_DECES, sep=",", encoding=enc, dtype=str, chunksize=size
            )
            return
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise UnicodeDecodeError("deces", b"", 0, 1, "encodage non reconnu")
