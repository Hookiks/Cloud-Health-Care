"""Filtrage RGPD : suppression des colonnes PII avant chargement.

`filter_pii` est appelé par CHAQUE job de dimension/fait qui manipule des
données issues d'une source contenant des identifiants personnels. La liste
des colonnes sensibles vit dans config/pii_fields.py (source unique de vérité).
"""
from __future__ import annotations

import pandas as pd

from chu_config.pii_fields import PII_FIELDS


def filter_pii(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Retourne une copie du DataFrame sans les colonnes PII de `source`.

    La correspondance des noms de colonnes est insensible à la casse.
    Les colonnes PII absentes sont ignorées silencieusement.
    """
    sensibles = {c.lower() for c in PII_FIELDS.get(source, [])}
    a_retirer = [c for c in df.columns if c.lower() in sensibles]
    if a_retirer:
        print(f"  [RGPD] source '{source}' : colonnes PII retirées -> {a_retirer}")
    return df.drop(columns=a_retirer)
