"""Référentiel géographique : département -> région (découpage 2016).

Permet de construire DIM_LOCALISATION et de rattacher décès / satisfaction /
établissements à une région à partir d'un code postal ou d'un code commune INSEE.
"""
from __future__ import annotations

import pandas as pd

# Code département -> nom de région (métropole + DOM)
DEPT_REGION: dict[str, str] = {}


def _add(region: str, depts: list[str]) -> None:
    for d in depts:
        DEPT_REGION[d] = region


_add("Auvergne-Rhône-Alpes", ["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"])
_add("Bourgogne-Franche-Comté", ["21", "25", "39", "58", "70", "71", "89", "90"])
_add("Bretagne", ["22", "29", "35", "56"])
_add("Centre-Val de Loire", ["18", "28", "36", "37", "41", "45"])
_add("Corse", ["2A", "2B", "20"])
_add("Grand Est", ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"])
_add("Hauts-de-France", ["02", "59", "60", "62", "80"])
_add("Île-de-France", ["75", "77", "78", "91", "92", "93", "94", "95"])
_add("Normandie", ["14", "27", "50", "61", "76"])
_add("Nouvelle-Aquitaine", ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"])
_add("Occitanie", ["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"])
_add("Pays de la Loire", ["44", "49", "53", "72", "85"])
_add("Provence-Alpes-Côte d'Azur", ["04", "05", "06", "13", "83", "84"])
_add("Guadeloupe", ["971"])
_add("Martinique", ["972"])
_add("Guyane", ["973"])
_add("La Réunion", ["974"])
_add("Mayotte", ["976"])

INCONNU = "Inconnu"


def code_to_departement(code: str | float | None) -> str | None:
    """Extrait le code département d'un code postal ou code commune INSEE.

    Gère la Corse (2A/2B) et les DOM (3 premiers chiffres 97x).
    Retourne None si le code est invalide/manquant.
    """
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return None
    s = str(code).strip().upper().replace(" ", "")
    if len(s) < 2:
        return None
    # Corse : codes commune 2A.../2B...
    if s[:2] in ("2A", "2B"):
        return s[:2]
    if not s[:2].isdigit():
        return None
    # DOM : 971..976
    if s[:2] == "97" and len(s) >= 3 and s[:3].isdigit():
        return s[:3]
    return s[:2]


def departement_to_region(dept: str | None) -> str:
    if dept is None:
        return INCONNU
    return DEPT_REGION.get(dept, INCONNU)


def code_to_region(code: str | float | None) -> str:
    return departement_to_region(code_to_departement(code))


def reference_table() -> pd.DataFrame:
    """Table de référence département -> région (utile pour DIM_LOCALISATION)."""
    rows = [{"code_departement": d, "region": r} for d, r in sorted(DEPT_REGION.items())]
    return pd.DataFrame(rows)
