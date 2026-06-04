"""DIM_LOCALISATION — dimension géographique au grain RÉGION.

Alimentée depuis le référentiel département->région (ETL/common/geo.py),
complétée d'une modalité 'Inconnu'. Sert de point de jointure aux faits
décès et satisfaction (analyses « par région »).
"""
from __future__ import annotations

import pandas as pd

from chu_config import settings
from ETL.common.db import load_dataframe
from ETL.common.geo import DEPT_REGION, INCONNU


def build() -> pd.DataFrame:
    regions = sorted(set(DEPT_REGION.values()) | {INCONNU})
    return pd.DataFrame({"region": regions})


def run() -> None:
    print("[DIM_LOCALISATION]")
    load_dataframe(settings.dw_engine(), build(), "DIM_LOCALISATION")


if __name__ == "__main__":
    run()
