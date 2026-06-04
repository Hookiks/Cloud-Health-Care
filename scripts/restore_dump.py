"""Restaure le dump opérationnel DATA2023 (format custom PostgreSQL) dans
la base de staging chu_source, lisible ensuite par l'ETL.

Prérequis : pg_restore / createdb dans le PATH (ou variable PG_BIN du .env)
et un serveur PostgreSQL local accessible avec les identifiants SRC_*.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chu_config import settings


def _run(cmd: list[str], env: dict) -> int:
    print("  $", " ".join(cmd))
    return subprocess.call(cmd, env=env)


def main() -> None:
    db = os.getenv("SRC_NAME", "chu_source")
    host = os.getenv("SRC_HOST", "localhost")
    port = os.getenv("SRC_PORT", "5432")
    user = os.getenv("SRC_USER", "postgres")
    pwd = os.getenv("SRC_PASSWORD", "postgres")

    env = {**os.environ, "PGPASSWORD": pwd}

    print(f"=== Création de la base de staging '{db}' (si absente) ===")
    _run([settings.pg_bin("createdb"), "-h", host, "-p", port, "-U", user, db], env)

    print(f"=== Restauration de {settings.PATH_DUMP.name} -> {db} ===")
    code = _run([
        settings.pg_bin("pg_restore"),
        "-h", host, "-p", port, "-U", user,
        "-d", db,
        "--no-owner", "--no-privileges", "--clean", "--if-exists",
        str(settings.PATH_DUMP),
    ], env)

    # pg_restore renvoie souvent un code != 0 sur des warnings bénins
    print(f"\npg_restore terminé (code {code}).")
    sys.exit(0)


if __name__ == "__main__":
    main()
