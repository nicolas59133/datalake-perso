"""Ouvre l'UI web DuckDB (explorateur de schéma + éditeur SQL) sur le lake.

Se branche sur DUCKDB_VIEW_PATH (data/datalake_view.duckdb), une COPIE mise à
jour automatiquement à la fin de chaque run Dagster (asset
ops/duckdb_view_snapshot), jamais sur data/datalake.duckdb directement :
DuckDB n'autorise qu'un writer/lecteur à la fois par fichier, donc garder
cette UI ouverte en pointant sur le fichier du pipeline ferait échouer tout
run Dagster/dbt avec un lock conflict. Cette copie peut être laissée ouverte
indéfiniment sans jamais bloquer un run.

Lance-le avec :
    .venv/bin/python scripts/duckdb_ui.py
Puis ouvre http://localhost:4213 dans ton navigateur. Ctrl+C pour arrêter.
"""
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")

import duckdb

from data_platform.config import DUCKDB_PATH, DUCKDB_VIEW_PATH


def main():
    if not Path(DUCKDB_VIEW_PATH).exists():
        if not Path(DUCKDB_PATH).exists():
            print(f"ERREUR : {DUCKDB_PATH} n'existe pas encore. Lance d'abord un refresh :")
            print("    ./scripts/refresh.sh")
            sys.exit(1)
        print(f"Pas encore de copie de visualisation, création depuis {DUCKDB_PATH}...")
        shutil.copyfile(DUCKDB_PATH, DUCKDB_VIEW_PATH)

    con = duckdb.connect(DUCKDB_VIEW_PATH)
    con.sql("INSTALL ui; LOAD ui;")
    con.sql("CALL start_ui();")
    print("UI DuckDB démarrée sur http://localhost:4213 (Ctrl+C pour arrêter)")
    print(f"Données : {DUCKDB_VIEW_PATH} (snapshot du dernier run, pas le lake live)")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
