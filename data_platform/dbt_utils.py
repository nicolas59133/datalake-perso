"""Appel de `dbt build` en subprocess, partagé par assets/silver.py et
assets/gold.py.

dagster-dbt (l'intégration officielle, qui génère un asset par modèle à
partir du manifest dbt) n'est pas installable ici : sa dernière version
épingle dagster==1.12.8, qui ne supporte pas Python 3.14 (le seul Python
présent sur cette machine — voir CLAUDE.md).
"""
import os
import subprocess
import sys
from pathlib import Path

import dagster as dg

from data_platform.config import DUCKDB_PATH, ROOT

DBT_PROJECT_DIR = ROOT / "dbt_project"
# Utilise l'exécutable dbt du même venv que Dagster, sans dépendre du PATH courant.
DBT_BIN = str(Path(sys.executable).parent / "dbt")


def dbt_build(context: dg.AssetExecutionContext, *model_names: str) -> None:
    result = subprocess.run(
        [
            DBT_BIN,
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--select",
            *model_names,
        ],
        cwd=ROOT,
        env={**os.environ, "DUCKDB_PATH": DUCKDB_PATH},
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise RuntimeError(f"dbt build a échoué (code {result.returncode})")
