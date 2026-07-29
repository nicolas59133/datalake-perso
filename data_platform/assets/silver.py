"""Couche SILVER — modèles dbt (dbt-duckdb), orchestrés par Dagster.

dagster-dbt (l'intégration officielle, qui génère un asset par modèle à partir
du manifest dbt) n'est pas installable ici : sa dernière version épingle
dagster==1.12.8, qui ne supporte pas Python 3.14 (le seul Python présent sur
cette machine — voir CLAUDE.md). En attendant une release dagster-dbt
compatible, on appelle `dbt build` en subprocess depuis un `@dg.multi_asset`
qui déclare à la main un noeud par modèle dbt, avec ses dépendances vers les
assets bronze correspondants — même niveau de granularité dans l'UI Dagster,
sans la génération automatique.
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


SILVER_SPECS = [
    dg.AssetSpec(
        key=["silver", "weather_daily"],
        group_name="silver",
        kinds={"dbt", "duckdb"},
        deps=[dg.AssetKey(["bronze", "weather_daily"])],
        description="Météo nettoyée + amplitude thermique (modèle dbt : dbt_project/models/silver/weather_daily.sql).",
    ),
    dg.AssetSpec(
        key=["silver", "withings_measures"],
        group_name="silver",
        kinds={"dbt", "duckdb"},
        deps=[dg.AssetKey(["bronze", "withings_measures"])],
        description="Mesures Withings nettoyées (modèle dbt : dbt_project/models/silver/withings_measures.sql).",
    ),
]


@dg.multi_asset(specs=SILVER_SPECS)
def silver_dbt_models(context: dg.AssetExecutionContext):
    result = subprocess.run(
        [
            DBT_BIN,
            "build",
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
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

    for spec in SILVER_SPECS:
        yield dg.MaterializeResult(asset_key=spec.key)
