"""Couche SILVER — modèles dbt (dbt-duckdb), orchestrés par Dagster.

`dbt build` est lancé en subprocess via data_platform/dbt_utils.py (voir ce
module pour le pourquoi, pas dagster-dbt) depuis des `@dg.multi_asset` qui
déclarent à la main un noeud par modèle dbt, avec leurs dépendances vers les
assets bronze correspondants — même niveau de granularité dans l'UI Dagster,
sans la génération automatique.

Deux groupes SÉPARÉS plutôt qu'un seul multi_asset pour tout : un
`@dg.multi_asset` est tout-ou-rien (un `dbt build` qui échoue sur UN modèle
fait échouer TOUT le step Dagster, y compris les modèles qui ont réellement
réussi côté DB). Tant qu'Apple Health n'est pas configuré (bronze absent),
son groupe échoue sans jamais impacter météo/Withings/Google Health.
"""
import os
import shutil

import dagster as dg

from data_platform.assets.bronze import _GOOGLE_HEALTH_TABLES
from data_platform.assets.gold import GOLD_SPECS
from data_platform.config import DUCKDB_PATH, DUCKDB_VIEW_PATH
from data_platform.dbt_utils import dbt_build


CORE_SILVER_SPECS = [
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
    dg.AssetSpec(
        key=["silver", "google_health_daily"],
        group_name="silver",
        kinds={"dbt", "duckdb"},
        # Toutes les tables bronze Google Health SAUF exercise (événements
        # discrets, pivoté à part dans silver/google_health_exercice.sql).
        deps=[
            dg.AssetKey(["bronze", table])
            for table in _GOOGLE_HEALTH_TABLES
            if table != "google_health_exercise"
        ],
        description="Fitbit Air / Google Health pivoté par jour : pas, distance, activité, FC, FC repos, HRV, SpO2, sommeil, poids, masse grasse (modèle dbt : dbt_project/models/silver/google_health_daily.sql).",
    ),
    dg.AssetSpec(
        key=["silver", "google_health_exercise"],
        group_name="silver",
        kinds={"dbt", "duckdb"},
        deps=[dg.AssetKey(["bronze", "google_health_exercise"])],
        description="Séances de sport Fitbit Air / Google Health nettoyées (modèle dbt : dbt_project/models/silver/google_health_exercise.sql).",
    ),
]


@dg.multi_asset(specs=CORE_SILVER_SPECS)
def silver_dbt_models_core(context: dg.AssetExecutionContext):
    """Sources fiables (pas d'export manuel requis). Groupe séparé
    d'Apple Health pour ne jamais être bloqué par son absence/échec."""
    dbt_build(context, "weather_daily", "withings_measures", "google_health_daily", "google_health_exercise")
    for spec in CORE_SILVER_SPECS:
        yield dg.MaterializeResult(asset_key=spec.key)


APPLE_HEALTH_SILVER_SPECS = [
    dg.AssetSpec(
        key=["silver", "health_daily"],
        group_name="silver",
        kinds={"dbt", "duckdb"},
        # + dép. purement ordinale vers le groupe core : deux `dbt build`
        # distincts (subprocess séparés) écrivant sur le même fichier DuckDB
        # en parallèle provoqueraient le même lock conflict que les assets
        # bronze — voir le commentaire équivalent dans bronze.py.
        deps=[dg.AssetKey(["bronze", "health_records"]), dg.AssetKey(["silver", "weather_daily"])],
        description="Apple Health pivoté par jour : pas, distance, fréquence cardiaque, sommeil (modèle dbt : dbt_project/models/silver/health_daily.sql).",
    ),
    dg.AssetSpec(
        key=["silver", "health_workouts"],
        group_name="silver",
        kinds={"dbt", "duckdb"},
        deps=[dg.AssetKey(["bronze", "health_workouts"])],
        description="Séances de sport Apple Health nettoyées (modèle dbt : dbt_project/models/silver/health_workouts.sql).",
    ),
    dg.AssetSpec(
        key=["silver", "health_activity_summary"],
        group_name="silver",
        kinds={"dbt", "duckdb"},
        deps=[dg.AssetKey(["bronze", "health_activity_summary"])],
        description="Anneaux d'activité Apple Health nettoyés (modèle dbt : dbt_project/models/silver/health_activity_summary.sql).",
    ),
]


@dg.multi_asset(specs=APPLE_HEALTH_SILVER_SPECS)
def silver_dbt_models_apple_health(context: dg.AssetExecutionContext):
    """Échoue tant que data/apple_health_export/export.xml n'est pas déposé
    (voir APPLE_HEALTH.md) — sans effet sur silver_dbt_models_core."""
    dbt_build(context, "health_daily", "health_workouts", "health_activity_summary")
    for spec in APPLE_HEALTH_SILVER_SPECS:
        yield dg.MaterializeResult(asset_key=spec.key)


@dg.asset(
    key=["ops", "duckdb_view_snapshot"],
    group_name="ops",
    kinds={"duckdb"},
    deps=[spec.key for spec in CORE_SILVER_SPECS] + [spec.key for spec in GOLD_SPECS],
    description=(
        "Copie de data/datalake.duckdb dédiée à scripts/duckdb_ui.py. DuckDB "
        "n'autorise qu'un writer/lecteur à la fois par fichier ; sans cette "
        "copie séparée, garder l'UI ouverte ferait échouer tout run Dagster "
        "avec un lock conflict. Dépend du groupe silver fiable + gold (pas "
        "Apple Health) pour toujours tourner même si celui-ci échoue."
    ),
)
def refresh_view_snapshot(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    shutil.copyfile(DUCKDB_PATH, DUCKDB_VIEW_PATH)
    wal = f"{DUCKDB_PATH}.wal"
    if os.path.exists(wal):
        shutil.copyfile(wal, f"{DUCKDB_VIEW_PATH}.wal")
    context.log.info(f"Snapshot de visualisation mis à jour : {DUCKDB_VIEW_PATH}")
    return dg.MaterializeResult(
        metadata={"path": dg.MetadataValue.text(DUCKDB_VIEW_PATH)}
    )
