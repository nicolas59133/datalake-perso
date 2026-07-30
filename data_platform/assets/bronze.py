"""Couche BRONZE — ingestion brute dans le lake (DuckDB), orchestrée par Dagster.

Chaque source = un asset. L'asset lance un pipeline dlt qui écrit dans le schéma
`bronze` du fichier DuckDB. Dagster gère l'exécution, le planning, l'observabilité
et la lignée (bronze -> silver).
"""
import dlt
import duckdb
import dagster as dg
from dlt.destinations import duckdb as duckdb_dest

from data_platform.config import (
    APPLE_HEALTH_EXPORT_PATH,
    DUCKDB_PATH,
    LATITUDE,
    LONGITUDE,
    WEATHER_START_DATE,
)
from data_platform.ingestion.apple_health import apple_health_resources
from data_platform.ingestion.google_health import google_health_resources
from data_platform.ingestion.weather import weather_daily
from data_platform.ingestion.withings import withings_measures


def _count(table: str) -> int:
    con = duckdb.connect(DUCKDB_PATH)
    try:
        return con.execute(f"select count(*) from {table}").fetchone()[0]
    finally:
        con.close()


@dg.asset(
    key=["bronze", "weather_daily"],
    group_name="bronze",
    kinds={"dlt", "duckdb"},
    description="Relevés météo journaliers (Open-Meteo) chargés en brut.",
)
def bronze_weather_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    pipeline = dlt.pipeline(
        pipeline_name="weather",
        destination=duckdb_dest(DUCKDB_PATH),
        dataset_name="bronze",
    )
    pipeline.run(weather_daily(LATITUDE, LONGITUDE, WEATHER_START_DATE))
    rows = _count("bronze.weather_daily")
    context.log.info(f"{rows} lignes météo en bronze")
    return dg.MaterializeResult(
        metadata={
            "lignes": dg.MetadataValue.int(rows),
            "table": dg.MetadataValue.text("bronze.weather_daily"),
        }
    )


@dg.asset(
    key=["bronze", "withings_measures"],
    group_name="bronze",
    kinds={"dlt", "duckdb"},
    description="Mesures corporelles Withings (poids, masse grasse, tension, pouls…).",
)
def bronze_withings_measures(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    pipeline = dlt.pipeline(
        pipeline_name="withings",
        destination=duckdb_dest(DUCKDB_PATH),
        dataset_name="bronze",
    )
    pipeline.run(withings_measures())
    rows = _count("bronze.withings_measures")
    context.log.info(f"{rows} groupes de mesures Withings en bronze")
    return dg.MaterializeResult(
        metadata={
            "lignes": dg.MetadataValue.int(rows),
            "table": dg.MetadataValue.text("bronze.withings_measures"),
        }
    )


APPLE_HEALTH_SPECS = [
    dg.AssetSpec(
        key=["bronze", "health_records"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        description="Toutes les mesures Apple Health, brutes (pas, fréquence cardiaque, sommeil, tension…), une ligne par enregistrement.",
    ),
    dg.AssetSpec(
        key=["bronze", "health_workouts"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        description="Séances de sport Apple Health (type, distance, énergie brûlée).",
    ),
    dg.AssetSpec(
        key=["bronze", "health_activity_summary"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        description="Anneaux d'activité Apple Health, un par jour (énergie, exercice, debout).",
    ),
]


@dg.multi_asset(specs=APPLE_HEALTH_SPECS)
def bronze_apple_health(context: dg.AssetExecutionContext):
    """Un seul parsing de export.xml (potentiellement volumineux) alimente les
    3 tables en un seul pipeline.run, plutôt que reparser le fichier 3 fois."""
    pipeline = dlt.pipeline(
        pipeline_name="apple_health",
        destination=duckdb_dest(DUCKDB_PATH),
        dataset_name="bronze",
    )
    pipeline.run(apple_health_resources(APPLE_HEALTH_EXPORT_PATH))

    counts = {
        "health_records": _count("bronze.health_records"),
        "health_workouts": _count("bronze.health_workouts"),
        "health_activity_summary": _count("bronze.health_activity_summary"),
    }
    context.log.info(f"Apple Health en bronze : {counts}")
    for spec in APPLE_HEALTH_SPECS:
        table = spec.key.path[-1]
        yield dg.MaterializeResult(
            asset_key=spec.key,
            metadata={
                "lignes": dg.MetadataValue.int(counts[table]),
                "table": dg.MetadataValue.text(f"bronze.{table}"),
            },
        )


GOOGLE_HEALTH_SPECS = [
    dg.AssetSpec(
        key=["bronze", "google_health_steps"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        description="Pas comptés par la Fitbit Air (Google Health API), par intervalle.",
    ),
    dg.AssetSpec(
        key=["bronze", "google_health_heart_rate_daily"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        description="Fréquence cardiaque agrégée par jour (avg/min/max), endpoint dailyRollUp (Google Health API) — pas les points bruts, échantillonnés en continu (~500k/mois).",
    ),
    dg.AssetSpec(
        key=["bronze", "google_health_sleep"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        description="Sessions de sommeil (Google Health API).",
    ),
    dg.AssetSpec(
        key=["bronze", "google_health_weight"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        description="Mesures de poids (Google Health API).",
    ),
]


@dg.multi_asset(specs=GOOGLE_HEALTH_SPECS)
def bronze_google_health(context: dg.AssetExecutionContext):
    """Un seul rafraîchissement de token alimente les 4 appels API (voir
    google_health_resources()), plutôt que se ré-authentifier 4 fois."""
    pipeline = dlt.pipeline(
        pipeline_name="google_health",
        destination=duckdb_dest(DUCKDB_PATH),
        dataset_name="bronze",
    )
    pipeline.run(google_health_resources())

    counts = {
        "google_health_steps": _count("bronze.google_health_steps"),
        "google_health_heart_rate_daily": _count("bronze.google_health_heart_rate_daily"),
        "google_health_sleep": _count("bronze.google_health_sleep"),
        "google_health_weight": _count("bronze.google_health_weight"),
    }
    context.log.info(f"Google Health en bronze : {counts}")
    for spec in GOOGLE_HEALTH_SPECS:
        table = spec.key.path[-1]
        yield dg.MaterializeResult(
            asset_key=spec.key,
            metadata={
                "lignes": dg.MetadataValue.int(counts[table]),
                "table": dg.MetadataValue.text(f"bronze.{table}"),
            },
        )
