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
    # Dépendance purement ordinale (pas de lien de données) : force ce step
    # après bronze_weather_daily quelle que soit la façon dont on déclenche
    # la matérialisation (bouton "Materialize all", CLI --select "*"...), qui
    # peut ignorer l'executor_def séquentiel de refresh_all. Sans ordre
    # explicite entre assets bronze, Dagster peut les paralléliser -> lock
    # DuckDB conflict (un seul writer à la fois). Voir CLAUDE.md.
    deps=[dg.AssetKey(["bronze", "weather_daily"])],
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
        # Ordre forcé après google_health (voir commentaire sur withings_measures).
        deps=[dg.AssetKey(["bronze", "google_health_steps"])],
        description="Toutes les mesures Apple Health, brutes (pas, fréquence cardiaque, sommeil, tension…), une ligne par enregistrement.",
    ),
    dg.AssetSpec(
        key=["bronze", "health_workouts"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        deps=[dg.AssetKey(["bronze", "google_health_steps"])],
        description="Séances de sport Apple Health (type, distance, énergie brûlée).",
    ),
    dg.AssetSpec(
        key=["bronze", "health_activity_summary"],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        deps=[dg.AssetKey(["bronze", "google_health_steps"])],
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


# table -> description. Une entrée = une table bronze produite par
# google_health_resources() (data_platform/ingestion/google_health.py) ;
# les deux listes doivent rester synchronisées (couvert par le test
# test_google_health_resources_tables_match_bronze_specs).
_GOOGLE_HEALTH_TABLES = {
    "google_health_steps": "Pas comptés par intervalle (minute par minute).",
    "google_health_distance": "Distance parcourue par intervalle (minute par minute).",
    "google_health_active_zone_minutes": "Minutes en zone cardio active par intervalle, avec la zone (FAT_BURN/CARDIO/PEAK).",
    "google_health_sedentary_periods": "Périodes sédentaires (intervalles sans valeur, juste début/fin).",
    "google_health_activity_level": "Niveau d'activité par intervalle (SEDENTARY/LIGHTLY_ACTIVE/MODERATELY_ACTIVE/VERY_ACTIVE).",
    "google_health_heart_rate_daily": "Fréquence cardiaque agrégée par jour (avg/min/max), endpoint dailyRollUp — pas les points bruts, échantillonnés en continu (~500k/mois).",
    "google_health_resting_heart_rate_daily": "Fréquence cardiaque au repos, déjà agrégée par jour côté API (type daily-native).",
    "google_health_hrv_daily": "Variabilité de la fréquence cardiaque, déjà agrégée par jour côté API (type daily-native).",
    "google_health_oxygen_saturation_daily": "SpO2 (saturation en oxygène), déjà agrégée par jour côté API (type daily-native).",
    "google_health_sleep": "Sessions de sommeil (stades, minutes endormi/éveillé).",
    "google_health_weight": "Mesures de poids ponctuelles (parfois synchronisées depuis une balance Withings).",
    "google_health_body_fat": "Mesures de masse grasse ponctuelles (parfois synchronisées depuis une balance Withings).",
    "google_health_exercise": "Séances de sport (type, durée, calories, distance, FC moyenne).",
}

GOOGLE_HEALTH_SPECS = [
    dg.AssetSpec(
        key=["bronze", table],
        group_name="bronze",
        kinds={"dlt", "duckdb"},
        # Un seul ordre forcé sur le premier (steps) après withings_measures ;
        # les 12 autres tables du même multi_asset héritent de cet ordre
        # puisqu'elles partagent le même step Dagster. Voir bronze_withings_measures.
        deps=[dg.AssetKey(["bronze", "withings_measures"])] if table == "google_health_steps" else [],
        description=f"{description} (Google Health API)",
    )
    for table, description in _GOOGLE_HEALTH_TABLES.items()
]


@dg.multi_asset(specs=GOOGLE_HEALTH_SPECS)
def bronze_google_health(context: dg.AssetExecutionContext):
    """Un seul rafraîchissement de token alimente tous les appels API (voir
    google_health_resources()), plutôt que se ré-authentifier par type."""
    pipeline = dlt.pipeline(
        pipeline_name="google_health",
        destination=duckdb_dest(DUCKDB_PATH),
        dataset_name="bronze",
    )
    pipeline.run(google_health_resources())

    for spec in GOOGLE_HEALTH_SPECS:
        table = spec.key.path[-1]
        rows = _count(f"bronze.{table}")
        context.log.info(f"{table} : {rows} lignes")
        yield dg.MaterializeResult(
            asset_key=spec.key,
            metadata={
                "lignes": dg.MetadataValue.int(rows),
                "table": dg.MetadataValue.text(f"bronze.{table}"),
            },
        )
