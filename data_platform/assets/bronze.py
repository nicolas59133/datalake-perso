"""Couche BRONZE — ingestion brute dans le lake (DuckDB), orchestrée par Dagster.

Chaque source = un asset. L'asset lance un pipeline dlt qui écrit dans le schéma
`bronze` du fichier DuckDB. Dagster gère l'exécution, le planning, l'observabilité
et la lignée (bronze -> silver).
"""
import dlt
import duckdb
import dagster as dg
from dlt.destinations import duckdb as duckdb_dest

from data_platform.config import DUCKDB_PATH, LATITUDE, LONGITUDE, WEATHER_START_DATE
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
