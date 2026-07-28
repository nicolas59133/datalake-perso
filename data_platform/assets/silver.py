"""Couche SILVER — nettoyage / typage / enrichissement, en SQL DuckDB.

Ici c'est du SQL simple pour démarrer. Quand tu voudras industrialiser, tu
remplaces ces transformations par des modèles dbt (dbt-duckdb) sans changer
l'architecture : bronze reste la source, silver/gold deviennent des modèles dbt.
"""
import duckdb
import dagster as dg

from data_platform.config import DUCKDB_PATH


def build_silver_weather(con: duckdb.DuckDBPyConnection) -> None:
    """Transformation pure (testable) : lit bronze, écrit silver."""
    con.execute("create schema if not exists silver")
    con.execute(
        """
        create or replace table silver.weather_daily as
        select
            cast(date as date)              as date,
            temp_max,
            temp_min,
            round(temp_max - temp_min, 1)   as temp_range,
            precipitation,
            latitude,
            longitude
        from bronze.weather_daily
        order by date
        """
    )


@dg.asset(
    key=["silver", "weather_daily"],
    group_name="silver",
    kinds={"duckdb"},
    deps=[dg.AssetKey(["bronze", "weather_daily"])],
    description="Météo nettoyée + amplitude thermique (temp_range).",
)
def silver_weather_daily(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    con = duckdb.connect(DUCKDB_PATH)
    build_silver_weather(con)
    rows = con.execute("select count(*) from silver.weather_daily").fetchone()[0]
    con.close()

    return dg.MaterializeResult(
        metadata={
            "lignes": dg.MetadataValue.int(rows),
            "table": dg.MetadataValue.text("silver.weather_daily"),
        }
    )
