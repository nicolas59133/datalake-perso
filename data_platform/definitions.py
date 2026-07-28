"""Point d'entrée Dagster : rassemble assets, job et planning.

`dagster dev` lit ce module (voir pyproject.toml -> [tool.dagster]).
"""
import dagster as dg

from data_platform.assets import bronze, silver

# Tous les assets déclarés dans ces modules.
all_assets = dg.load_assets_from_modules([bronze, silver])

# Un job qui rafraîchit tout le pipeline (bronze -> silver).
refresh_all = dg.define_asset_job(name="refresh_all", selection="*")

# Planning : tous les jours à 06:00.
daily_schedule = dg.ScheduleDefinition(
    name="daily_refresh",
    job=refresh_all,
    cron_schedule="0 6 * * *",
)

defs = dg.Definitions(
    assets=all_assets,
    jobs=[refresh_all],
    schedules=[daily_schedule],
)
