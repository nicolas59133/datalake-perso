"""Point d'entrée Dagster : rassemble assets, job et planning.

`dagster dev` lit ce module (voir pyproject.toml -> [tool.dagster]).
"""
import dagster as dg

from data_platform.assets import bronze, silver

# Tous les assets déclarés dans ces modules.
all_assets = dg.load_assets_from_modules([bronze, silver])

# Un job qui rafraîchit tout le pipeline (bronze -> silver).
# Exécution séquentielle : les assets bronze écrivent tous dans le même fichier
# DuckDB, qui n'autorise qu'un seul writer à la fois (sinon "Could not set lock").
refresh_all = dg.define_asset_job(
    name="refresh_all",
    selection="*",
    executor_def=dg.in_process_executor,
)

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
