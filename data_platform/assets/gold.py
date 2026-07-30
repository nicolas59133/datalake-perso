"""Couche GOLD — agrégats métier construits à partir de la couche silver
(jointures/regroupements multi-source, prêts à consommer sans SQL). Voir
data_platform/dbt_utils.py pour le pourquoi du subprocess `dbt build`
plutôt que dagster-dbt.
"""
import dagster as dg

from data_platform.dbt_utils import dbt_build

GOLD_SPECS = [
    dg.AssetSpec(
        key=["gold", "google_health_exercise_minutes_by_type"],
        group_name="gold",
        kinds={"dbt", "duckdb"},
        # Dépendance réelle (ref() dbt vers un modèle silver, pas source())
        # -> Dagster ordonne déjà correctement ce step après
        # silver_dbt_models_core, pas besoin de dépendance ordinale.
        deps=[dg.AssetKey(["silver", "google_health_exercise"])],
        description="Minutes totales par type d'exercice (modèle dbt : dbt_project/models/gold/google_health_exercise_minutes_by_type.sql).",
    ),
]


@dg.multi_asset(specs=GOLD_SPECS)
def gold_dbt_models(context: dg.AssetExecutionContext):
    dbt_build(context, "google_health_exercise_minutes_by_type")
    for spec in GOLD_SPECS:
        yield dg.MaterializeResult(asset_key=spec.key)
