-- Colonnes de mesure dynamiques (voir data_platform/ingestion/withings.py::TYPE_NAMES) :
-- dlt les ajoute à la table bronze seulement au premier run où le type apparaît.
-- Si Withings te renvoie un nouveau type (tension, température...), ajoute-le ici.
select
    grpid,
    cast(date as date) as date,
    poids_kg,
    masse_maigre_kg,
    masse_grasse_kg,
    masse_musculaire_kg,
    hydratation_kg,
    masse_osseuse_kg,
    taux_graisse_pct,
    pouls,
    spo2_pct
from {{ source('bronze', 'withings_measures') }}
order by date desc
