"""Source météo — API archive (historique) Open-Meteo (aucune clé requise).

C'est la source "facile" pour démarrer : elle tourne dès le premier `dagster dev`
sans aucun identifiant. Les sources qui demandent un login (Withings, Garmin…)
suivront le même patron, avec des secrets en plus.

On utilise l'API "archive" (réanalyse ERA5) plutôt que l'API "forecast" : elle
couvre tout l'historique depuis WEATHER_START_DATE jusqu'à aujourd'hui en un
seul appel (l'API forecast ne remonte que ~92 jours en arrière via `past_days`).
"""
from datetime import date

import dlt
import requests

OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def parse_open_meteo(payload: dict, latitude: float, longitude: float) -> list[dict]:
    """Transforme la réponse JSON (des tableaux parallèles) en lignes propres.

    Fonction pure -> facile à tester sans réseau.
    """
    daily = payload.get("daily", {}) or {}
    dates = daily.get("time", []) or []
    tmax = daily.get("temperature_2m_max", []) or []
    tmin = daily.get("temperature_2m_min", []) or []
    prcp = daily.get("precipitation_sum", []) or []
    pressure = daily.get("pressure_msl_mean", []) or []

    rows: list[dict] = []
    for i, day in enumerate(dates):
        rows.append(
            {
                "date": day,
                "temp_max": tmax[i] if i < len(tmax) else None,
                "temp_min": tmin[i] if i < len(tmin) else None,
                "precipitation": prcp[i] if i < len(prcp) else None,
                "pressure_msl": pressure[i] if i < len(pressure) else None,
                "latitude": latitude,
                "longitude": longitude,
            }
        )
    return rows


@dlt.resource(name="weather_daily", write_disposition="merge", primary_key="date")
def weather_daily(latitude: float, longitude: float, start_date: str):
    """Récupère tout l'historique journalier de start_date à aujourd'hui.
    `merge` sur `date` => réexécuter met à jour sans créer de doublons
    (comportement incrémental "pro" offert par dlt) ; chaque run re-télécharge
    tout l'historique (payload journalier léger, simple et toujours cohérent),
    seul le dernier jour change vraiment d'un run à l'autre."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,pressure_msl_mean",
        "timezone": "auto",
        "start_date": start_date,
        "end_date": date.today().isoformat(),
    }
    resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=60)
    resp.raise_for_status()
    yield from parse_open_meteo(resp.json(), latitude, longitude)
