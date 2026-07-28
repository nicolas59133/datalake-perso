"""Source météo — API ouverte Open-Meteo (aucune clé requise).

C'est la source "facile" pour démarrer : elle tourne dès le premier `dagster dev`
sans aucun identifiant. Les sources qui demandent un login (Withings, Garmin…)
suivront le même patron, avec des secrets en plus.
"""
import dlt
import requests

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def parse_open_meteo(payload: dict, latitude: float, longitude: float) -> list[dict]:
    """Transforme la réponse JSON (des tableaux parallèles) en lignes propres.

    Fonction pure -> facile à tester sans réseau.
    """
    daily = payload.get("daily", {}) or {}
    dates = daily.get("time", []) or []
    tmax = daily.get("temperature_2m_max", []) or []
    tmin = daily.get("temperature_2m_min", []) or []
    prcp = daily.get("precipitation_sum", []) or []

    rows: list[dict] = []
    for i, day in enumerate(dates):
        rows.append(
            {
                "date": day,
                "temp_max": tmax[i] if i < len(tmax) else None,
                "temp_min": tmin[i] if i < len(tmin) else None,
                "precipitation": prcp[i] if i < len(prcp) else None,
                "latitude": latitude,
                "longitude": longitude,
            }
        )
    return rows


@dlt.resource(name="weather_daily", write_disposition="merge", primary_key="date")
def weather_daily(latitude: float, longitude: float, past_days: int = 7):
    """Récupère les relevés journaliers. `merge` sur `date` => réexécuter met à jour
    sans créer de doublons (comportement incrémental "pro" offert par dlt)."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "auto",
        "past_days": past_days,
        "forecast_days": 1,
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    yield from parse_open_meteo(resp.json(), latitude, longitude)
