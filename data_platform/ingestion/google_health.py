"""Source Google Health (Fitbit Air) — remplace l'ancienne API Fitbit Web.

Google a rebrandé Fitbit en "Google Health" (mai 2026) et sort une nouvelle
API cloud, health.googleapis.com/v4, en OAuth2 standard (contrairement à
Withings, pas de signature HMAC maison). L'ancienne API Fitbit Web ferme en
septembre 2026 — voir GOOGLE_HEALTH.md.

Flux, même patron que withings.py :
  - Tu obtiens un refresh_token UNE fois via scripts/google_health_auth.py.
  - Ici, on échange ce refresh_token contre un access_token à chaque run
    (Google ne fait PAS tourner le refresh_token à chaque usage, contrairement
    à Withings — pas besoin de le re-sauvegarder).

Périmètre v1 : 4 types de données au schéma confirmé dans la doc officielle
(steps, heart-rate, sleep, weight). Google Health en expose ~15 au total
(distance, floors, SpO2, VO2 max, glycémie...) ; en ajouter un = répéter le
patron de list_data_points()/parse_xxx() ci-dessous une fois le schéma exact
vérifié (la doc publique ne détaillait pas tous les types au moment où ce
connecteur a été écrit).
"""
import json

import dlt
import requests

from data_platform.config import (
    GOOGLE_HEALTH_CLIENT_ID,
    GOOGLE_HEALTH_CLIENT_SECRET,
    GOOGLE_HEALTH_TOKEN_PATH,
)

AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
API_BASE = "https://health.googleapis.com/v4"

SCOPES = [
    "https://www.googleapis.com/auth/googlehealth.activity_and_fitness.readonly",
    "https://www.googleapis.com/auth/googlehealth.health_metrics_and_measurements.readonly",
    "https://www.googleapis.com/auth/googlehealth.sleep.readonly",
]


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    f = _to_float(value)
    return int(f) if f is not None else None


def _offset_seconds(value):
    """Google encode les décalages horaires en durée protobuf ("-18000s") ->
    secondes (int). Nécessaire pour bucketer une mesure sur le bon jour
    calendaire local plutôt que celui du fuseau de la session DuckDB."""
    if not value:
        return None
    return _to_int(str(value).rstrip("s"))


def exchange_code(client_id: str, client_secret: str, code: str, redirect_uri: str) -> dict:
    """Échange le code d'autorisation (obtenu dans le navigateur) contre des tokens."""
    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """Échange le refresh_token contre un nouvel access_token."""
    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _load_token() -> dict:
    try:
        with open(GOOGLE_HEALTH_TOKEN_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        raise RuntimeError(
            "Aucun token Google Health trouvé. Lance d'abord :\n"
            "    .venv/bin/python scripts/google_health_auth.py\n"
            "pour autoriser l'accès une première fois. Voir GOOGLE_HEALTH.md."
        )


def get_valid_access_token() -> str:
    """Renvoie un access_token valide. Contrairement à Withings, le
    refresh_token Google reste valable tel quel (pas de rotation à gérer)."""
    saved = _load_token()
    fresh = refresh_access_token(
        GOOGLE_HEALTH_CLIENT_ID, GOOGLE_HEALTH_CLIENT_SECRET, saved["refresh_token"]
    )
    return fresh["access_token"]


def list_data_points(access_token: str, data_type: str) -> list[dict]:
    """Récupère TOUS les dataPoints d'un type (pagination via nextPageToken).
    Pas de filtre de date : comme pour la météo, on retélécharge tout et le
    `merge` dlt dédoublonne (dataPoint["name"] est un ID stable côté Google,
    contrairement à Apple Health)."""
    points: list[dict] = []
    page_token = None
    while True:
        params = {"pageSize": 1000}
        if page_token:
            params["pageToken"] = page_token
        resp = requests.get(
            f"{API_BASE}/users/me/dataTypes/{data_type}/dataPoints",
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        points.extend(body.get("dataPoints", []))
        page_token = body.get("nextPageToken")
        if not page_token:
            break
    return points


def parse_steps(data_points: list[dict]) -> list[dict]:
    rows = []
    for dp in data_points:
        steps = dp.get("steps") or {}
        interval = steps.get("interval", {})
        rows.append(
            {
                "data_point_id": dp.get("name"),
                "platform": (dp.get("dataSource") or {}).get("platform"),
                "start_time": interval.get("startTime"),
                "start_utc_offset_s": _offset_seconds(interval.get("startUtcOffset")),
                "end_time": interval.get("endTime"),
                "count": _to_int(steps.get("count")),
            }
        )
    return rows


def parse_heart_rate(data_points: list[dict]) -> list[dict]:
    rows = []
    for dp in data_points:
        hr = dp.get("heartRate") or {}
        sample_time = hr.get("sampleTime") or {}
        rows.append(
            {
                "data_point_id": dp.get("name"),
                "platform": (dp.get("dataSource") or {}).get("platform"),
                "sample_time": sample_time.get("physicalTime"),
                "sample_utc_offset_s": _offset_seconds(sample_time.get("utcOffset")),
                "beats_per_minute": _to_int(hr.get("beatsPerMinute")),
            }
        )
    return rows


def parse_sleep(data_points: list[dict]) -> list[dict]:
    rows = []
    for dp in data_points:
        sleep = dp.get("sleep") or {}
        interval = sleep.get("interval", {})
        summary = sleep.get("summary", {})
        rows.append(
            {
                "data_point_id": dp.get("name"),
                "platform": (dp.get("dataSource") or {}).get("platform"),
                "start_time": interval.get("startTime"),
                "start_utc_offset_s": _offset_seconds(interval.get("startUtcOffset")),
                "end_time": interval.get("endTime"),
                "sleep_type": sleep.get("type"),
                "minutes_asleep": _to_int(summary.get("minutesAsleep")),
                "minutes_awake": _to_int(summary.get("minutesAwake")),
            }
        )
    return rows


def parse_weight(data_points: list[dict]) -> list[dict]:
    rows = []
    for dp in data_points:
        w = dp.get("weight") or {}
        grams = _to_float(w.get("weightGrams"))
        sample_time = w.get("sampleTime") or {}
        rows.append(
            {
                "data_point_id": dp.get("name"),
                "platform": (dp.get("dataSource") or {}).get("platform"),
                "sample_time": sample_time.get("physicalTime"),
                "sample_utc_offset_s": _offset_seconds(sample_time.get("utcOffset")),
                "weight_kg": grams / 1000 if grams is not None else None,
            }
        )
    return rows


def google_health_resources():
    """Renvoie les 4 dlt resources prêtes pour `pipeline.run([...])` — un seul
    rafraîchissement de token réutilisé pour les 4 appels API."""
    access_token = get_valid_access_token()
    return [
        dlt.resource(
            parse_steps(list_data_points(access_token, "steps")),
            name="google_health_steps",
            write_disposition="merge",
            primary_key="data_point_id",
        ),
        dlt.resource(
            parse_heart_rate(list_data_points(access_token, "heart-rate")),
            name="google_health_heart_rate",
            write_disposition="merge",
            primary_key="data_point_id",
        ),
        dlt.resource(
            parse_sleep(list_data_points(access_token, "sleep")),
            name="google_health_sleep",
            write_disposition="merge",
            primary_key="data_point_id",
        ),
        dlt.resource(
            parse_weight(list_data_points(access_token, "weight")),
            name="google_health_weight",
            write_disposition="merge",
            primary_key="data_point_id",
        ),
    ]
