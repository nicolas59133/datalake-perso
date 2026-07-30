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

Périmètre v1 : 4 types de données (steps, heart-rate, sleep, weight).
Google Health en expose ~15 au total (distance, floors, SpO2, VO2 max,
glycémie...) ; en ajouter un = répéter le patron de
list_data_points()/parse_xxx() ci-dessous une fois le schéma exact vérifié
par un appel réel (la doc publique s'est révélée incomplète/imprécise sur
plusieurs points, voir CLAUDE.md).

heart-rate est échantillonné en continu (~500k points vus pour 1 mois
d'usage) : au lieu de paginer les points bruts, on utilise l'endpoint
`dataPoints:dailyRollUp` (agrégation journalière côté serveur — avg/min/max
BPM par jour), qui ne renvoie qu'une poignée de lignes au lieu de centaines
de milliers.
"""
import hashlib
import json
from datetime import date, timedelta

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


def _data_point_id(dp: dict, *fallback_fields) -> str:
    """Certains types (steps, heart-rate observés en pratique) n'ont PAS de
    `dataPoint["name"]` contrairement à ce que laissait supposer l'exemple
    "exercise" de la doc publique — seuls sleep/weight en ont un dans les
    tests réels. Fallback : hash déterministe des champs fournis, même
    patron que record_id/workout_id dans apple_health.py."""
    name = dp.get("name")
    if name:
        return name
    key = "|".join(str(f) for f in fallback_fields)
    return hashlib.sha1(key.encode()).hexdigest()


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


def list_data_points(access_token: str, data_type: str, max_pages: int | None = None) -> list[dict]:
    """Récupère les dataPoints d'un type (pagination via nextPageToken).
    Pas de filtre de date côté requête (non confirmé par type dans la doc
    publique au moment de l'écriture) : comme pour la météo, on retélécharge
    tout et le `merge` dlt dédoublonne (dataPoint["name"] est un ID stable
    côté Google, contrairement à Apple Health).

    `max_pages` : garde-fou pour les types à très haut débit (heart-rate
    échantillonné en continu peut dépasser 500k points/mois -> des dizaines
    de minutes de pagination). Si atteint, log un avertissement clair (pas de
    troncature silencieuse) plutôt que de tout retélécharger à chaque run.
    TODO : remplacer par un vrai filtre de date côté requête une fois son
    champ exact confirmé par type (voir GOOGLE_HEALTH.md)."""
    points: list[dict] = []
    page_token = None
    pages = 0
    while True:
        pages += 1
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
        if max_pages is not None and pages >= max_pages:
            print(
                f"[google_health] ATTENTION : {data_type} plafonné à {pages} pages "
                f"({len(points)} points) — il reste des données non chargées. "
                "Voir GOOGLE_HEALTH.md (filtre de date à ajouter)."
            )
            break
    return points


def parse_steps(data_points: list[dict]) -> list[dict]:
    rows = []
    for dp in data_points:
        steps = dp.get("steps") or {}
        interval = steps.get("interval", {})
        rows.append(
            {
                "data_point_id": _data_point_id(
                    dp, "steps", interval.get("startTime"), interval.get("endTime"), steps.get("count")
                ),
                "platform": (dp.get("dataSource") or {}).get("platform"),
                "start_time": interval.get("startTime"),
                "start_utc_offset_s": _offset_seconds(interval.get("startUtcOffset")),
                "end_time": interval.get("endTime"),
                "count": _to_int(steps.get("count")),
            }
        )
    return rows


def _civil_date(d: date) -> dict:
    return {"date": {"year": d.year, "month": d.month, "day": d.day}}


def list_daily_rollup(
    access_token: str, data_type: str, start_date: date, end_date: date, chunk_days: int = 14
) -> list[dict]:
    """Agrégats journaliers côté serveur (`dataPoints:dailyRollUp`) sur
    `[start_date, end_date)`. Chaque requête est plafonnée à `chunk_days`
    jours (14 = plafond observé pour heart-rate ; un autre type pourrait
    avoir un plafond différent, ajuste si `INVALID_ROLLUP_QUERY_DURATION`
    apparaît dans les logs)."""
    points: list[dict] = []
    cursor = start_date
    while cursor < end_date:
        chunk_end = min(cursor + timedelta(days=chunk_days), end_date)
        page_token = None
        while True:
            body = {
                "range": {"start": _civil_date(cursor), "end": _civil_date(chunk_end)},
                "windowSizeDays": 1,
            }
            if page_token:
                body["pageToken"] = page_token
            resp = requests.post(
                f"{API_BASE}/users/me/dataTypes/{data_type}/dataPoints:dailyRollUp",
                json=body,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30,
            )
            resp.raise_for_status()
            resp_body = resp.json()
            points.extend(resp_body.get("rollupDataPoints", []))
            page_token = resp_body.get("nextPageToken")
            if not page_token:
                break
        cursor = chunk_end
    return points


def parse_heart_rate_daily(rollup_points: list[dict]) -> list[dict]:
    rows = []
    for rp in rollup_points:
        hr = rp.get("heartRate") or {}
        d = (rp.get("civilStartTime") or {}).get("date") or {}
        date_str = f"{d['year']:04d}-{d['month']:02d}-{d['day']:02d}" if d else None
        rows.append(
            {
                "date": date_str,
                "avg_bpm": _to_float(hr.get("beatsPerMinuteAvg")),
                "min_bpm": _to_int(hr.get("beatsPerMinuteMin")),
                "max_bpm": _to_int(hr.get("beatsPerMinuteMax")),
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
                "data_point_id": _data_point_id(dp, "sleep", interval.get("startTime"), interval.get("endTime")),
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
                "data_point_id": _data_point_id(dp, "weight", sample_time.get("physicalTime"), grams),
                "platform": (dp.get("dataSource") or {}).get("platform"),
                "sample_time": sample_time.get("physicalTime"),
                "sample_utc_offset_s": _offset_seconds(sample_time.get("utcOffset")),
                "weight_kg": grams / 1000 if grams is not None else None,
            }
        )
    return rows


# Recul par défaut pour l'agrégat quotidien de FC — généreux (l'API ne facture
# rien par appel), pas besoin d'un réglage fin tant que l'historique tient
# dans quelques requêtes de chunk_days jours.
HEART_RATE_LOOKBACK_DAYS = 90


def google_health_resources():
    """Renvoie les 4 dlt resources prêtes pour `pipeline.run([...])` — un seul
    rafraîchissement de token réutilisé pour les appels API."""
    access_token = get_valid_access_token()
    today = date.today()
    return [
        dlt.resource(
            parse_steps(list_data_points(access_token, "steps")),
            name="google_health_steps",
            write_disposition="merge",
            primary_key="data_point_id",
        ),
        dlt.resource(
            parse_heart_rate_daily(
                list_daily_rollup(
                    access_token,
                    "heart-rate",
                    today - timedelta(days=HEART_RATE_LOOKBACK_DAYS),
                    today + timedelta(days=1),  # end exclusif -> inclut aujourd'hui
                )
            ),
            name="google_health_heart_rate_daily",
            write_disposition="merge",
            primary_key="date",
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
