"""Source Withings — mesures corporelles (poids, masse grasse, tension, pouls…).

Withings impose une authentification signée (HMAC-SHA256) + un "nonce". Toute
cette complexité est gérée ici une fois pour toutes : le reste du projet n'a
qu'à appeler `withings_measures()`.

Flux :
  - Tu obtiens un refresh_token UNE fois via scripts/withings_auth.py.
  - Ici, on échange ce refresh_token contre un access_token à chaque run.
  - Withings fait TOURNER le refresh_token à chaque échange -> on le resauvegarde.
  - Avec l'access_token, on appelle getmeas et on renvoie des lignes propres.
"""
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone

import dlt
import requests

from data_platform.config import (
    WITHINGS_CLIENT_ID,
    WITHINGS_CLIENT_SECRET,
    WITHINGS_TOKEN_PATH,
)

WBS = "https://wbsapi.withings.net"

# Types de mesures Withings -> noms lisibles. La vraie valeur = value * 10**unit.
TYPE_NAMES = {
    1: "poids_kg",
    5: "masse_maigre_kg",
    6: "taux_graisse_pct",
    8: "masse_grasse_kg",
    9: "tension_diastolique",
    10: "tension_systolique",
    11: "pouls",
    12: "temperature",
    54: "spo2_pct",
    76: "masse_musculaire_kg",
    77: "hydratation_kg",
    88: "masse_osseuse_kg",
}


def _sign(values: list, client_secret: str) -> str:
    """HMAC-SHA256 des valeurs concaténées par des virgules (clé = client_secret)."""
    message = ",".join(str(v) for v in values)
    return hmac.new(
        client_secret.encode(), message.encode(), hashlib.sha256
    ).hexdigest()


def get_nonce(client_id: str, client_secret: str) -> str:
    """Récupère un nonce (jeton à usage unique) requis avant tout appel signé."""
    ts = int(time.time())
    # Ordre alphabétique des clés : action, client_id, timestamp.
    signature = _sign(["getnonce", client_id, ts], client_secret)
    resp = requests.post(
        f"{WBS}/v2/signature",
        data={
            "action": "getnonce",
            "client_id": client_id,
            "timestamp": ts,
            "signature": signature,
        },
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != 0:
        raise RuntimeError(f"getnonce a échoué (status {body.get('status')}): {body}")
    return body["body"]["nonce"]


def _requesttoken(client_id: str, client_secret: str, extra: dict) -> dict:
    """Appel signé au service requesttoken (échange de code OU rafraîchissement)."""
    nonce = get_nonce(client_id, client_secret)
    # Signature sur action, client_id, nonce (ordre alphabétique).
    signature = _sign(["requesttoken", client_id, nonce], client_secret)
    params = {
        "action": "requesttoken",
        "client_id": client_id,
        "nonce": nonce,
        "signature": signature,
        **extra,
    }
    resp = requests.post(f"{WBS}/v2/oauth2", data=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != 0:
        raise RuntimeError(
            f"requesttoken a échoué (status {body.get('status')}): {body}"
        )
    return body["body"]


def exchange_code(client_id, client_secret, code, redirect_uri) -> dict:
    """Échange le code d'autorisation (obtenu dans le navigateur) contre des tokens."""
    return _requesttoken(
        client_id,
        client_secret,
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        },
    )


def refresh_tokens(client_id, client_secret, refresh_token) -> dict:
    """Rafraîchit l'access_token. Renvoie AUSSI un nouveau refresh_token (rotation)."""
    return _requesttoken(
        client_id,
        client_secret,
        {"grant_type": "refresh_token", "refresh_token": refresh_token},
    )


def _load_token() -> dict:
    try:
        with open(WITHINGS_TOKEN_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        raise RuntimeError(
            "Aucun token Withings trouvé. Lance d'abord :\n"
            "    .venv/bin/python scripts/withings_auth.py\n"
            "pour autoriser l'accès une première fois."
        )


def _save_token(data: dict) -> None:
    with open(WITHINGS_TOKEN_PATH, "w") as f:
        json.dump(data, f, indent=2)


def get_valid_access_token() -> str:
    """Renvoie un access_token valide, en rafraîchissant + resauvegardant le token."""
    saved = _load_token()
    fresh = refresh_tokens(
        WITHINGS_CLIENT_ID, WITHINGS_CLIENT_SECRET, saved["refresh_token"]
    )
    # Withings fait tourner le refresh_token : on garde toujours le plus récent.
    _save_token(
        {
            "refresh_token": fresh["refresh_token"],
            "access_token": fresh["access_token"],
            "userid": fresh.get("userid", saved.get("userid")),
            "expires_in": fresh.get("expires_in"),
        }
    )
    return fresh["access_token"]


def parse_measures(payload: dict) -> list[dict]:
    """Transforme la réponse getmeas en lignes larges (une ligne par mesure/jour).

    Fonction pure -> testable sans réseau.
    """
    groups = payload.get("body", {}).get("measuregrps", []) or []
    rows = []
    for g in groups:
        ts = g.get("date")
        row = {
            "grpid": g.get("grpid"),
            "date": datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            if ts
            else None,
            "unix_date": ts,
        }
        for m in g.get("measures", []):
            name = TYPE_NAMES.get(m["type"], f"type_{m['type']}")
            row[name] = m["value"] * (10 ** m["unit"])
        rows.append(row)
    return rows


def fetch_measures(access_token: str, startdate: int | None = None) -> dict:
    """Appelle getmeas. `startdate` = timestamp unix (par défaut ~2 ans en arrière)."""
    if startdate is None:
        startdate = int(time.time()) - 2 * 365 * 24 * 3600
    resp = requests.post(
        f"{WBS}/measure",
        data={
            "action": "getmeas",
            "meastypes": ",".join(str(t) for t in TYPE_NAMES),
            "category": 1,  # 1 = vraies mesures (pas les objectifs)
            "startdate": startdate,
        },
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


@dlt.resource(name="withings_measures", write_disposition="merge", primary_key="grpid")
def withings_measures():
    """Resource dlt : renvoie tes mesures corporelles. `merge` sur grpid => pas de doublon."""
    access_token = get_valid_access_token()
    payload = fetch_measures(access_token)
    if payload.get("status") != 0:
        raise RuntimeError(f"getmeas a échoué (status {payload.get('status')}): {payload}")
    yield from parse_measures(payload)
