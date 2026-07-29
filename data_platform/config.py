"""Configuration centrale du projet.

Tout se règle par variables d'environnement (fichier .env), avec des valeurs
par défaut sensées pour démarrer en local sans rien configurer.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Racine du repo (deux niveaux au-dessus de ce fichier).
ROOT = Path(__file__).resolve().parent.parent

# Charge automatiquement le fichier .env s'il existe (secrets, réglages).
load_dotenv(ROOT / ".env")

# Le "lake" : un simple fichier DuckDB sur ton disque.
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(ROOT / "data" / "datalake.duckdb"))
# Copie dédiée à la visualisation (UI DuckDB, scripts/duckdb_ui.py), mise à
# jour par l'asset ops/duckdb_view_snapshot après chaque run. DuckDB n'autorise
# qu'un writer/lecteur à la fois sur un même fichier (même en lecture seule) ;
# séparer le fichier "vue" du fichier "écrit par le pipeline" évite tout lock
# concurrent entre l'UI (ouverte en continu) et Dagster/dbt.
DUCKDB_VIEW_PATH = os.getenv("DUCKDB_VIEW_PATH", str(ROOT / "data" / "datalake_view.duckdb"))

# --- Source météo (Lille par défaut) ---
LATITUDE = float(os.getenv("LATITUDE", "50.6292"))
LONGITUDE = float(os.getenv("LONGITUDE", "3.0573"))
# Historique chargé à chaque run (API archive Open-Meteo, voir ingestion/weather.py).
WEATHER_START_DATE = os.getenv("WEATHER_START_DATE", "2018-01-01")

# --- Source Withings ---
WITHINGS_CLIENT_ID = os.getenv("WITHINGS_CLIENT_ID", "")
WITHINGS_CLIENT_SECRET = os.getenv("WITHINGS_CLIENT_SECRET", "")
WITHINGS_REDIRECT_URI = os.getenv("WITHINGS_REDIRECT_URI", "http://localhost:3000")
# Fichier où l'on garde le refresh_token (Withings le fait tourner à chaque usage).
WITHINGS_TOKEN_PATH = os.getenv(
    "WITHINGS_TOKEN_PATH", str(ROOT / "data" / "withings_token.json")
)

# --- Source Apple Health ---
# Pas d'API : export manuel depuis l'app Santé (profil -> Exporter toutes les
# données de santé) -> zip contenant export.xml -> à décompresser ici. Voir
# APPLE_HEALTH.md. Chaque nouvel export déposé remplace le précédent ; le
# `merge` dlt (voir ingestion/apple_health.py) dédoublonne sur les runs.
APPLE_HEALTH_EXPORT_PATH = os.getenv(
    "APPLE_HEALTH_EXPORT_PATH", str(ROOT / "data" / "apple_health_export" / "export.xml")
)

# --- Source Google Health (Fitbit Air) ---
# OAuth2 standard Google (contrairement à Withings, pas de signature HMAC).
# Voir GOOGLE_HEALTH.md pour créer le client OAuth (Google Cloud Console).
GOOGLE_HEALTH_CLIENT_ID = os.getenv("GOOGLE_HEALTH_CLIENT_ID", "")
GOOGLE_HEALTH_CLIENT_SECRET = os.getenv("GOOGLE_HEALTH_CLIENT_SECRET", "")
GOOGLE_HEALTH_REDIRECT_URI = os.getenv("GOOGLE_HEALTH_REDIRECT_URI", "http://localhost:3000")
GOOGLE_HEALTH_TOKEN_PATH = os.getenv(
    "GOOGLE_HEALTH_TOKEN_PATH", str(ROOT / "data" / "google_health_token.json")
)

# On s'assure que le dossier data/ existe.
Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
