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

# --- Source météo (Villeneuve-d'Ascq par défaut) ---
LATITUDE = float(os.getenv("LATITUDE", "50.62"))
LONGITUDE = float(os.getenv("LONGITUDE", "3.13"))
PAST_DAYS = int(os.getenv("PAST_DAYS", "7"))

# --- Source Withings ---
WITHINGS_CLIENT_ID = os.getenv("WITHINGS_CLIENT_ID", "")
WITHINGS_CLIENT_SECRET = os.getenv("WITHINGS_CLIENT_SECRET", "")
WITHINGS_REDIRECT_URI = os.getenv("WITHINGS_REDIRECT_URI", "http://localhost:3000")
# Fichier où l'on garde le refresh_token (Withings le fait tourner à chaque usage).
WITHINGS_TOKEN_PATH = os.getenv(
    "WITHINGS_TOKEN_PATH", str(ROOT / "data" / "withings_token.json")
)

# On s'assure que le dossier data/ existe.
Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
