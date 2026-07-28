"""Autorisation Withings — À LANCER UNE SEULE FOIS.

Ce script te fait autoriser l'accès à tes données dans le navigateur, récupère
un refresh_token (ta clé longue durée) et le range dans data/withings_token.json.
Ensuite, l'ingestion tourne toute seule.

Lance-le avec :
    .venv/bin/python scripts/withings_auth.py
"""
import json
import secrets
import sys
from urllib.parse import urlencode, urlparse, parse_qs

# Permet d'importer le package depuis la racine du repo.
sys.path.insert(0, ".")

from data_platform.config import (
    WITHINGS_CLIENT_ID,
    WITHINGS_CLIENT_SECRET,
    WITHINGS_REDIRECT_URI,
    WITHINGS_TOKEN_PATH,
)
from data_platform.ingestion.withings import exchange_code

AUTHORIZE_URL = "https://account.withings.com/oauth2_user/authorize2"
SCOPE = "user.info,user.metrics,user.activity"


def main():
    if not WITHINGS_CLIENT_ID or not WITHINGS_CLIENT_SECRET:
        print("ERREUR : WITHINGS_CLIENT_ID / WITHINGS_CLIENT_SECRET manquants dans .env")
        sys.exit(1)

    state = secrets.token_urlsafe(8)
    url = AUTHORIZE_URL + "?" + urlencode(
        {
            "response_type": "code",
            "client_id": WITHINGS_CLIENT_ID,
            "scope": SCOPE,
            "redirect_uri": WITHINGS_REDIRECT_URI,
            "state": state,
        }
    )

    print("\n=== ÉTAPE 1 : autorise l'accès ===")
    print("Ouvre CE lien dans ton navigateur, connecte-toi et clique 'Accepter' :\n")
    print(url + "\n")
    print("Ton navigateur va ensuite afficher une page d'erreur 'site inaccessible'")
    print("(c'est NORMAL : rien n'écoute sur localhost). Regarde la barre d'adresse :")
    print("elle contient ...?code=XXXXXX&state=...  ->  c'est ce 'code' qu'il nous faut.\n")

    print("=== ÉTAPE 2 : colle ici l'adresse complète (ou juste le code) ===")
    raw = input("> ").strip()

    # Accepte soit l'URL complète, soit le code brut.
    code = raw
    if "code=" in raw:
        parsed = parse_qs(urlparse(raw).query)
        code = parsed.get("code", [raw])[0]

    if not code:
        print("Aucun code détecté. Réessaie.")
        sys.exit(1)

    print("\nÉchange du code contre les tokens…")
    body = exchange_code(
        WITHINGS_CLIENT_ID, WITHINGS_CLIENT_SECRET, code, WITHINGS_REDIRECT_URI
    )

    with open(WITHINGS_TOKEN_PATH, "w") as f:
        json.dump(
            {
                "refresh_token": body["refresh_token"],
                "access_token": body["access_token"],
                "userid": body.get("userid"),
                "expires_in": body.get("expires_in"),
            },
            f,
            indent=2,
        )

    print(f"\n✅ C'est bon ! Token enregistré dans {WITHINGS_TOKEN_PATH}")
    print("Tu peux maintenant matérialiser l'asset Withings dans Dagster.")


if __name__ == "__main__":
    main()
