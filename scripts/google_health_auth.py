"""Autorisation Google Health — À LANCER UNE SEULE FOIS.

Ce script te fait autoriser l'accès à tes données dans le navigateur, récupère
un refresh_token (ta clé longue durée) et le range dans
data/google_health_token.json. Ensuite, l'ingestion tourne toute seule.

Lance-le avec :
    .venv/bin/python scripts/google_health_auth.py
"""
import json
import secrets
import sys
from urllib.parse import urlencode, urlparse, parse_qs

# Permet d'importer le package depuis la racine du repo.
sys.path.insert(0, ".")

from data_platform.config import (
    GOOGLE_HEALTH_CLIENT_ID,
    GOOGLE_HEALTH_CLIENT_SECRET,
    GOOGLE_HEALTH_REDIRECT_URI,
    GOOGLE_HEALTH_TOKEN_PATH,
)
from data_platform.ingestion.google_health import AUTH_URL, SCOPES, exchange_code


def main():
    if not GOOGLE_HEALTH_CLIENT_ID or not GOOGLE_HEALTH_CLIENT_SECRET:
        print("ERREUR : GOOGLE_HEALTH_CLIENT_ID / GOOGLE_HEALTH_CLIENT_SECRET manquants dans .env")
        sys.exit(1)

    state = secrets.token_urlsafe(8)
    url = AUTH_URL + "?" + urlencode(
        {
            "response_type": "code",
            "client_id": GOOGLE_HEALTH_CLIENT_ID,
            "redirect_uri": GOOGLE_HEALTH_REDIRECT_URI,
            "scope": " ".join(SCOPES),
            "access_type": "offline",  # nécessaire pour obtenir un refresh_token
            "prompt": "consent",       # force le consentement -> garantit le refresh_token même en re-auth
            "state": state,
        }
    )

    print("\n=== ÉTAPE 1 : autorise l'accès ===")
    print("Ouvre CE lien dans ton navigateur, connecte-toi avec le compte Google")
    print("relié à ta Fitbit Air, et clique 'Autoriser' :\n")
    print(url + "\n")
    print("Ton navigateur va ensuite afficher une page d'erreur 'site inaccessible'")
    print("(c'est NORMAL : rien n'écoute sur localhost). Regarde la barre d'adresse :")
    print("elle contient ...?code=XXXXXX&state=...  ->  c'est ce 'code' qu'il nous faut.\n")

    print("=== ÉTAPE 2 : colle ici l'adresse complète (ou juste le code) ===")
    raw = input("> ").strip()

    code = raw
    if "code=" in raw:
        parsed = parse_qs(urlparse(raw).query)
        code = parsed.get("code", [raw])[0]

    if not code:
        print("Aucun code détecté. Réessaie.")
        sys.exit(1)

    print("\nÉchange du code contre les tokens…")
    body = exchange_code(
        GOOGLE_HEALTH_CLIENT_ID, GOOGLE_HEALTH_CLIENT_SECRET, code, GOOGLE_HEALTH_REDIRECT_URI
    )

    if "refresh_token" not in body:
        print(
            "\n⚠️  Pas de refresh_token dans la réponse. Si tu as déjà autorisé cette appli"
            " avant, Google ne le renvoie parfois qu'une fois : révoque l'accès sur"
            " https://myaccount.google.com/permissions puis relance ce script."
        )
        sys.exit(1)

    with open(GOOGLE_HEALTH_TOKEN_PATH, "w") as f:
        json.dump(
            {
                "refresh_token": body["refresh_token"],
                "access_token": body["access_token"],
                "expires_in": body.get("expires_in"),
            },
            f,
            indent=2,
        )

    print(f"\n✅ C'est bon ! Token enregistré dans {GOOGLE_HEALTH_TOKEN_PATH}")
    print("Tu peux maintenant matérialiser les assets bronze/google_health_* dans Dagster.")


if __name__ == "__main__":
    main()
