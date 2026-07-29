# Ajouter tes données Google Health (Fitbit Air)

Google a remplacé l'ancienne API Fitbit Web par une nouvelle API cloud,
**Google Health API** (`health.googleapis.com/v4`), en OAuth2 standard.
L'ancienne API Fitbit ferme en septembre 2026 — ce connecteur vise la nouvelle
d'emblée.

## 1. Créer le client OAuth (Google Cloud Console)

1. Va sur [console.cloud.google.com](https://console.cloud.google.com), crée
   un projet (ou réutilise un projet perso existant).
2. **APIs & Services → Library** → cherche "Google Health API" → **Enable**.
3. **APIs & Services → OAuth consent screen** :
   - Type **External**, statut **Testing** (suffisant en perso, pas besoin de
     validation Google tant que tu restes sous 100 utilisateurs).
   - Renseigne juste le nom de l'appli + ton email (contact + support).
   - Dans **Test users**, ajoute ton propre compte Google (celui relié à ta
     Fitbit Air).
   - Dans **Scopes**, ajoute :
     - `.../auth/googlehealth.activity_and_fitness.readonly`
     - `.../auth/googlehealth.health_metrics_and_measurements.readonly`
     - `.../auth/googlehealth.sleep.readonly`
4. **APIs & Services → Credentials → Create Credentials → OAuth client ID** :
   - Type **Desktop app** (pas "Web application" — évite d'avoir à déclarer
     une URI de redirection publique).
   - Note le **Client ID** et le **Client Secret** générés.

## 2. Mettre tes clés dans .env

```bash
cp .env.example .env   # si pas déjà fait
open -e .env
```

Colle `GOOGLE_HEALTH_CLIENT_ID` / `GOOGLE_HEALTH_CLIENT_SECRET`.

## 3. Autoriser l'accès (une seule fois)

```bash
.venv/bin/python scripts/google_health_auth.py
```

Même flow que Withings : le script affiche un lien, tu l'ouvres, tu te
connectes avec le compte Google relié à ta Fitbit Air, tu autorises. Le
navigateur atterrit ensuite sur une page "site inaccessible" (normal) — copie
l'URL complète et colle-la dans le terminal. Le token est enregistré dans
`data/google_health_token.json`.

## 4. Charger les données

```bash
.venv/bin/dagster dev
```

Sur http://localhost:3000 → matérialise les assets `bronze/google_health_*`
(ou **Materialize all**).

## Ce qui est chargé (v1)

4 types de données, ceux dont Google documente le schéma exact publiquement
au moment où ce connecteur a été écrit : **pas**, **fréquence cardiaque**,
**sommeil**, **poids**. Google Health en expose une quinzaine au total
(distance, SpO2, VO2 max, HRV, glycémie...) — pour en ajouter un, complète
`data_platform/ingestion/google_health.py` (une fonction `parse_xxx()` +
l'entrée dans `google_health_resources()`) une fois son schéma JSON exact
vérifié sur un vrai appel API (`data_platform/ingestion/google_health.py`
n'a jamais été testé contre un vrai compte, seulement contre les exemples de
la doc officielle — si un type casse au premier run, regarde la structure
JSON réelle renvoyée et ajuste `parse_xxx()` en conséquence).

## En cas de souci

- « Aucun token Google Health trouvé » → tu n'as pas encore fait l'étape 3.
- « Pas de refresh_token dans la réponse » → Google ne le renvoie parfois
  qu'au tout premier consentement. Révoque l'accès sur
  [myaccount.google.com/permissions](https://myaccount.google.com/permissions)
  puis relance `scripts/google_health_auth.py`.
- Contrairement à Withings, le refresh_token Google ne tourne pas à chaque
  usage — pas besoin de le re-sauvegarder à chaque run.
