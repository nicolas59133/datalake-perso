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

4 types de données : **pas**, **fréquence cardiaque**, **sommeil**, **poids**.
Testé contre un vrai compte (Fitbit Air) les 2026-07-29/30 :

- `steps` et `heart-rate` n'ont **pas** de champ `dataPoint["name"]` en
  pratique, contrairement à l'exemple "exercise" de la doc publique (seuls
  `sleep`/`weight` en ont un) — `google_health.py::_data_point_id()` gère un
  fallback (hash déterministe des champs pertinents) pour ces deux types.
- `weight` peut provenir d'une balance Withings synchronisée dans Google
  Health (`dataSource.platform = "HEALTH_KIT"`, `device.manufacturer =
  "Withings"`) — mêmes valeurs que dans `bronze.withings_measures`, donc
  doublon entre les deux sources si tu as les deux actives (pas dédoublonné
  automatiquement entre sources différentes, seulement au sein d'une même
  source via `merge`).
- `heart-rate` est échantillonné en continu (**~500k points vus pour 1 seul
  mois** d'usage, un point toutes les ~5s) — bien trop volumineux pour tout
  charger en brut. Le connecteur utilise l'endpoint serveur
  `dataPoints:dailyRollUp` (agrégats journaliers avg/min/max calculés côté
  API), qui ne renvoie qu'une poignée de lignes au lieu de centaines de
  milliers. Bronze : `bronze.google_health_heart_rate_daily` (une ligne par
  jour), sur les 90 derniers jours par défaut
  (`HEART_RATE_LOOKBACK_DAYS` dans `google_health.py`). L'historique
  intraday brut (heure par heure) n'est pas conservé.

Google Health expose une quinzaine de types au total (distance, SpO2, VO2
max, HRV, glycémie...) — pour en ajouter un, complète
`data_platform/ingestion/google_health.py` (une fonction `parse_xxx()` +
l'entrée dans `google_health_resources()`) après avoir vérifié son schéma
JSON réel (fais un appel direct comme dans l'historique de debug plutôt que
de te fier à la doc publique, qui s'est révélée incomplète/imprécise sur le
champ `name`). Si le type est aussi à haut débit, regarde d'abord s'il
supporte `dataPoints:dailyRollUp` (même patron que heart-rate) avant de
paginer les points bruts.

## En cas de souci

- « Aucun token Google Health trouvé » → tu n'as pas encore fait l'étape 3.
- « Pas de refresh_token dans la réponse » → Google ne le renvoie parfois
  qu'au tout premier consentement. Révoque l'accès sur
  [myaccount.google.com/permissions](https://myaccount.google.com/permissions)
  puis relance `scripts/google_health_auth.py`.
- Contrairement à Withings, le refresh_token Google ne tourne pas à chaque
  usage — pas besoin de le re-sauvegarder à chaque run.
- Si un run échoue (`UnboundColumnException` ou autre), dlt garde le paquet
  extrait en attente et le **rejoue tel quel** au prochain run — donc si tu as
  corrigé le code entre-temps, ça ne suffit pas, le bug réapparaît à
  l'identique. `dlt pipeline google_health drop-pending-packages` ne l'a pas
  vidé de façon fiable en pratique ; le plus sûr est de supprimer directement
  `rm -rf ~/.dlt/pipelines/google_health` avant de relancer.
