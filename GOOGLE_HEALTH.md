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

## Ce qui est chargé

**13 types de données**, tous ceux qui avaient de vraies données sur un vrai
compte testé (Fitbit Air) au 2026-07-30, sur la vingtaine que Google Health
expose. Absents/vides sur ce compte (tracker basique, sans écran ni capteurs
avancés) : altitude, floors, VO2 max, glycémie. Accès refusé (403, scope non
demandé) : nutrition-log, hydration-log, electrocardiogram.

Chaque type suit l'une de ces formes (voir le docstring en tête de
`data_platform/ingestion/google_health.py` pour le détail) :

| Forme | Types | Traitement |
|---|---|---|
| interval (minute par minute) | steps, distance, active-zone-minutes, sedentary-period, activity-level | pagination brute, agrégé par jour en SQL (dbt) |
| instant (échantillon ponctuel) | weight, body-fat | pagination brute |
| rollup (échantillonné en continu) | heart-rate | `dataPoints:dailyRollUp` — agrégats serveur avg/min/max/jour, PAS les points bruts |
| daily-native (déjà 1 ligne/jour côté API) | daily-resting-heart-rate, daily-oxygen-saturation, daily-heart-rate-variability | GET direct, pas d'agrégation nécessaire |
| session (événement avec durée) | sleep, exercise | pagination brute, silver dédié (pas pivoté dans le daily) |

Points relevés en testant contre un vrai compte :

- `steps` et `heart-rate` n'ont **pas** de champ `dataPoint["name"]` en
  pratique, contrairement à l'exemple "exercise" de la doc publique (seuls
  `sleep`/`weight`/etc. en ont un) — `google_health.py::_data_point_id()` gère
  un fallback (hash déterministe des champs pertinents) pour ces cas.
- `weight` et `body-fat` peuvent provenir d'une balance Withings synchronisée
  dans Google Health (`dataSource.platform = "HEALTH_KIT"`, `device.
  manufacturer = "Withings"`) — mêmes valeurs que dans
  `bronze.withings_measures`, donc doublon entre les deux sources si tu as
  les deux actives (pas dédoublonné automatiquement entre sources
  différentes, seulement au sein d'une même source via `merge`).
- `heart-rate` est échantillonné en continu (**~500k points vus pour 1 seul
  mois** d'usage, un point toutes les ~5s) — bien trop volumineux pour tout
  charger en brut, d'où le rollup. Sur les 90 derniers jours par défaut
  (`ROLLUP_LOOKBACK_DAYS` dans `google_health.py`). L'historique intraday
  brut (heure par heure) n'est pas conservé.
- `daily-resting-heart-rate`, `daily-oxygen-saturation` et
  `daily-heart-rate-variability` sont des métriques Google **différentes**
  de la moyenne/min/max FC de la journée entière (`heart-rate` rollup) —
  respectivement le pouls au repos spécifiquement, la SpO2, et la
  variabilité — pas des doublons.

Silver : `silver.google_health_daily` (une ligne par jour, 18 colonnes —
pas, distance, activité, FC, FC repos, HRV, SpO2, sommeil, poids, masse
grasse) + `silver.google_health_exercise` (une ligne par séance de sport,
pas pivoté par jour car plusieurs séances possibles le même jour).

Pour ajouter un type supplémentaire (VO2 max si tu changes de tracker,
glycémie si tu branches un CGM...) : vérifie d'abord son schéma JSON réel par
un appel direct (la doc publique s'est révélée incomplète/imprécise, voir
CLAUDE.md), regarde s'il rentre dans une des formes ci-dessus (réutilise
`_parse_interval_metric()`/`_parse_instant_metric()`/`_parse_daily_native()`
plutôt que d'écrire un nouveau parseur), ajoute-le dans le registre
correspondant (`_INTERVAL_METRICS`/`_INSTANT_METRICS`/`_DAILY_NATIVE_METRICS`
dans `google_health.py`) et dans `_GOOGLE_HEALTH_TABLES`
(`data_platform/assets/bronze.py`) — `test_google_health_bronze_specs_match_resources`
vérifie que les deux restent synchronisés.

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
