# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Vue d'ensemble

Datalake personnel, 100% local. Dagster orchestre :
- des pipelines `dlt` qui écrivent dans un **unique fichier DuckDB**
  (`data/datalake.duckdb`), couche **bronze** (ingestion brute) ;
- des modèles **dbt** (dbt-duckdb) qui construisent la couche **silver**
  (nettoyage/typage) à partir du bronze.

Quatre sources aujourd'hui : météo (Open-Meteo, aucune config requise),
Withings (mesures corporelles, OAuth2 signé HMAC), Apple Health (export
manuel `export.xml`, pas d'API — voir APPLE_HEALTH.md) et Google Health /
Fitbit Air (OAuth2 standard — voir GOOGLE_HEALTH.md).

## Commandes

### Setup initial
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install --upgrade "mashumaro[msgpack]>=3.22"   # obligatoire, voir "Python 3.14 vs dbt" plus bas
```
Le venv est lié au chemin absolu où il a été créé (shebang de `pip`/`dagster` dans
`.venv/bin`). Si le dossier du projet est déplacé/renommé après coup, ces
commandes cassent silencieusement (`command not found`) — recréer le venv
(`rm -rf .venv && python3 -m venv .venv && pip install -r requirements.txt`).

Si `pip install` échoue avec `SSL: CERTIFICATE_VERIFY_FAILED` (typiquement sur
`dbt-core-experimental-parser`, qui télécharge un wheel prébuilt depuis
GitHub pendant le build) : le Python système (python.org,
`/Library/Frameworks/Python.framework/...`) n'a pas de CA bundle configuré.
Fix : lancer `/Applications/Python 3.x/Install Certificates.command`, ou
`export SSL_CERT_FILE=.venv/lib/python3.14/site-packages/certifi/cacert.pem`
une fois `requests` installé (il tire `certifi`). Seulement nécessaire pour
les installs — `dbt build`/`dagster dev` n'ont pas besoin de réseau donc
tournent sans ce fix une fois les paquets installés.

### Lancer Dagster (UI + daemon + scheduler)
```bash
dagster dev
```
→ http://localhost:3000, onglet **Assets** → **Materialize all**. Le planning
quotidien (6h) est dans l'onglet **Automation**.

### Rafraîchir sans ouvrir l'UI
```bash
./scripts/refresh.sh
```

### Matérialiser un asset précis / en respectant l'executor du job
```bash
dagster asset materialize --select "bronze/withings_measures" -m data_platform.definitions
dagster job execute -m data_platform.definitions -j refresh_all   # tout le pipeline, séquentiel
```

### Lancer/déboguer dbt directement (hors Dagster)
```bash
export DUCKDB_PATH="$PWD/data/datalake.duckdb"   # sinon fallback data/datalake.duckdb relatif au cwd
.venv/bin/dbt build --project-dir dbt_project --profiles-dir dbt_project
.venv/bin/dbt build --project-dir dbt_project --profiles-dir dbt_project --select weather_daily
```
Toujours invoquer depuis la racine du repo (les chemins relatifs de
`dbt_project/profiles.yml` en dépendent).

### Tests
```bash
pytest -q
pytest -q tests/test_pipeline.py::test_parse_open_meteo   # un seul test
```
`[tool.pytest.ini_options] pythonpath = ["."]` dans `pyproject.toml` — sans
ça, `pytest -q` (sans `python -m`) ne trouve pas le package `data_platform`
(pytest n'ajoute pas le cwd à `sys.path`, contrairement à `python -m pytest`).

### Activer Withings (une fois par machine)
```bash
cp .env.example .env   # coller WITHINGS_CLIENT_ID / WITHINGS_CLIENT_SECRET
                        # (créés sur developer.withings.com, callback = http://localhost:3000)
.venv/bin/python scripts/withings_auth.py   # flow OAuth interactif -> data/withings_token.json
```
Ensuite l'ingestion rafraîchit le token toute seule à chaque run (Withings fait
tourner le refresh_token, `withings.py` le re-sauvegarde automatiquement).

### Activer Google Health / Fitbit Air (une fois par machine)
```bash
cp .env.example .env   # coller GOOGLE_HEALTH_CLIENT_ID / GOOGLE_HEALTH_CLIENT_SECRET
                        # (créés dans Google Cloud Console, client OAuth type "Desktop app")
.venv/bin/python scripts/google_health_auth.py   # flow OAuth interactif -> data/google_health_token.json
```
Contrairement à Withings, le refresh_token Google ne tourne pas à chaque
usage (pas de resauvegarde nécessaire à chaque run).

### Visualiser les données (UI DuckDB)
```bash
.venv/bin/python scripts/duckdb_ui.py
```
→ http://localhost:4213. Se branche sur la copie `data/datalake_view.duckdb`
(jamais le fichier live), donc peut rester ouvert indéfiniment sans bloquer
les runs Dagster — voir "Contrainte : DuckDB n'autorise qu'un seul writer".

## Architecture

- `data_platform/config.py` — charge `.env` et centralise tous les settings
  (`DUCKDB_PATH`, coordonnées météo, credentials Withings/Google Health,
  chemin d'export Apple Health). Le reste du code importe toujours depuis ce
  module plutôt que de lire `os.getenv` directement.
- `data_platform/ingestion/*.py` — resources `dlt` (`@dlt.resource`). Le parsing
  est volontairement séparé de l'appel réseau (`parse_open_meteo`,
  `parse_measures`) pour rester testable sans requête HTTP.
- `data_platform/assets/bronze.py` — un asset Dagster par source. Chaque asset
  instancie **son propre** `dlt.pipeline(destination=duckdb(DUCKDB_PATH),
  dataset_name="bronze")` et écrit dans le schéma `bronze`.
- `dbt_project/` — modèles SQL de la couche silver (`models/silver/*.sql`),
  déclarant les tables bronze comme `source()` (`models/sources.yml`). Schéma
  cible = `silver` (profiles.yml), donc `models/silver/weather_daily.sql`
  produit `silver.weather_daily` (pas de préfixe grâce à l'override standard
  `macros/generate_schema_name.sql`). Tests dbt (`not_null`/`unique`) dans
  `models/silver/schema.yml`.
- `data_platform/assets/silver.py` — **PAS** l'intégration `dagster-dbt`
  (voir contrainte ci-dessous). Deux `@dg.multi_asset` séparés, chacun lançant
  un `dbt build --select <modèles du groupe>` en subprocess : `_core`
  (weather_daily/withings_measures/google_health_daily, sources fiables) et
  `_apple_health` (health_daily/health_workouts/health_activity_summary,
  dépend de l'export manuel). Scindé en deux **exprès** : un seul
  `@dg.multi_asset` est tout-ou-rien (un modèle qui échoue dans le `dbt
  build` fait échouer TOUT le step Dagster, y compris les modèles qui ont
  réellement réussi côté DB) — avec un seul groupe, tant qu'Apple Health
  n'est pas configuré, weather_daily/withings_measures/google_health_daily
  apparaissaient à tort en échec dans l'UI Dagster alors que leurs tables
  étaient bien à jour. `ops/duckdb_view_snapshot` ne dépend que du groupe
  `_core` pour la même raison (toujours tourner même si Apple Health échoue).
- `data_platform/definitions.py` — point d'entrée Dagster, référencé par
  `pyproject.toml` (`[tool.dagster] module_name`). Assemble les assets, le job
  `refresh_all` et le schedule quotidien.
- `data_platform/ingestion/withings.py` — implémente la signature
  HMAC-SHA256 + nonce exigée par l'API Withings pour chaque appel signé
  (`getnonce`, `requesttoken`) ; `scripts/withings_auth.py` ne sert qu'au flow
  OAuth initial dans le navigateur.
- `data_platform/ingestion/apple_health.py` — parse `export.xml` en streaming
  (`ET.iterparse` + `elem.clear()`, pas de DOM complet en mémoire : l'export
  peut faire plusieurs centaines de Mo). Un seul passage alimente 3 tables
  (`health_records`/`health_workouts`/`health_activity_summary`) ; l'asset
  bronze (`bronze_apple_health`, multi_asset) appelle `apple_health_resources()`
  une fois et passe les 3 `dlt.resource(...)` à un seul `pipeline.run([...])`
  pour ne pas reparser le fichier 3 fois. Comme Apple ne fournit pas d'ID
  stable par enregistrement, `record_id`/`workout_id` sont des hash SHA1
  déterministes (type/source/dates/valeur) — le `merge` dlt dédoublonne bien
  entre deux exports qui se chevauchent tant que ces champs ne changent pas.
- `data_platform/ingestion/google_health.py` — OAuth2 standard (auth
  `accounts.google.com`, token `oauth2.googleapis.com`, contrairement au HMAC
  maison de Withings). `google_health_resources()` récupère UN access_token
  et l'utilise pour les 4 appels API, pas un refresh par type. Testé contre
  un vrai compte (Fitbit Air) le 2026-07-29/30 :
  - Contrairement à ce que suggérait l'exemple "exercise" de la doc publique,
    `steps` et `heart-rate` n'ont **pas** de `dataPoint["name"]` en pratique
    (seuls `sleep`/`weight` en ont un) -> `_data_point_id()` retombe sur un
    hash déterministe pour ces deux types, même patron que
    record_id/workout_id dans apple_health.py.
  - `heart-rate` est échantillonné en continu (~500k points/mois vus en
    test) : au lieu de paginer les points bruts (`list_data_points`), on
    utilise l'endpoint serveur `dataPoints:dailyRollUp`
    (`list_daily_rollup()`) qui renvoie directement des agrégats
    avg/min/max **par jour** — quelques dizaines de lignes au lieu de
    centaines de milliers. Plafonné à `chunk_days=14` par requête (limite
    observée côté API pour heart-rate, `INVALID_ROLLUP_QUERY_DURATION` sinon
    — un autre type pourrait avoir un plafond différent). Bronze :
    `google_health_heart_rate_daily` (primary_key=`date`), pas
    `google_health_heart_rate`.
  - Piège dlt rencontré : si un run échoue, dlt rejoue le paquet extrait tel
    quel au run suivant **même si le code a été corrigé entre-temps** —
    `rm -rf ~/.dlt/pipelines/google_health` avant de relancer si ça arrive
    (`dlt pipeline <name> drop-pending-packages` ne l'a pas vidé de façon
    fiable en pratique).

### Contrainte : Python 3.14 vs écosystème dbt

Cette machine n'a que Python 3.14 (pas de pyenv/Homebrew pour en installer un
autre). Deux blocages rencontrés, à garder en tête si on retouche `dbt_project/`
ou les deps dbt :
- **`dagster-dbt` (intégration officielle) est inutilisable ici.** Sa dernière
  version (0.28.8) épingle `dagster==1.12.8` exact, qui ne supporte pas
  Python 3.14 (seul `dagster>=1.12.9` le supporte). D'où le wrapper
  `subprocess` manuel dans `assets/silver.py` plutôt que `@dbt_assets`. À
  réévaluer si une version plus récente de `dagster-dbt` sort.
- **`mashumaro` (dépendance de dbt-core/dbt-adapters/dbt-common) a un bug
  interne sur Python 3.14** : sa propre classe `JSONObjectSchema` (utilisée
  pour générer le manifest JSON schema) ne compile pas
  (`UnserializableField: Field "schema"...`), quelle que soit la version de
  dbt-core testée (1.5 à 1.12). dbt-core plafonne `mashumaro<3.18`, mais ce
  plafond casse tout import dbt sur cette machine ; la version installée
  (`>=3.22`, forcée en `pip install --upgrade` séparé après
  `requirements.txt`, impossible à exprimer dans le même fichier — voir
  commentaire dans `requirements.txt`) fonctionne malgré l'avertissement de
  conflit pip.
- Une install `pip` "propre" de `dagster-dbt` sans épingler `dagster` fait
  **downgrader dagster à 1.6.6** (incompatible avec `dagster-webserver`
  1.13.15) — toujours fixer les versions du trio
  `dagster`/`dagster-webserver`/`dagster-graphql` ensemble si on retouche ces
  deps.

### Contrainte : DuckDB n'autorise qu'un seul writer

Les assets bronze n'ont pas de dépendance entre eux, donc Dagster peut vouloir
les exécuter en parallèle — ce qui produit `IO Error: Could not set lock on
file datalake.duckdb` puisque DuckDB refuse les écritures concurrentes sur le
même fichier (même souci pour le subprocess `dbt build` de `silver.py` s'il
tournait en même temps qu'un asset bronze). Le job `refresh_all`
(`definitions.py`) fixe donc `executor_def=dg.in_process_executor` pour
forcer une exécution séquentielle. Piège : `dagster asset materialize
--select "..."` construit un job éphémère qui **ignore cet executor_def** et
retombe sur le multiprocess executor par défaut — la collision reste donc
possible par ce chemin (`scripts/refresh.sh` utilise donc `dagster job
execute -j refresh_all`, pas `asset materialize`).

Autre source de collision, définitivement réglée : l'UI DuckDB locale
(`scripts/duckdb_ui.py`) ouvre une connexion persistante — testé empiriquement
(voir historique), même une connexion **read-only** d'un autre process bloque
un writer DuckDB, donc "juste ouvrir l'UI en lecture seule" ne suffit pas.
Solution : l'UI ne se branche jamais sur `data/datalake.duckdb` (le fichier
écrit par le pipeline) mais sur une **copie dédiée**, `DUCKDB_VIEW_PATH`
(`data/datalake_view.duckdb`), régénérée par l'asset
`ops/duckdb_view_snapshot` (dernier step de `refresh_all`, dépend de tous les
assets silver). L'UI peut donc rester ouverte indéfiniment sans jamais
bloquer un run — au prix d'une vue figée au dernier `refresh_all` plutôt que
vraiment live.

## Sécurité / données sensibles

- Les secrets (clés/tokens Withings, Google Health, futures clés d'API) vont
  **uniquement** dans `.env`, jamais en dur dans le code. `config.py` est le
  seul point de lecture (`os.getenv`) ; tout le reste du code importe ses
  valeurs depuis là.
- `.env` (clés Withings/Google Health) et tout `data/` (lake DuckDB, tokens,
  export Apple Health) sont exclus de Git via `.gitignore`, en liste blanche :
  **tout** `data/*` est ignoré par défaut, sauf ajout explicite
  (`!data/mon_fichier`).
- Ne jamais `git add -f` un fichier dans `data/`.
