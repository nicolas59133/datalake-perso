# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Vue d'ensemble

Datalake personnel, 100% local. Dagster orchestre des pipelines `dlt` qui écrivent
dans un **unique fichier DuckDB** (`data/datalake.duckdb`), organisé en couches
**bronze** (ingestion brute) → **silver** (nettoyage/typage SQL). Deux sources
aujourd'hui : météo (Open-Meteo, aucune config requise) et Withings (mesures
corporelles, OAuth2 signé HMAC).

## Commandes

### Setup initial
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Le venv est lié au chemin absolu où il a été créé (shebang de `pip`/`dagster` dans
`.venv/bin`). Si le dossier du projet est déplacé/renommé après coup, ces
commandes cassent silencieusement (`command not found`) — recréer le venv
(`rm -rf .venv && python3 -m venv .venv && pip install -r requirements.txt`).

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

### Tests
```bash
pip install pytest
pytest -q
pytest -q tests/test_pipeline.py::test_parse_open_meteo   # un seul test
```
Pas de config pytest dans `pyproject.toml` — un seul fichier `tests/test_pipeline.py`.

### Activer Withings (une fois par machine)
```bash
cp .env.example .env   # coller WITHINGS_CLIENT_ID / WITHINGS_CLIENT_SECRET
                        # (créés sur developer.withings.com, callback = http://localhost:3000)
.venv/bin/python scripts/withings_auth.py   # flow OAuth interactif -> data/withings_token.json
```
Ensuite l'ingestion rafraîchit le token toute seule à chaque run (Withings fait
tourner le refresh_token, `withings.py` le re-sauvegarde automatiquement).

## Architecture

- `data_platform/config.py` — charge `.env` et centralise tous les settings
  (`DUCKDB_PATH`, coordonnées météo, credentials Withings). Le reste du code
  importe toujours depuis ce module plutôt que de lire `os.getenv` directement.
- `data_platform/ingestion/*.py` — resources `dlt` (`@dlt.resource`). Le parsing
  est volontairement séparé de l'appel réseau (`parse_open_meteo`,
  `parse_measures`) pour rester testable sans requête HTTP.
- `data_platform/assets/bronze.py` — un asset Dagster par source. Chaque asset
  instancie **son propre** `dlt.pipeline(destination=duckdb(DUCKDB_PATH),
  dataset_name="bronze")` et écrit dans le schéma `bronze`.
- `data_platform/assets/silver.py` — transformations SQL DuckDB pures
  (`build_silver_weather`) appelées par un asset qui déclare sa dépendance
  bronze via `deps=[dg.AssetKey([...])]`.
- `data_platform/definitions.py` — point d'entrée Dagster, référencé par
  `pyproject.toml` (`[tool.dagster] module_name`). Assemble les assets, le job
  `refresh_all` et le schedule quotidien.
- `data_platform/ingestion/withings.py` — implémente la signature
  HMAC-SHA256 + nonce exigée par l'API Withings pour chaque appel signé
  (`getnonce`, `requesttoken`) ; `scripts/withings_auth.py` ne sert qu'au flow
  OAuth initial dans le navigateur.

### Contrainte : DuckDB n'autorise qu'un seul writer

Les assets bronze n'ont pas de dépendance entre eux, donc Dagster peut vouloir
les exécuter en parallèle — ce qui produit `IO Error: Could not set lock on
file datalake.duckdb` puisque DuckDB refuse les écritures concurrentes sur le
même fichier. Le job `refresh_all` (`definitions.py`) fixe donc
`executor_def=dg.in_process_executor` pour forcer une exécution séquentielle.
Piège : `dagster asset materialize --select "..."` (utilisé par
`scripts/refresh.sh`) construit un job éphémère qui **ignore cet
executor_def** et retombe sur le multiprocess executor par défaut — la
collision reste donc possible par ce chemin. Passer par `dagster job execute
-j refresh_all` la respecte.

## Sécurité / données sensibles

- Les secrets (clés/tokens Withings, futures clés d'API) vont **uniquement**
  dans `.env`, jamais en dur dans le code. `config.py` est le seul point de
  lecture (`os.getenv`) ; tout le reste du code importe ses valeurs depuis là.
- `.env` (clés Withings) et tout `data/` (lake DuckDB + `withings_token.json`)
  sont exclus de Git via `.gitignore`, en liste blanche : **tout** `data/*` est
  ignoré par défaut, sauf ajout explicite (`!data/mon_fichier`).
- Ne jamais `git add -f` un fichier dans `data/`.
