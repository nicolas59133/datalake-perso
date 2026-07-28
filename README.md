# Datalake perso — Dagster + dlt + DuckDB

Point de départ de la plateforme data, en **local sur ton ordi**. Une source qui
tourne sans aucune config (météo Open-Meteo), rangée en couches **bronze → silver**
dans un fichier **DuckDB**, le tout **orchestré et lancé par Dagster**.

## Ce qu'il y a dedans

```
data_platform/
  config.py                 # chemin DuckDB + localisation (réglable par .env)
  ingestion/
    weather.py              # source météo (dlt) — marche tout de suite
    withings.py             # squelette Withings (OAuth) à activer plus tard
  assets/
    bronze.py               # asset Dagster : ingestion brute -> DuckDB (schéma bronze)
    silver.py               # asset Dagster : nettoyage/enrichissement (schéma silver)
  definitions.py            # assets + job + planning quotidien
tests/                      # tests unitaires (parsing + transfo)
pyproject.toml              # dit à Dagster où trouver les définitions
```

Le flux : `Dagster (asset bronze) → dlt → DuckDB.bronze` puis
`Dagster (asset silver) → SQL DuckDB → DuckDB.silver`. Dagster gère l'ordre, le
planning (tous les jours à 6h), l'observabilité et la lignée entre les couches.

## Démarrer (3 minutes)

```bash
# 1. Environnement
python3 -m venv .venv
source .venv/bin/activate            # Windows : .venv\Scripts\activate
pip install -r requirements.txt

# 2. Lancer Dagster
dagster dev
```

Ouvre http://localhost:3000 → onglet **Assets** → **Materialize all**.
Ça remplit `data/datalake.duckdb`. (Le planning quotidien est dans l'onglet
**Automation**, à activer quand tu veux.)

## Regarder les données

```bash
python -c "import duckdb; c=duckdb.connect('data/datalake.duckdb'); \
print(c.execute('select * from silver.weather_daily order by date').fetchdf())"
```

## Lancer les tests

```bash
pip install pytest
pytest -q
```

## Mettre sur Git / rafraîchir en une commande

Voir **GIT_ET_REFRESH.md** : mise sur GitHub (en privé, secrets exclus
automatiquement) et `./scripts/refresh.sh` pour recharger tout le lake sans
ouvrir l'interface.

## Ajouter une source (ex. Withings)

1. Copie `.env.example` en `.env` et mets tes secrets (jamais dans le code).
2. Implémente la resource dans `data_platform/ingestion/withings.py`.
3. Crée un asset `bronze_withings` dans `assets/bronze.py`, calqué sur la météo.
4. Il apparaît automatiquement dans Dagster.

## Prochaines étapes suggérées

- **dbt-duckdb** pour remplacer le SQL de silver/gold par des modèles testés.
- **Cloudflare R2 + Parquet/Iceberg** quand tu veux sortir le stockage de l'ordi.
- **MotherDuck / BigQuery** le jour où le volume dépasse ta machine — mêmes modèles.

> Rappel données sensibles : santé + banque = secrets hors du code (.env), et
> `data/*.duckdb` est déjà exclu de Git via `.gitignore`.
