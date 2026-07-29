# Datalake perso — Dagster + dlt + DuckDB

Point de départ de la plateforme data, en **local sur ton ordi**. Deux sources —
météo (Open-Meteo, aucune config) et Withings (mesures corporelles, OAuth) —
rangées en couches **bronze → silver** dans un fichier **DuckDB**, le tout
**orchestré et lancé par Dagster**. La couche silver est faite de modèles
**dbt** (dbt-duckdb).

## Ce qu'il y a dedans

```
data_platform/
  config.py                 # chemin DuckDB + localisation (réglable par .env)
  ingestion/
    weather.py              # source météo (dlt) — marche tout de suite
    withings.py             # source Withings (OAuth signé HMAC)
  assets/
    bronze.py               # assets Dagster : ingestion brute -> DuckDB (schéma bronze)
    silver.py               # asset Dagster qui lance `dbt build` (schéma silver)
  definitions.py            # assets + job + planning quotidien
dbt_project/                # modèles dbt (SQL) qui construisent la couche silver
tests/                      # tests unitaires (parsing + transfo)
pyproject.toml              # dit à Dagster où trouver les définitions
```

Le flux : `Dagster (assets bronze) → dlt → DuckDB.bronze` puis
`Dagster (asset silver) → dbt build → DuckDB.silver`. Dagster gère l'ordre, le
planning (tous les jours à 6h), l'observabilité et la lignée entre les couches.

## Démarrer (3 minutes)

```bash
# 1. Environnement
python3 -m venv .venv
source .venv/bin/activate            # Windows : .venv\Scripts\activate
pip install -r requirements.txt
pip install --upgrade "mashumaro[msgpack]>=3.22"   # obligatoire sur Python 3.14, voir CLAUDE.md

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

## Ajouter une source

1. Copie `.env.example` en `.env` et mets tes secrets (jamais dans le code).
2. Implémente la resource dans `data_platform/ingestion/<source>.py`, calquée
   sur `weather.py` ou `withings.py`.
3. Crée un asset bronze dans `assets/bronze.py`.
4. Ajoute un modèle dbt dans `dbt_project/models/silver/` qui lit
   `{{ source('bronze', '<table>') }}`, déclare la table dans
   `dbt_project/models/sources.yml`, et ajoute son `AssetSpec` (avec `deps`
   vers l'asset bronze) dans `assets/silver.py`.

Withings est déjà actif — voir **WITHINGS.md** pour l'auth.

## Prochaines étapes suggérées

- **dagster-dbt** pour remplacer le wrapper `subprocess` de `assets/silver.py`
  par l'intégration officielle (asset par modèle auto-généré depuis le
  manifest dbt) dès qu'une version compatible avec Python 3.14 sort — voir
  CLAUDE.md pour le blocage actuel.
- Un modèle **gold** (dbt) si des agrégats/jointures entre sources deviennent utiles.
- **Cloudflare R2 + Parquet/Iceberg** quand tu veux sortir le stockage de l'ordi.
- **MotherDuck / BigQuery** le jour où le volume dépasse ta machine — mêmes modèles.

> Rappel données sensibles : santé + banque = secrets hors du code (.env), et
> `data/*.duckdb` est déjà exclu de Git via `.gitignore`.
