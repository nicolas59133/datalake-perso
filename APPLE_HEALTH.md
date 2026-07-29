# Ajouter tes données Apple Health

Apple n'expose aucune API publique pour Apple Health : l'export est **manuel**,
depuis l'iPhone. Une fois le fichier déposé, l'ingestion tourne comme les
autres sources (Dagster/dlt), mais il faut refaire l'export à la main quand tu
veux des données plus récentes (pas de rafraîchissement automatique quotidien
possible sans app tierce payante type "Health Auto Export").

## 1. Exporter depuis l'iPhone

Dans l'app **Santé** : icône de profil (en haut à droite) → tout en bas,
**Exporter toutes les données de santé** → confirme. L'export peut prendre
plusieurs minutes (plus ton historique est long). iOS propose ensuite de
partager le zip généré : **AirDrop** vers ce Mac est le plus simple.

## 2. Décompresser au bon endroit

Le zip contient un dossier `apple_health_export/` avec `export.xml` dedans
(et `export_cda.xml`, ignoré ici). Décompresse-le pour obtenir :

```
data/apple_health_export/export.xml
```

(`APPLE_HEALTH_EXPORT_PATH` dans `.env` si tu veux un autre emplacement.)

```bash
# Exemple si le zip est dans Downloads :
unzip ~/Downloads/export.zip -d /tmp/ah && \
mkdir -p data/apple_health_export && \
cp /tmp/ah/apple_health_export/export.xml data/apple_health_export/export.xml
```

## 3. Charger les données

```bash
.venv/bin/dagster dev
```

Sur http://localhost:3000 → onglet **Assets** → matérialise les 3 assets
`bronze / health_records`, `bronze / health_workouts`,
`bronze / health_activity_summary` (ou **Materialize all**). Le fichier
export.xml peut faire plusieurs centaines de Mo pour plusieurs années de
suivi Apple Watch — le premier chargement peut prendre une minute ou deux
(parsing en streaming, pas de souci mémoire).

## Ce qui est chargé

- **bronze.health_records** — TOUT ce que contient l'export, une ligne par
  mesure Apple (pas, fréquence cardiaque, sommeil, tension, VO2max...). Il y a
  des dizaines de `type` différents (`HKQuantityTypeIdentifier...`,
  `HKCategoryTypeIdentifier...`) ; consultable tel quel dans l'UI DuckDB.
- **bronze.health_workouts** — séances de sport.
- **bronze.health_activity_summary** — anneaux d'activité (énergie, exercice,
  debout), un par jour.
- **silver.health_daily** — les métriques les plus courantes pivotées en une
  ligne par jour (pas, distance, fréquence cardiaque, sommeil). Pour ajouter
  un autre type HealthKit à ce pivot (tension, VO2max...), édite
  `dbt_project/models/silver/health_daily.sql`.

## Remettre à jour plus tard

Refais un export (étape 1), remplace `data/apple_health_export/export.xml`,
relance `./scripts/refresh.sh` (ou remets ça dans Dagster). Le `merge` dlt
dédoublonne automatiquement entre deux exports qui se chevauchent.

## En cas de souci

- « Aucun export Apple Health trouvé » → tu n'as pas encore fait l'étape 1/2.
- Un type de mesure qui t'intéresse n'apparaît pas dans `silver.health_daily`
  → il est quand même dans `bronze.health_records` (filtre sur `type`), le
  pivot ne reprend que les types les plus courants.
