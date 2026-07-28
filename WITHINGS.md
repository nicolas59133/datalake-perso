# Ajouter tes données Withings

## 1. Mettre tes clés dans .env

À la racine du projet :

```bash
cp .env.example .env
```

Ouvre `.env` (double-clic, ou `open -e .env`) et colle ton **Client ID** et ton
**Client Secret** Withings. Vérifie que `WITHINGS_REDIRECT_URI` correspond bien à
la Callback URI enregistrée chez Withings (ici `http://localhost:3000`).

## 2. Autoriser l'accès (une seule fois)

```bash
.venv/bin/python scripts/withings_auth.py
```

Le script t'affiche un lien. Ouvre-le, connecte-toi, clique **Accepter**. Ton
navigateur affiche alors une page « site inaccessible » (normal). Copie l'**adresse
complète** depuis la barre du navigateur et colle-la dans le terminal. Le script
récupère ton refresh_token et le range dans `data/withings_token.json`
(ce fichier est privé, exclu de Git).

## 3. Charger les données

```bash
.venv/bin/dagster dev
```

Sur http://localhost:3000 → onglet **Assets** → materialise
`bronze / withings_measures`. Puis pour voir tes mesures :

```bash
.venv/bin/python -c "import duckdb; c=duckdb.connect('data/datalake.duckdb'); [print(r) for r in c.execute('select * from bronze.withings_measures order by date').fetchall()]"
```

## En cas de souci

- « Aucun token Withings trouvé » → tu n'as pas encore fait l'étape 2.
- Le token est rafraîchi et re-sauvegardé à chaque run (Withings le fait tourner) :
  c'est géré automatiquement, tu n'as rien à refaire.
