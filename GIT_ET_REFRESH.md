# Mettre le projet sur Git + rafraîchir facilement

## ⚠️ D'abord, le point sécurité (à lire une fois)

Ce projet contient des données de santé et des clés d'API. Deux fichiers ne
doivent **jamais** partir sur GitHub :

- `.env` → tes clés Withings
- `data/` → ton lake DuckDB et ton token Withings

Le `.gitignore` du projet les bloque déjà **automatiquement** (testé), et le
dossier `data/` est ignoré en entier par défaut. Tu n'as rien à faire de
spécial — juste ne jamais forcer l'ajout d'un fichier dans `data/` avec `git
add -f`.

**Deuxième règle : mets le dépôt GitHub en "Private".** Même sans secrets
dedans, c'est ton infrastructure perso — pas de raison de la rendre publique.

## Créer le dépôt (une seule fois)

**1. Sur GitHub** (dans ton navigateur) : bouton **New repository** → nom
`datalake-perso` (ou ce que tu veux) → **Private** → ne coche RIEN d'autre
(pas de README, pas de .gitignore : on les a déjà) → **Create repository**.
GitHub t'affiche une page avec des commandes — garde-la ouverte, on en a besoin.

**2. Dans le Terminal**, à la racine du projet :
```bash
git init
git add .
git commit -m "Premier commit : lake perso (météo + Withings)"
```
Avant le commit, tu peux vérifier ce qui va partir :
```bash
git status
```
Tu dois voir uniquement du code (`data_platform/`, `scripts/`, `README.md`…),
**jamais** `.env` ni rien dans `data/`. Si tu vois l'un des deux, arrête-toi et
dis-le-moi avant de continuer.

**3. Relier au dépôt GitHub** (remplace l'URL par celle de TA page GitHub,
affichée à l'étape 1) :
```bash
git remote add origin https://github.com/TON-PSEUDO/datalake-perso.git
git branch -M main
git push -u origin main
```

## Rafraîchir tes données au quotidien

Plus besoin d'ouvrir l'interface Dagster à chaque fois. Une seule commande,
qui matérialise tous les assets (météo + Withings) d'un coup :

```bash
./scripts/refresh.sh
```

Ça fait exactement ce que faisait le bouton **Materialize all** dans
l'interface, mais en une ligne, sans navigateur.

Tu peux toujours ouvrir `.venv/bin/dagster dev` quand tu veux **visualiser**
les runs, les logs, ou ajouter un nouvel asset — le script `refresh.sh` est
juste le raccourci pour l'usage courant.

## Versionner tes changements plus tard

À chaque fois que tu modifies le code (nouvelle source, nouvelle transfo) :
```bash
git add .
git status          # vérifie encore une fois ce qui part
git commit -m "Description courte du changement"
git push
```

## Optionnel : automatiser le refresh (sans y penser)

Le planning quotidien (6h du matin) est déjà défini dans
`data_platform/definitions.py`, mais il ne se déclenche que si `dagster dev`
tourne. Pour un vrai automatisme sans rien laisser ouvert, l'étape suivante
serait une tâche planifiée macOS (`launchd`) qui lance `refresh.sh` chaque
jour — dis-moi si tu veux qu'on la mette en place, c'est une config à part.
