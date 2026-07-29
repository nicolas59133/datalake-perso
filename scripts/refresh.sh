#!/usr/bin/env bash
# Rafraîchit tout le lake en une commande, sans ouvrir l'interface Dagster.
# Usage :  ./scripts/refresh.sh
set -euo pipefail
cd "$(dirname "$0")/.."   # se place à la racine du projet, peu importe d'où on l'appelle

if [ ! -d ".venv" ]; then
    echo "Erreur : .venv introuvable. Lance d'abord :"
    echo "  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi

echo "→ Rafraîchissement du lake ($(date '+%Y-%m-%d %H:%M'))"
# `job execute -j refresh_all` (et non `asset materialize --select "*"`, qui
# reconstruit un job éphémère et ignore l'executor_def=in_process_executor de
# definitions.py) — nécessaire pour éviter les écritures concurrentes sur
# DuckDB. Voir CLAUDE.md.
.venv/bin/dagster job execute -m data_platform.definitions -j refresh_all
echo "→ Terminé. Lake à jour : data/datalake.duckdb"
