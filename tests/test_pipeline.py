import subprocess
import sys
from pathlib import Path

from data_platform.ingestion.weather import parse_open_meteo
from data_platform.config import ROOT

DBT_PROJECT_DIR = ROOT / "dbt_project"
DBT_BIN = str(Path(sys.executable).parent / "dbt")


def test_parse_open_meteo():
    payload = {
        "daily": {
            "time": ["2026-07-25", "2026-07-26"],
            "temperature_2m_max": [24.1, 26.3],
            "temperature_2m_min": [14.0, 15.2],
            "precipitation_sum": [0.0, 2.4],
            "pressure_msl_mean": [1013.2, 1009.8],
        }
    }
    rows = parse_open_meteo(payload, 50.6292, 3.0573)
    assert len(rows) == 2
    assert rows[0]["date"] == "2026-07-25"
    assert rows[1]["temp_max"] == 26.3
    assert rows[0]["pressure_msl"] == 1013.2
    assert rows[0]["latitude"] == 50.6292


def test_dbt_project_parses():
    """La transfo silver (temp_range, colonnes withings...) vit dans les
    modèles dbt (dbt_project/models/silver/) et est couverte par les tests
    dbt (dbt_project/models/silver/schema.yml), pas par des tests Python.
    Ici on vérifie juste que le projet compile (SQL + Jinja valides)."""
    result = subprocess.run(
        [DBT_BIN, "parse", "--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_withings_signature_deterministic():
    from data_platform.ingestion.withings import _sign
    s1 = _sign(["getnonce", "cid123", 1700000000], "secret")
    s2 = _sign(["getnonce", "cid123", 1700000000], "secret")
    assert s1 == s2 and len(s1) == 64  # hex sha256


def test_withings_parse_measures():
    from data_platform.ingestion.withings import parse_measures
    payload = {
        "status": 0,
        "body": {
            "measuregrps": [
                {"grpid": 111, "date": 1751000000,
                 "measures": [{"value": 70500, "type": 1, "unit": -3},
                              {"value": 155, "type": 6, "unit": -1}]},
                {"grpid": 222, "date": 1751100000,
                 "measures": [{"value": 620, "type": 11, "unit": -1}]},
            ]
        },
    }
    rows = parse_measures(payload)
    assert rows[0]["grpid"] == 111
    assert rows[0]["poids_kg"] == 70.5          # 70500 * 10**-3
    assert rows[0]["taux_graisse_pct"] == 15.5  # 155 * 10**-1
    assert rows[1]["pouls"] == 62.0             # 620 * 10**-1
    assert rows[0]["date"] is not None
