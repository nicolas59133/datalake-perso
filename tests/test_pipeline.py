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


APPLE_HEALTH_EXPORT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="fr_FR">
  <Record type="HKQuantityTypeIdentifierStepCount" sourceName="iPhone" unit="count"
          creationDate="2026-01-01 08:00:00 +0100" startDate="2026-01-01 07:00:00 +0100"
          endDate="2026-01-01 08:00:00 +0100" value="1200"/>
  <Record type="HKQuantityTypeIdentifierStepCount" sourceName="iPhone" unit="count"
          creationDate="2026-01-01 12:00:00 +0100" startDate="2026-01-01 11:00:00 +0100"
          endDate="2026-01-01 12:00:00 +0100" value="800"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" sourceName="Watch" unit="count/min"
          creationDate="2026-01-01 09:00:00 +0100" startDate="2026-01-01 09:00:00 +0100"
          endDate="2026-01-01 09:00:00 +0100" value="62"/>
  <Record type="HKCategoryTypeIdentifierSleepAnalysis" sourceName="Watch" unit=""
          creationDate="2026-01-01 07:00:00 +0100" startDate="2026-01-01 00:00:00 +0100"
          endDate="2026-01-01 01:30:00 +0100" value="HKCategoryValueSleepAnalysisAsleepCore"/>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning" sourceName="Watch"
           creationDate="2026-01-02 07:30:00 +0100" startDate="2026-01-02 07:00:00 +0100"
           endDate="2026-01-02 07:30:00 +0100" totalDistance="5.2" totalDistanceUnit="km"
           totalEnergyBurned="320" totalEnergyBurnedUnit="Cal"/>
  <ActivitySummary dateComponents="2026-01-01" activeEnergyBurned="450" activeEnergyBurnedGoal="500"
                    appleExerciseTime="35" appleExerciseTimeGoal="30"
                    appleStandHours="10" appleStandHoursGoal="12"/>
</HealthData>
"""


def test_parse_apple_health_export(tmp_path):
    from data_platform.ingestion.apple_health import parse_apple_health_export, _record_id

    xml_path = tmp_path / "export.xml"
    xml_path.write_text(APPLE_HEALTH_EXPORT_XML)

    records, workouts, activity = parse_apple_health_export(str(xml_path))

    assert len(records) == 4
    steps = [r for r in records if r["type"] == "HKQuantityTypeIdentifierStepCount"]
    assert sum(r["value"] for r in steps) == 2000

    sleep = next(r for r in records if r["type"] == "HKCategoryTypeIdentifierSleepAnalysis")
    assert sleep["value"] is None            # catégoriel, pas numérique
    assert sleep["value_text"] == "HKCategoryValueSleepAnalysisAsleepCore"

    # record_id déterministe -> le merge dlt dédoublonne bien entre deux imports.
    assert _record_id(steps[0]) == _record_id(steps[0])
    assert _record_id(steps[0]) != _record_id(steps[1])

    assert len(workouts) == 1
    assert workouts[0]["workout_type"] == "HKWorkoutActivityTypeRunning"
    assert workouts[0]["total_distance_km"] == 5.2
    assert workouts[0]["total_energy_kcal"] == 320

    assert len(activity) == 1
    assert activity[0]["date"] == "2026-01-01"
    assert activity[0]["active_energy_kcal"] == 450
    assert activity[0]["stand_hours"] == 10


def test_parse_apple_health_export_missing_file():
    from data_platform.ingestion.apple_health import apple_health_resources

    try:
        apple_health_resources("/tmp/does-not-exist-export.xml")
        assert False, "aurait dû lever RuntimeError"
    except RuntimeError as e:
        assert "APPLE_HEALTH.md" in str(e)


def test_google_health_parse_steps():
    from data_platform.ingestion.google_health import parse_steps

    data_points = [
        {
            "name": "users/u1/dataTypes/steps/dataPoints/p1",
            "dataSource": {"platform": "FITBIT"},
            "steps": {
                "interval": {
                    "startTime": "2026-01-01T07:00:00Z",
                    "startUtcOffset": "3600s",
                    "endTime": "2026-01-01T08:00:00Z",
                },
                "count": "1200",
            },
        }
    ]
    rows = parse_steps(data_points)
    assert rows[0]["data_point_id"] == "users/u1/dataTypes/steps/dataPoints/p1"
    assert rows[0]["count"] == 1200
    assert rows[0]["start_utc_offset_s"] == 3600


def test_google_health_parse_heart_rate_and_weight():
    from data_platform.ingestion.google_health import parse_heart_rate, parse_weight

    hr_points = [
        {
            "name": "p2",
            "dataSource": {"platform": "FITBIT"},
            "heartRate": {
                "sampleTime": {"physicalTime": "2026-01-01T09:00:00Z", "utcOffset": "-18000s"},
                "beatsPerMinute": "62",
            },
        }
    ]
    rows = parse_heart_rate(hr_points)
    assert rows[0]["beats_per_minute"] == 62
    assert rows[0]["sample_utc_offset_s"] == -18000

    weight_points = [
        {
            "name": "p3",
            "dataSource": {"platform": "FITBIT"},
            "weight": {
                "sampleTime": {"physicalTime": "2026-01-01T07:00:00Z"},
                "weightGrams": "70500",
            },
        }
    ]
    rows = parse_weight(weight_points)
    assert rows[0]["weight_kg"] == 70.5


def test_google_health_missing_token():
    from data_platform.ingestion.google_health import _load_token
    import data_platform.ingestion.google_health as gh

    original = gh.GOOGLE_HEALTH_TOKEN_PATH
    try:
        gh.GOOGLE_HEALTH_TOKEN_PATH = "/tmp/does-not-exist-google-health-token.json"
        try:
            _load_token()
            assert False, "aurait dû lever RuntimeError"
        except RuntimeError as e:
            assert "GOOGLE_HEALTH.md" in str(e)
    finally:
        gh.GOOGLE_HEALTH_TOKEN_PATH = original
