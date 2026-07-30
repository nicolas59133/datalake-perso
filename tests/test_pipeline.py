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


def test_google_health_parse_steps_without_name():
    """En pratique (compte réel testé le 2026-07-29), les dataPoints steps
    n'ont PAS de champ "name" malgré l'exemple "exercise" de la doc
    publique -> fallback hash déterministe."""
    from data_platform.ingestion.google_health import parse_steps

    data_points = [
        {
            "dataSource": {"platform": "FITBIT"},
            "steps": {
                "interval": {"startTime": "2026-01-01T07:00:00Z", "endTime": "2026-01-01T08:00:00Z"},
                "count": "26",
            },
        }
    ]
    rows = parse_steps(data_points)
    assert rows[0]["data_point_id"]
    assert rows[0]["count"] == 26


def test_google_health_parse_heart_rate_daily():
    from data_platform.ingestion.google_health import parse_heart_rate_daily

    rollup_points = [
        {
            "civilStartTime": {"date": {"year": 2026, "month": 7, "day": 29}},
            "civilEndTime": {"date": {"year": 2026, "month": 7, "day": 30}},
            "heartRate": {
                "beatsPerMinuteAvg": 69.65212104463176,
                "beatsPerMinuteMax": 122,
                "beatsPerMinuteMin": 45,
            },
        }
    ]
    rows = parse_heart_rate_daily(rollup_points)
    assert rows[0]["date"] == "2026-07-29"
    assert rows[0]["avg_bpm"] == 69.65212104463176
    assert rows[0]["min_bpm"] == 45
    assert rows[0]["max_bpm"] == 122


def test_google_health_daily_rollup_chunking():
    """list_daily_rollup doit découper une plage > chunk_days en plusieurs
    requêtes (heart-rate plafonne à 14 jours côté API)."""
    from datetime import date
    from unittest.mock import patch, MagicMock
    from data_platform.ingestion.google_health import list_daily_rollup

    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append((json["range"]["start"]["date"], json["range"]["end"]["date"]))
        resp = MagicMock()
        resp.json.return_value = {"rollupDataPoints": [{"civilStartTime": {"date": json["range"]["start"]["date"]}}]}
        resp.raise_for_status.return_value = None
        return resp

    with patch("data_platform.ingestion.google_health.requests.post", side_effect=fake_post):
        points = list_daily_rollup(
            "token", "heart-rate", date(2026, 6, 1), date(2026, 7, 30), chunk_days=14
        )
    assert len(calls) == 5  # 59 jours / 14 -> 5 requêtes (14+14+14+14+3)
    assert len(points) == 5


def test_google_health_parse_weight():
    from data_platform.ingestion.google_health import parse_weight

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


def test_google_health_parse_interval_metric():
    from data_platform.ingestion.google_health import _parse_interval_metric

    data_points = [
        {
            "dataSource": {"platform": "FITBIT"},
            "activeZoneMinutes": {
                "interval": {"startTime": "2026-07-29T16:11:00Z", "endTime": "2026-07-29T16:12:00Z"},
                "heartRateZone": "FAT_BURN",
                "activeZoneMinutes": "1",
            },
        }
    ]
    rows = _parse_interval_metric(data_points, "activeZoneMinutes", "activeZoneMinutes", "heartRateZone")
    assert rows[0]["value"] == 1
    assert rows[0]["category"] == "FAT_BURN"
    assert rows[0]["data_point_id"]


def test_google_health_parse_instant_metric():
    from data_platform.ingestion.google_health import _parse_instant_metric

    data_points = [
        {
            "dataSource": {"platform": "HEALTH_KIT"},
            "bodyFat": {"sampleTime": {"physicalTime": "2026-07-22T03:40:26Z"}, "percentage": 21.545},
        }
    ]
    rows = _parse_instant_metric(data_points, "bodyFat", "percentage")
    assert rows[0]["value"] == 21.545


def test_google_health_parse_daily_native():
    from data_platform.ingestion.google_health import _parse_daily_native

    data_points = [
        {
            "dailyRestingHeartRate": {
                "date": {"year": 2026, "month": 7, "day": 30},
                "beatsPerMinute": "62",
            }
        }
    ]
    rows = _parse_daily_native(data_points, "dailyRestingHeartRate", {"resting_bpm": "beatsPerMinute"})
    assert rows[0]["date"] == "2026-07-30"
    assert rows[0]["resting_bpm"] == 62.0


def test_google_health_parse_exercise():
    from data_platform.ingestion.google_health import parse_exercise

    data_points = [
        {
            "dataSource": {"platform": "FITBIT"},
            "exercise": {
                "interval": {"startTime": "2026-07-28T05:42:43Z", "endTime": "2026-07-28T06:15:20Z"},
                "exerciseType": "TREADMILL",
                "displayName": "Tapis de course",
                "activeDuration": "1954s",
                "metricsSummary": {
                    "caloriesKcal": 403,
                    "distanceMillimeters": 2409648,
                    "steps": "3434",
                    "averageHeartRateBeatsPerMinute": "138",
                    "activeZoneMinutes": "57",
                },
            },
        }
    ]
    rows = parse_exercise(data_points)
    assert rows[0]["exercise_type"] == "TREADMILL"
    assert rows[0]["calories_kcal"] == 403
    assert rows[0]["distance_m"] == 2409.648
    assert rows[0]["steps"] == 3434
    assert rows[0]["active_duration_s"] == 1954


def test_google_health_bronze_specs_match_resources():
    """Les tables déclarées dans assets/bronze.py (GOOGLE_HEALTH_SPECS)
    doivent correspondre exactement aux tables que google_health_resources()
    produit réellement — sinon un asset Dagster resterait "vide" en
    permanence ou une table ne serait jamais exposée dans l'UI."""
    from data_platform.assets.bronze import _GOOGLE_HEALTH_TABLES
    from data_platform.ingestion import google_health as gh

    expected = {"google_health_steps", "google_health_heart_rate_daily", "google_health_sleep",
                "google_health_weight", "google_health_exercise"}
    expected |= {table for table, *_ in gh._INTERVAL_METRICS}
    expected |= {table for table, *_ in gh._INSTANT_METRICS}
    expected |= {table for table, *_ in gh._DAILY_NATIVE_METRICS}

    assert set(_GOOGLE_HEALTH_TABLES.keys()) == expected


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
