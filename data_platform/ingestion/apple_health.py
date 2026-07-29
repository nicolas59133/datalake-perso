"""Source Apple Health — export manuel (aucune API publique n'existe).

Depuis l'app Santé sur iPhone : icône profil -> "Exporter toutes les données
de santé" -> zip AirDrop/transféré sur ce Mac -> décompresser export.xml à
APPLE_HEALTH_EXPORT_PATH (voir config.py et APPLE_HEALTH.md). Chaque nouvel
export remplace le précédent ; le `merge` dlt dédoublonne entre deux imports
qui se chevauchent.

On importe tout ce que l'export contient, en 3 tables "brutes" (une ligne =
un enregistrement Apple, pas de tri par métrique à l'ingestion — il y a des
dizaines de types différents) :
  - Record          -> health_records (mesures ponctuelles : pas, fréquence
                        cardiaque, sommeil, tension...)
  - Workout         -> health_workouts (séances de sport)
  - ActivitySummary -> health_activity_summary (anneaux d'activité, un par jour)

Parsing en streaming (xml.etree.ElementTree.iterparse + elem.clear()) : un
export de plusieurs années avec suivi Apple Watch peut faire plusieurs
centaines de Mo, le charger en DOM complet saturerait la mémoire.
"""
import hashlib
import os
import xml.etree.ElementTree as ET

import dlt

# Types HealthKit dont on dérive la distance/l'énergie d'une séance de sport
# quand l'export ne les donne pas en attributs directs (format iOS récent).
_WORKOUT_DISTANCE_TYPE = "HKQuantityTypeIdentifierDistanceWalkingRunning"
_WORKOUT_ENERGY_TYPE = "HKQuantityTypeIdentifierActiveEnergyBurned"


def _to_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _record_id(rec: dict) -> str:
    key = f"{rec['type']}|{rec['source_name']}|{rec['start_date']}|{rec['end_date']}|{rec['value_text']}"
    return hashlib.sha1(key.encode()).hexdigest()


def _workout_id(w: dict) -> str:
    key = f"{w['workout_type']}|{w['source_name']}|{w['start_date']}|{w['end_date']}"
    return hashlib.sha1(key.encode()).hexdigest()


def _workout_statistic_sum(elem: ET.Element, hk_type: str):
    for stat in elem.findall("WorkoutStatistics"):
        if stat.get("type") == hk_type:
            return _to_float(stat.get("sum"))
    return None


def parse_apple_health_export(xml_path: str) -> tuple[list[dict], list[dict], list[dict]]:
    """Parse export.xml en streaming. Fonction pure (prend un chemin, pas de
    dépendance Dagster/dlt) -> testable sur un petit fichier d'exemple."""
    records: list[dict] = []
    workouts: list[dict] = []
    activity_summaries: list[dict] = []

    for _, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag == "Record":
            value_text = elem.get("value")
            row = {
                "type": elem.get("type"),
                "source_name": elem.get("sourceName"),
                "unit": elem.get("unit"),
                "value": _to_float(value_text),
                "value_text": value_text,
                "creation_date": elem.get("creationDate"),
                "start_date": elem.get("startDate"),
                "end_date": elem.get("endDate"),
            }
            row["record_id"] = _record_id(row)
            records.append(row)
            elem.clear()

        elif elem.tag == "Workout":
            row = {
                "workout_type": elem.get("workoutActivityType"),
                "source_name": elem.get("sourceName"),
                "start_date": elem.get("startDate"),
                "end_date": elem.get("endDate"),
                "total_distance_km": _to_float(elem.get("totalDistance"))
                or _workout_statistic_sum(elem, _WORKOUT_DISTANCE_TYPE),
                "total_energy_kcal": _to_float(elem.get("totalEnergyBurned"))
                or _workout_statistic_sum(elem, _WORKOUT_ENERGY_TYPE),
            }
            row["workout_id"] = _workout_id(row)
            workouts.append(row)
            elem.clear()

        elif elem.tag == "ActivitySummary":
            activity_summaries.append(
                {
                    "date": elem.get("dateComponents"),
                    "active_energy_kcal": _to_float(elem.get("activeEnergyBurned")),
                    "active_energy_goal_kcal": _to_float(elem.get("activeEnergyBurnedGoal")),
                    "exercise_minutes": _to_float(elem.get("appleExerciseTime")),
                    "exercise_goal_minutes": _to_float(elem.get("appleExerciseTimeGoal")),
                    "stand_hours": _to_float(elem.get("appleStandHours")),
                    "stand_goal_hours": _to_float(elem.get("appleStandHoursGoal")),
                }
            )
            elem.clear()

    return records, workouts, activity_summaries


def apple_health_resources(export_path: str):
    """Renvoie les 3 dlt resources prêtes pour `pipeline.run([...])`."""
    if not os.path.exists(export_path):
        raise RuntimeError(
            f"Aucun export Apple Health trouvé à {export_path}.\n"
            "Exporte depuis l'app Santé (icône profil -> Exporter toutes les "
            "données de santé), décompresse le zip, et place export.xml à cet "
            "emplacement. Voir APPLE_HEALTH.md."
        )
    records, workouts, activity_summaries = parse_apple_health_export(export_path)
    return [
        dlt.resource(
            records, name="health_records", write_disposition="merge", primary_key="record_id"
        ),
        dlt.resource(
            workouts, name="health_workouts", write_disposition="merge", primary_key="workout_id"
        ),
        dlt.resource(
            activity_summaries,
            name="health_activity_summary",
            write_disposition="merge",
            primary_key="date",
        ),
    ]
