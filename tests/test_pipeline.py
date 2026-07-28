import duckdb
from data_platform.ingestion.weather import parse_open_meteo
from data_platform.assets.silver import build_silver_weather


def test_parse_open_meteo():
    payload = {
        "daily": {
            "time": ["2026-07-25", "2026-07-26"],
            "temperature_2m_max": [24.1, 26.3],
            "temperature_2m_min": [14.0, 15.2],
            "precipitation_sum": [0.0, 2.4],
        }
    }
    rows = parse_open_meteo(payload, 50.62, 3.13)
    assert len(rows) == 2
    assert rows[0]["date"] == "2026-07-25"
    assert rows[1]["temp_max"] == 26.3
    assert rows[0]["latitude"] == 50.62


def test_build_silver_weather(tmp_path):
    db = str(tmp_path / "t.duckdb")
    con = duckdb.connect(db)
    con.execute("create schema bronze")
    con.execute(
        """
        create table bronze.weather_daily as
        select * from (values
            ('2026-07-25', 24.1, 14.0, 0.0, 50.62, 3.13),
            ('2026-07-26', 26.3, 15.2, 2.4, 50.62, 3.13)
        ) as t(date, temp_max, temp_min, precipitation, latitude, longitude)
        """
    )
    build_silver_weather(con)
    result = con.execute(
        "select date, temp_range from silver.weather_daily order by date"
    ).fetchall()
    con.close()
    assert float(result[0][1]) == 10.1   # 24.1 - 14.0
    assert float(result[1][1]) == 11.1   # 26.3 - 15.2


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
