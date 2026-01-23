import pytest
from datetime import datetime
from extract_transform.main_script_extract_transform import (
    normalize_hourly_amateur,
    remove_duplicates,
    parse_float,
    parse_int,
    parse_datetime,
    clean_and_convert_hourly,
    make_serializable
)

# Tests parse_float, parse_int, parse_datetime

def test_parse_float():
    print("\n--- test_parse_float ---")
    print(parse_float("3.14"), parse_float("abc"), parse_float(None))
    assert parse_float("3.14") == 3.14
    assert parse_float("abc") is None
    assert parse_float(None) is None

def test_parse_int():
    print("\n--- test_parse_int ---")
    print(parse_int("10"), parse_int("10.5"), parse_int(""))
    assert parse_int("10") == 10
    assert parse_int("10.5") is None
    assert parse_int("") is None

def test_parse_datetime():
    print("\n--- test_parse_datetime ---")
    print(parse_datetime("2026-01-23 12:00:00"), parse_datetime(""), parse_datetime(None), parse_datetime("invalid"))
    assert parse_datetime("2026-01-23 12:00:00") == datetime(2026,1,23,12,0,0)
    assert parse_datetime("") is None
    assert parse_datetime(None) is None
    assert parse_datetime("invalid") is None

# Test remove_duplicates

def test_remove_duplicates():
    print("\n--- test_remove_duplicates ---")
    data = [
        {"id_station": "A", "dh_utc": "2026-01-23T00:00:00"},
        {"id_station": "A", "dh_utc": "2026-01-23T00:00:00"},
        {"id_station": "B", "dh_utc": "2026-01-23T01:00:00"}
    ]
    cleaned = remove_duplicates(data, ["id_station", "dh_utc"])
    print(cleaned)
    assert len(cleaned) == 2

# Test normalize_hourly_amateur

def test_normalize_hourly_amateur():
    sample = [
        {
            "Temperature": "68°F",
            "Pressure": "29.92 in",
            "Humidity": "50",
            "Dew Point": "50°F",
            "Speed": "10 mph",
            "Gust": "15 mph",
            "Precip. Rate.": "0.1 in",
            "Precip. Accum.": "0.2 in",
            "Time": "14:30:00"
        }
    ]
    result = normalize_hourly_amateur(sample, "TEST01")
    r = result[0]

    print("\n--- Résultat normalize_hourly_amateur ---")
    for k, v in r.items():
        print(f"{k}: {v}")

    assert abs(r["temperature"] - 20.0) < 0.1
    assert abs(r["pression"] - 1013.2) < 0.1
    assert abs(r["vent_moyen"] - 4.4704) < 0.01
    assert abs(r["vent_rafales"] - 6.7056) < 0.01
    assert abs(r["pluie_1h"] - 2.54) < 0.01
    assert abs(r["pluie_3h"] - 5.08) < 0.01
    assert r["dh_utc"] == "1900-01-01T14:30:00"

# Test clean_and_convert_hourly

def test_clean_and_convert_hourly():
    sample = [
        {"id_station": "X", "dh_utc": "2026-01-23 12:00:00", "temperature": "20"},
        {"id_station": "X", "dh_utc": "2026-01-23 13:00:00", "temperature": "999"}
    ]
    cleaned = clean_and_convert_hourly(sample)
    print("\n--- Résultat clean_and_convert_hourly ---")
    for h in cleaned:
        print(h)

    assert len(cleaned) == 1
    assert cleaned[0]["temperature"] == 20.0


##appeller le pytest : pytest -v extract_transform/test_extract_transform.py