from extract_transform.main_script_extract_transform import remove_duplicates

def test_remove_duplicates():
    data = [
        {"id_station": "A", "dh_utc": "2024-01-01"},
        {"id_station": "A", "dh_utc": "2024-01-01"},
        {"id_station": "B", "dh_utc": "2024-01-01"},
    ]

    result = remove_duplicates(data, ["id_station", "dh_utc"])

    assert len(result) == 2