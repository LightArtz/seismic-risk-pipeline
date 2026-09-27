import sys
sys.path.insert(0, "/opt/airflow/dags")
from seismic_pipeline import parse_feature

def test_parse_feature():
    sample_feature = {
        "id": "us1234abcd",
        "properties": {
            "mag": 5.2,
            "place": "10km N of Somewhere",
            "time": 1695600000000
        },
        "geometry": {
            "coordinates": [123.45, -6.78, 10.5]
        }
    }

    result = parse_feature(sample_feature)

    assert result["id"] == "us1234abcd"
    assert result["mag"] == 5.2
    assert result["place"] == "10km N of Somewhere"
    assert result["time"] == 1695600000000
    assert result["longitude"] == 123.45
    assert result["latitude"] == -6.78
    assert result["depth_km"] == 10.5