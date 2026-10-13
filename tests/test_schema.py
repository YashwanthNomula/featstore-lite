import pytest

from featstore_lite.schema import FeatureVersion, coerce, format_ts, parse_ts


def test_parse_ts_iso_and_epoch():
    assert parse_ts("2026-09-08") == parse_ts("2026-09-08T00:00:00Z")
    assert parse_ts(1725753600) == 1725753600.0
    assert parse_ts("1725753600") == 1725753600.0
    assert parse_ts("2026-09-08T12:30:00") < parse_ts("2026-09-09")


def test_parse_ts_rejects_garbage():
    for bad in ["", "not-a-date", "2026-13-45"]:
        with pytest.raises(ValueError):
            parse_ts(bad)


def test_format_ts_roundtrip():
    assert format_ts(parse_ts("2026-09-08")) == "2026-09-08"


def test_coerce_int():
    assert coerce("int", "12") == 12
    assert coerce("int", 12.0) == 12
    with pytest.raises(ValueError):
        coerce("int", "12.5")
    with pytest.raises(ValueError):
        coerce("int", True)


def test_coerce_float_bool_str():
    assert coerce("float", "3.5") == 3.5
    assert coerce("bool", "yes") is True
    assert coerce("bool", "0") is False
    assert coerce("str", 42) == "42"
    with pytest.raises(ValueError):
        coerce("bool", "maybe")


def test_feature_version_validation():
    with pytest.raises(ValueError):
        FeatureVersion("v1", 0.0, "uuid")
    with pytest.raises(ValueError):
        FeatureVersion("v1", 0.0, "int", ttl_seconds=-5)
    with pytest.raises(ValueError):
        FeatureVersion("v1", 0.0, "int", ttl_seconds="soon")
    v = FeatureVersion("v1", 0.0, "int", ttl_seconds=3600)
    assert v.to_dict()["ttl_seconds"] == 3600
    assert FeatureVersion.from_dict(v.to_dict()).dtype == "int"
