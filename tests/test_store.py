import os

import pytest

from featstore_lite.schema import parse_ts
from featstore_lite.store import FeatureStore


@pytest.fixture()
def store(tmp_path):
    s = FeatureStore(str(tmp_path / "store"))
    s.init()
    return s


def test_register_creates_v1(store):
    fv = store.register("f1", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    assert fv.version == "v1"
    assert store.features["f1"].entity == "user"


def test_reregister_creates_next_version(store):
    store.register("f1", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    fv = store.register("f1", entity="user", dtype="float", description="v2",
                        now=parse_ts("2026-10-01"))
    assert fv.version == "v2"
    assert store.features["f1"].versions["v2"].dtype == "float"


def test_register_rejects_entity_change(store):
    store.register("f1", entity="user", dtype="int")
    with pytest.raises(ValueError):
        store.register("f1", entity="order", dtype="int")


def test_register_rejects_backdated_version(store):
    store.register("f1", entity="user", dtype="int", now=parse_ts("2026-10-01"))
    with pytest.raises(ValueError):
        store.register("f1", entity="user", dtype="int", now=parse_ts("2026-09-01"))


def test_ingest_and_value_at(store):
    store.register("tickets", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    store.ingest("tickets", "u1", "2026-09-05", 0)
    store.ingest("tickets", "u1", "2026-09-12", 3)
    val, ver, stale = store.value_at("tickets", "u1", parse_ts("2026-09-08"))
    assert (val, ver, stale) == (0, "v1", False)
    val, ver, stale = store.value_at("tickets", "u1", parse_ts("2026-09-14"))
    assert (val, ver, stale) == (3, "v1", False)


def test_value_at_before_first_row_is_null(store):
    store.register("tickets", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    store.ingest("tickets", "u1", "2026-09-05", 0)
    val, ver, stale = store.value_at("tickets", "u1", parse_ts("2026-09-01"))
    assert val is None and ver is None and stale is False


def test_value_at_unknown_entity_is_null(store):
    store.register("tickets", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    val, _, _ = store.value_at("tickets", "ghost", parse_ts("2026-09-20"))
    assert val is None


def test_version_validity_per_event(store):
    store.register("logins", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    store.ingest("logins", "u1", "2026-09-10", 9)
    store.register("logins", entity="user", dtype="int", now=parse_ts("2026-09-15"))
    store.ingest("logins", "u1", "2026-09-16", 14, version="v2")
    # before v2 existed -> v1
    val, ver, _ = store.value_at("logins", "u1", parse_ts("2026-09-14"))
    assert (val, ver) == (9, "v1")
    # after v2 exists -> v2
    val, ver, _ = store.value_at("logins", "u1", parse_ts("2026-09-20"))
    assert (val, ver) == (14, "v2")


def test_version_fallback_to_older_version(store):
    store.register("logins", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    store.ingest("logins", "u1", "2026-09-10", 9)
    store.register("logins", entity="user", dtype="int", now=parse_ts("2026-09-15"))
    # v2 valid at event time but has no rows for u1 -> falls back to v1 row
    val, ver, _ = store.value_at("logins", "u1", parse_ts("2026-09-20"))
    assert (val, ver) == (9, "v1")


def test_pinned_version_before_valid_from_is_null(store):
    store.register("logins", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    store.register("logins", entity="user", dtype="int", now=parse_ts("2026-09-15"))
    store.ingest("logins", "u1", "2026-09-16", 14, version="v2")
    val, ver, _ = store.value_at("logins", "u1", parse_ts("2026-09-14"), version="v2")
    assert val is None


def test_ttl_staleness(store):
    store.register("p95", entity="user", dtype="float", ttl_seconds=3 * 86400,
                   now=parse_ts("2026-09-01"))
    store.ingest("p95", "u1", "2026-09-02", 410.0)
    val, ver, stale = store.value_at("p95", "u1", parse_ts("2026-09-14"))
    assert val is None and ver == "v1" and stale is True
    # within TTL -> value returned
    val, _, stale = store.value_at("p95", "u1", parse_ts("2026-09-04"))
    assert val == 410.0 and stale is False


def test_ingest_rejects_unknown_feature(store):
    with pytest.raises(KeyError):
        store.ingest("nope", "u1", "2026-09-05", 1)


def test_ingest_rejects_bad_value(store):
    store.register("tickets", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    with pytest.raises(ValueError):
        store.ingest("tickets", "u1", "2026-09-05", "many")


def test_ingest_rejects_row_before_version_birth(store):
    store.register("f1", entity="user", dtype="int", now=parse_ts("2026-09-15"))
    with pytest.raises(ValueError):
        store.ingest("f1", "u1", "2026-09-01", 1)


def test_persistence_roundtrip(tmp_path):
    path = str(tmp_path / "store")
    s = FeatureStore(path)
    s.register("tickets", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    s.ingest("tickets", "u1", "2026-09-05", 7)
    s2 = FeatureStore(path)
    val, ver, _ = s2.value_at("tickets", "u1", parse_ts("2026-09-20"))
    assert (val, ver) == (7, "v1")
    assert os.path.exists(os.path.join(path, "features.json"))
    assert os.path.exists(os.path.join(path, "values.jsonl"))


def test_latest_materialized_view(store):
    store.register("tickets", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    store.ingest("tickets", "u1", "2026-09-05", 0)
    store.ingest("tickets", "u1", "2026-09-12", 3)
    store.ingest("tickets", "u2", "2026-09-06", 1)
    mat = store.latest("tickets")
    assert mat["u1"][0] == 3
    assert mat["u2"][0] == 1


def test_latest_prefers_newest_version(store):
    store.register("f", entity="user", dtype="int", now=parse_ts("2026-09-01"))
    store.ingest("f", "u1", "2026-09-05", 1)
    store.register("f", entity="user", dtype="int", now=parse_ts("2026-09-15"))
    store.ingest("f", "u1", "2026-09-16", 2, version="v2")
    store.ingest("f", "u2", "2026-09-06", 9)  # only ever had v1
    mat = store.latest("f")
    assert mat["u1"][0] == 2 and mat["u1"][2] == "v2"
    assert mat["u2"][0] == 9 and mat["u2"][2] == "v1"
