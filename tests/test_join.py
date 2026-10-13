import pytest

from featstore_lite.demo import build_demo_store, demo_events
from featstore_lite.join import (compare_with_naive, naive_join,
                                 point_in_time_join, resolve_specs)
from featstore_lite.schema import parse_ts


@pytest.fixture()
def demo(tmp_path):
    store = build_demo_store(str(tmp_path / "store"))
    return store, demo_events()


def test_asof_join_values(demo):
    store, events = demo
    rows, stats = point_in_time_join(
        store, events, ["support_tickets_30d", "login_count_7d", "session_p95_ms"])
    by_key = {(r["entity_id"], r["event_ts"]): r for r in rows}
    # user_1 @ Sep 8: tickets row from Sep 5, v1 logins, p95 stale (TTL 3d)
    r = by_key[("user_1", "2026-09-08")]
    assert r["support_tickets_30d"] == 0
    assert r["login_count_7d"] == 12
    assert r["_version_login_count_7d"] == "v1"
    assert r["session_p95_ms"] is None
    # user_1 @ Sep 14: v2 not valid yet -> still v1 logins
    r = by_key[("user_1", "2026-09-14")]
    assert r["support_tickets_30d"] == 3
    assert r["login_count_7d"] == 9
    assert r["_version_login_count_7d"] == "v1"
    # user_3 @ Sep 21: never had login rows -> null
    r = by_key[("user_3", "2026-09-21")]
    assert r["support_tickets_30d"] == 2
    assert r["login_count_7d"] is None


def test_label_columns_preserved(demo):
    store, events = demo
    rows, _ = point_in_time_join(store, events, ["support_tickets_30d"])
    assert [r["churned"] for r in rows] == [0, 1, 0, 1]


def test_version_pinning(demo):
    store, events = demo
    rows, _ = point_in_time_join(store, events, ["login_count_7d@v1"])
    r = [x for x in rows if x["entity_id"] == "user_1" and x["event_ts"] == "2026-09-14"][0]
    assert r["login_count_7d@v1"] == 9


def test_naive_join_leaks_future(demo):
    store, events = demo
    specs = ["support_tickets_30d", "login_count_7d", "session_p95_ms"]
    asof_rows, _ = point_in_time_join(store, events, specs)
    naive_rows = naive_join(store, events, specs)
    leak = compare_with_naive(asof_rows, naive_rows, specs)
    assert leak["leaked_cells"] > 0
    assert leak["total_cells"] == 12
    # the headline demo numbers
    assert leak["leaked_cells"] == 7
    assert leak["per_feature"]["support_tickets_30d"] == 2
    assert leak["per_feature"]["login_count_7d"] == 3
    assert leak["per_feature"]["session_p95_ms"] == 2


def test_naive_join_specific_leak(demo):
    store, events = demo
    naive_rows = naive_join(store, events, ["support_tickets_30d"])
    r = [x for x in naive_rows if x["entity_id"] == "user_1"
         and x["event_ts"] == "2026-09-08"][0]
    # tickets filed Sep 10-12 visible to a Sep 8 prediction: classic leakage
    assert r["support_tickets_30d"] == 3


def test_stats_coverage(demo):
    store, events = demo
    _, stats = point_in_time_join(store, events, ["login_count_7d"])
    fs = stats["features"]["login_count_7d"]
    assert fs["filled"] == 3 and fs["null"] == 1
    assert fs["versions"] == {"v1": 3}


def test_stats_stale_counted(demo):
    store, events = demo
    _, stats = point_in_time_join(store, events, ["session_p95_ms"])
    fs = stats["features"]["session_p95_ms"]
    assert fs["filled"] == 0 and fs["null"] == 4 and fs["stale"] == 2


def test_resolve_specs_rejects_unknown(tmp_path):
    store = build_demo_store(str(tmp_path / "store"))
    with pytest.raises(KeyError):
        resolve_specs(store, ["nope"])
    with pytest.raises(KeyError):
        resolve_specs(store, ["support_tickets_30d@v9"])
    assert resolve_specs(store, ["support_tickets_30d@v1"]) == [("support_tickets_30d", "v1")]
