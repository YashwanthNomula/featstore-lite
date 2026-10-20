"""Tests for `fstore history` — per-feature version audit trail."""
import csv
import json

import pytest

from featstore_lite.cli import main
from featstore_lite.store import FeatureStore


@pytest.fixture()
def workdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _write_csv(path, rows):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["entity_id", "event_ts", "value"])
        w.writeheader()
        w.writerows(rows)


def _rows(*vals):
    """Rows on 2026-09-05 (v1 live), 2026-09-20 (v2 live)."""
    return [{"entity_id": f"u{i}", "event_ts": ts, "value": str(v)}
            for i, (ts, v) in enumerate(vals)]


def _setup_two_versions():
    main(["init", "--dir", "s"])
    main(["register", "--dir", "s", "--feature", "spend_30d",
          "--entity", "customer", "--dtype", "int", "--ttl", "86400",
          "--description", "nightly rollup", "--valid-from", "2026-09-01"])
    main(["register", "--dir", "s", "--feature", "spend_30d",
          "--entity", "customer", "--dtype", "float", "--ttl", "86400",
          "--description", "rewritten pipeline", "--valid-from", "2026-09-15"])


def test_history_multi_version_terminal(workdir, capsys):
    _setup_two_versions()
    _write_csv("rows.csv", _rows(("2026-09-05", 10), ("2026-09-06", 20), ("2026-09-20", 3.5)))
    assert main(["ingest", "--dir", "s", "--feature", "spend_30d", "--csv", "rows.csv"]) == 0
    assert main(["history", "--dir", "s", "spend_30d"]) == 0
    out = capsys.readouterr().out
    assert "version history: spend_30d" in out
    assert "entity: customer" in out
    # both versions listed with dates, dtypes, and per-version row counts
    assert "v1" in out and "2026-09-01" in out and "int" in out
    assert "v2" in out and "2026-09-15" in out and "float" in out
    assert "86400s" in out
    lines = out.splitlines()
    v1 = next(l for l in lines if l.strip().startswith("v1 "))
    v2 = next(l for l in lines if l.strip().startswith("v2 "))
    assert "2" in v1.split() and "1" in v2.split()  # rows attributed per version


def test_history_backfill_attribution(workdir):
    _setup_two_versions()
    store = FeatureStore("s")
    # row timestamped before v2 existed lands in v1 even though v2 is newest now
    store.ingest("spend_30d", "u1", "2026-09-10", 7)
    store.ingest("spend_30d", "u2", "2026-09-18", 2.5)
    hist = store.feature_history("spend_30d")
    counts = {v["version"]: v["rows"] for v in hist["versions"]}
    assert counts == {"v1": 1, "v2": 1}


def test_history_version_pinned_ingest_attribution(workdir):
    _setup_two_versions()
    store = FeatureStore("s")
    store.ingest("spend_30d", "u1", "2026-09-20", 5, version="v1")
    hist = store.feature_history("spend_30d")
    counts = {v["version"]: v["rows"] for v in hist["versions"]}
    assert counts == {"v1": 1, "v2": 0}


def test_history_entities_counted(workdir):
    _setup_two_versions()
    store = FeatureStore("s")
    store.ingest("spend_30d", "u1", "2026-09-05", 1)
    store.ingest("spend_30d", "u1", "2026-09-06", 2)
    store.ingest("spend_30d", "u2", "2026-09-06", 3)
    hist = store.feature_history("spend_30d")
    v1 = next(v for v in hist["versions"] if v["version"] == "v1")
    assert v1["rows"] == 3 and v1["entities"] == 2


def test_history_json_shape(workdir, capsys):
    _setup_two_versions()
    _write_csv("rows.csv", _rows(("2026-09-05", 10), ("2026-09-20", 2.5)))
    main(["ingest", "--dir", "s", "--feature", "spend_30d", "--csv", "rows.csv"])
    capsys.readouterr()  # drain ingest chatter before capturing history output
    assert main(["history", "--dir", "s", "spend_30d", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["feature"] == "spend_30d"
    assert payload["entity"] == "customer"
    assert payload["total_rows"] == 2
    assert [v["version"] for v in payload["versions"]] == ["v1", "v2"]
    v1, v2 = payload["versions"]
    assert set(v1) == {"version", "valid_from", "valid_from_date", "dtype",
                       "ttl_seconds", "description", "rows", "entities"}
    assert isinstance(v1["valid_from"], float)
    assert v1["valid_from_date"] == "2026-09-01"
    assert (v1["dtype"], v1["rows"]) == ("int", 1)
    assert (v2["dtype"], v2["rows"], v2["ttl_seconds"]) == ("float", 1, 86400.0)
    assert v2["description"] == "rewritten pipeline"


def test_history_zero_row_feature_still_lists_versions(workdir, capsys):
    main(["init", "--dir", "s"])
    main(["register", "--dir", "s", "--feature", "f0",
          "--entity", "user", "--dtype", "str", "--valid-from", "2026-09-01"])
    assert main(["history", "--dir", "s", "f0"]) == 0
    out = capsys.readouterr().out
    assert "v1" in out and "2026-09-01" in out


def test_history_zero_row_feature_json(workdir, capsys):
    main(["init", "--dir", "s"])
    main(["register", "--dir", "s", "--feature", "f0",
          "--entity", "user", "--dtype", "str"])
    capsys.readouterr()  # drain register chatter
    assert main(["history", "--dir", "s", "f0", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["total_rows"] == 0 and len(payload["versions"]) == 1
    assert payload["versions"][0]["ttl_seconds"] is None  # no TTL registered


def test_history_unknown_feature_fails(workdir, capsys):
    main(["init", "--dir", "s"])
    assert main(["history", "--dir", "s", "nope"]) == 1
    assert "not registered" in capsys.readouterr().err


def test_history_suggests_close_spelling(workdir, capsys):
    _setup_two_versions()
    assert main(["history", "--dir", "s", "spend_30x"]) == 1
    err = capsys.readouterr().err
    assert "did you mean 'spend_30d'?" in err


def test_history_no_suggestion_when_nothing_close(workdir, capsys):
    _setup_two_versions()
    assert main(["history", "--dir", "s", "zzz"]) == 1
    err = capsys.readouterr().err
    assert "did you mean" not in err
    assert "not registered" in err


def test_history_api_raises_keyerror_unknown(workdir):
    main(["init", "--dir", "s"])
    with pytest.raises(KeyError):
        FeatureStore("s").feature_history("nope")
