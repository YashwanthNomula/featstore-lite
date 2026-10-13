import csv
import json
import os

import pytest

from featstore_lite.cli import main
from featstore_lite.demo import run_demo


@pytest.fixture()
def workdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def _write_csv(path, fieldnames, rows):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def test_cli_init_register_ingest_latest(workdir):
    assert main(["init", "--dir", "s"]) == 0
    assert main(["register", "--dir", "s", "--feature", "f1",
                 "--entity", "user", "--dtype", "int",
                 "--valid-from", "2026-09-01"]) == 0
    _write_csv("rows.csv", ["entity_id", "event_ts", "value"],
               [{"entity_id": "u1", "event_ts": "2026-09-05", "value": "5"}])
    assert main(["ingest", "--dir", "s", "--feature", "f1", "--csv", "rows.csv"]) == 0
    assert main(["latest", "--dir", "s", "--feature", "f1"]) == 0


def test_cli_register_second_version(workdir):
    main(["init", "--dir", "s"])
    main(["register", "--dir", "s", "--feature", "f1", "--entity", "user", "--dtype", "int"])
    assert main(["register", "--dir", "s", "--feature", "f1",
                 "--entity", "user", "--dtype", "int"]) == 0
    with open(os.path.join("s", "features.json")) as fh:
        reg = json.load(fh)
    assert set(reg["features"]["f1"]["versions"]) == {"v1", "v2"}


def test_cli_join_writes_csv(workdir):
    main(["init", "--dir", "s"])
    main(["register", "--dir", "s", "--feature", "f1", "--entity", "user",
          "--dtype", "int", "--valid-from", "2026-09-01"])
    _write_csv("rows.csv", ["entity_id", "event_ts", "value"],
               [{"entity_id": "u1", "event_ts": "2026-09-05", "value": "5"}])
    main(["ingest", "--dir", "s", "--feature", "f1", "--csv", "rows.csv"])
    _write_csv("events.csv", ["entity_id", "event_ts", "label"],
               [{"entity_id": "u1", "event_ts": "2026-09-10", "label": "1"}])
    assert main(["join", "--dir", "s", "--events", "events.csv",
                 "--features", "f1", "--out", "joined.csv"]) == 0
    with open("joined.csv") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["f1"] == "5" and rows[0]["label"] == "1"


def test_cli_join_compare_naive_exits_3_on_leak(workdir):
    result = run_demo("demo-out")
    store_dir = result["store_dir"]
    rc = main(["join", "--dir", store_dir, "--events", result["events_csv"],
               "--features", "support_tickets_30d,login_count_7d,session_p95_ms",
               "--compare-naive"])
    assert rc == 3


def test_cli_join_unknown_feature_fails(workdir):
    main(["init", "--dir", "s"])
    _write_csv("events.csv", ["entity_id", "event_ts"],
               [{"entity_id": "u1", "event_ts": "2026-09-10"}])
    assert main(["join", "--dir", "s", "--events", "events.csv",
                 "--features", "nope"]) == 1


def test_cli_demo_runs(workdir, capsys):
    assert main(["demo", "-o", "d"]) == 0
    out = capsys.readouterr().out
    assert "point-in-time" in out
    assert os.path.exists(os.path.join("d", "report.html"))
    assert os.path.exists(os.path.join("d", "joined.csv"))
