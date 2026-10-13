"""Scripted demo: the churn model that trained on the future.

A tiny SaaS team builds a churn model. Features are computed by nightly
jobs on different days; labels are "did this user churn as of date T?".
The demo shows what happens with a plain SQL-style join (latest value per
user) versus a point-in-time join — and why the offline AUC never
survived contact with production.
"""
from __future__ import annotations

import csv
import os
import tempfile
from typing import Any, Dict, List

from .join import compare_with_naive, naive_join, point_in_time_join
from .report import report_html, report_terminal
from .schema import parse_ts
from .store import FeatureStore

FEATURES = ["support_tickets_30d", "login_count_7d", "session_p95_ms"]


def build_demo_store(path: str) -> FeatureStore:
    store = FeatureStore(path)
    store.init()
    t = parse_ts

    # v1 features, live since Sep 1
    store.register("support_tickets_30d", entity="user", dtype="int",
                   description="support tickets filed in trailing 30d", now=t("2026-09-01"))
    store.register("login_count_7d", entity="user", dtype="int",
                   description="logins in trailing 7d (legacy counter)", now=t("2026-09-01"))
    store.register("session_p95_ms", entity="user", dtype="float", ttl_seconds=3 * 86400,
                   description="p95 session latency ms; stale after 3d", now=t("2026-09-01"))

    # nightly job outputs land on different days
    ingest = [
        ("support_tickets_30d", "user_1", "2026-09-05", 0),
        ("support_tickets_30d", "user_1", "2026-09-12", 3),   # 3 tickets filed Sep 10-12
        ("support_tickets_30d", "user_2", "2026-09-06", 1),
        ("support_tickets_30d", "user_2", "2026-09-18", 4),
        ("support_tickets_30d", "user_3", "2026-09-20", 2),
        ("login_count_7d", "user_1", "2026-09-05", 12),
        ("login_count_7d", "user_1", "2026-09-10", 9),
        ("login_count_7d", "user_2", "2026-09-06", 20),
        ("session_p95_ms", "user_1", "2026-09-02", 410.0),    # goes stale fast (TTL 3d)
    ]
    for name, entity, ts, value in ingest:
        store.ingest(name, entity, ts, value)

    # Sep 15: login counter is rewritten (bot traffic excluded) -> v2.
    # Rows from the old job must NOT bleed into predictions made before Sep 15.
    store.register("login_count_7d", entity="user", dtype="int",
                   description="logins in trailing 7d (bot-filtered)", now=t("2026-09-15"))
    store.ingest("login_count_7d", "user_1", "2026-09-16", 14, version="v2")
    store.ingest("login_count_7d", "user_2", "2026-09-17", 22, version="v2")
    return store


def demo_events() -> List[Dict[str, Any]]:
    return [
        {"entity_id": "user_1", "event_ts": "2026-09-08", "churned": 0},
        {"entity_id": "user_1", "event_ts": "2026-09-14", "churned": 1},
        {"entity_id": "user_2", "event_ts": "2026-09-10", "churned": 0},
        {"entity_id": "user_3", "event_ts": "2026-09-21", "churned": 1},
    ]


NARRATIVE = """\
featstore-lite demo: the churn model that trained on the future
================================================================
A SaaS team trains churn prediction on 4 label events ("did this user
churn as of date T?"). Three features are computed by nightly jobs that
land on different days -- and mid-month the login counter is rewritten
as v2.

Two joins, same data:
  * naive:  plain entity_id JOIN -- the latest value per user, whatever
            the event date (this is what most hand-rolled pipelines do)
  * as-of:  point-in-time join -- only values that existed when the
            event happened, honoring feature versions and TTLs

"""


def run_demo(out_dir: str | None = None) -> Dict[str, Any]:
    if out_dir is None:
        out_dir = os.path.join(tempfile.mkdtemp(prefix="featstore-demo-"))
    os.makedirs(out_dir, exist_ok=True)

    store = build_demo_store(os.path.join(out_dir, "store"))
    events = demo_events()

    events_csv = os.path.join(out_dir, "events.csv")
    with open(events_csv, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["entity_id", "event_ts", "churned"])
        w.writeheader()
        w.writerows(events)

    asof_rows, stats = point_in_time_join(store, events, FEATURES)
    naive_rows = naive_join(store, events, FEATURES)
    leak = compare_with_naive(asof_rows, naive_rows, FEATURES)

    terminal = NARRATIVE + report_terminal(asof_rows, stats, FEATURES, leak)

    joined_csv = os.path.join(out_dir, "joined.csv")
    with open(joined_csv, "w", newline="") as fh:
        cols = ["entity_id", "event_ts", "churned"] + FEATURES
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(asof_rows)

    html_path = os.path.join(out_dir, "report.html")
    with open(html_path, "w") as fh:
        fh.write(report_html(asof_rows, stats, FEATURES, leak))

    return {
        "out_dir": out_dir,
        "terminal": terminal,
        "stats": stats,
        "leak": leak,
        "events_csv": events_csv,
        "joined_csv": joined_csv,
        "html": html_path,
        "store_dir": os.path.join(out_dir, "store"),
    }
