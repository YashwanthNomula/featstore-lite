# featstore-lite

A tiny feature store in pure Python: **versioned features**, **point-in-time-correct joins**, TTL staleness, and a materialized latest-value view for serving. Zero dependencies.

The whole point of a feature store is one join: for each training event at time T, attach the feature values *as they were known at T* — never values computed later. Get this wrong and your model trains on the future: offline metrics look great, production falls apart. This is the temporal join behind Feast and Tecton, in under 1,000 lines you can read in one sitting.

## The 60-second demo

```bash
pip install -e .
fstore demo
```

A scripted churn-prediction story: 4 label events, 3 features from nightly jobs landing on different days, and a mid-month v2 rewrite of the login counter. The demo runs both joins on the same data:

```
featstore-lite demo: the churn model that trained on the future
================================================================
...
featstore-lite · point-in-time join
events: 4   features: 3

coverage per feature:
  support_tickets_30d      4/4   filled (100.0%)   versions[v1:4]
  login_count_7d           3/4   filled ( 75.0%)   versions[v1:3]
  session_p95_ms           0/4   filled (  0.0%)   versions[v1:2]   stale>ttl: 2

leak check — point-in-time vs naive latest-value join:
  7/12 cells (58.3%) contain future data in the naive join
    support_tickets_30d: 2 leaked cells
    login_count_7d: 3 leaked cells
    session_p95_ms: 2 leaked cells

joined rows (feature columns only):
  entity_id  event_ts    support_tickets_30d  login_count_7d  session_p95_ms
  user_1     2026-09-08  0                    12              None
  user_1     2026-09-14  3                    9               None
  user_2     2026-09-10  1                    20              None
  user_3     2026-09-21  2                    None            None
```

What the naive join got wrong on the same rows:

| event | feature | point-in-time (correct) | naive (leaked) | why |
|---|---|---|---|---|
| user_1 @ Sep 8 | support_tickets_30d | 0 | **3** | tickets were filed Sep 10–12, *after* the event |
| user_1 @ Sep 8 | login_count_7d | 12 (v1) | **14 (v2)** | v2 didn't exist until Sep 15 |
| user_1 @ Sep 14 | login_count_7d | 9 (v1) | **14 (v2)** | v2 still didn't exist at event time |
| user_2 @ Sep 10 | support_tickets_30d | 1 | **4** | filed Sep 18, after the event |
| user_2 @ Sep 10 | login_count_7d | 20 (v1) | **22 (v2)** | future version again |
| user_1 @ Sep 8/14 | session_p95_ms | null (stale) | **410.0** | 12-day-old reading, TTL is 3 days |

The demo also writes `joined.csv` and a standalone `report.html`.

## How it works

**Register** a feature (re-register to cut a new version; each version is valid from its registration date, backdatable with `--valid-from`):

```bash
fstore init --dir ./store
fstore register --dir ./store --feature spend_30d --entity customer \
    --dtype float --ttl 2592000 --description "trailing 30d spend" \
    --valid-from 2026-09-01
```

**Ingest** timestamped rows from CSV (`entity_id,event_ts,value`; ISO or epoch timestamps). Rows land in the newest version that existed at the row's timestamp — values can never predate the version that produced them, and dtype mismatches are rejected:

```bash
fstore ingest --dir ./store --feature spend_30d --csv spend.csv
fstore ingest --dir ./store --feature spend_30d --csv spend_v2.csv --version v2
```

**Join** an events CSV (`entity_id,event_ts` + your label columns) point-in-time. Pin versions with `feature@v1`, diff against a naive latest-value join with `--compare-naive` (exits 3 when leaks are found — CI-gate friendly):

```bash
fstore join --dir ./store --events labels.csv \
    --features spend_30d,login_count_7d@v1 \
    --out training_set.csv --html report.html --compare-naive
```

**Serve** the freshest values per entity:

```bash
fstore latest --dir ./store --feature spend_30d
```

**Audit** a feature's version history — who changed what, when, and how many rows each version produced (`--json` for machines; unknown names fail closed with a spelling suggestion):

```bash
fstore history --dir ./store spend_30d
```

```
featstore-lite · version history: spend_30d
entity: customer   versions: 2   rows: 5

  version  valid_from  dtype  ttl       rows  entities  description
  v1       2026-09-01  float  2592000s  3     3         trailing 30d spend
  v2       2026-09-15  float  2592000s  2     2         rewritten pipeline, tax-inclusive
```

Each ingested row is pinned to exactly one version — the version that was live at the row's timestamp (or the explicitly pinned one) — so `rows` is an exact attribution, not an estimate. A feature with no rows yet still lists its versions (exit 0); a bad name exits 1:

```
$ fstore history --dir ./store spend_30x
error: feature 'spend_30x' is not registered (did you mean 'spend_30d'?)
```

### Join semantics (the important part)

For each `(entity_id, event_ts)`:
1. Consider only versions with `valid_from <= event_ts`, newest first (or the pinned version).
2. Take the newest row with `row_ts <= event_ts` (binary search per entity timeline).
3. If that row is older than the version's TTL → **null** (stale data is worse than no data; no fallback).
4. If the newest valid version has no row yet → fall back to the older version's timeline (best knowledge available at T).

Storage is a JSON registry plus an append-only `values.jsonl` — the full feature history is auditable and rebuildable.

## Python API

```python
from featstore_lite import FeatureStore, point_in_time_join

store = FeatureStore("./store")
store.register("spend_30d", entity="customer", dtype="float")
store.ingest("spend_30d", "c1", "2026-09-01", 120.0)

events = [{"entity_id": "c1", "event_ts": "2026-09-10", "churned": 0}]
rows, stats = point_in_time_join(store, events, ["spend_30d"])
```

See `examples/quickstart.py`.

## Tests

```bash
python -m pytest tests/ -q   # 48 tests
```

## Command reference

| command | what it does | exit codes |
|---|---|---|
| `fstore init --dir DIR` | create a store | 0 |
| `fstore register ...` | register a feature / new version | 0 / 1 on bad input |
| `fstore ingest ...` | ingest rows from CSV | 0 / 1 if rows skipped |
| `fstore history FEATURE` | version audit trail | 0 / 1 on unknown feature |
| `fstore latest ...` | materialized view per entity | 0 / 1 on unknown feature |
| `fstore join ...` | point-in-time join | 0 / 3 if `--compare-naive` finds leaks |
| `fstore demo` | scripted churn demo | 0 |

## What's deliberately missing

Online serving (Redis), streaming ingestion, Spark/DuckDB backends, and feature transformations (dbt-style SQL) — those are the next 10,000 lines. This is the *correctness core*: temporal joins, versioning, and TTLs, which is where most hand-rolled pipelines silently break.
