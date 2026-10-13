"""Programmatic example: build a store, ingest, and run a point-in-time join."""
import tempfile

from featstore_lite import FeatureStore, parse_ts, point_in_time_join

store = FeatureStore(tempfile.mkdtemp(prefix="featstore-example-"))
store.init()

store.register("spend_30d", entity="customer", dtype="float",
               description="trailing 30d spend",
               now=parse_ts("2026-08-01"))  # the feature existed before we registered it
store.ingest("spend_30d", "c1", "2026-09-01", 120.0)
store.ingest("spend_30d", "c1", "2026-09-20", 310.5)  # big purchase AFTER the label date

events = [{"entity_id": "c1", "event_ts": "2026-09-10", "churned": 0}]
rows, stats = point_in_time_join(store, events, ["spend_30d"])

print(rows[0]["spend_30d"])   # 120.0 -- the Sep 20 purchase is invisible here
print(stats["features"]["spend_30d"])
