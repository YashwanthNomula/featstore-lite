"""Point-in-time joins: the heart of featstore-lite.

For each event row ``(entity_id, event_ts)`` the join attaches the value of
every requested feature *as it was known at that moment* — never a value
computed later. This is the temporal join that separates feature stores
from a spreadsheet: without it, training rows silently see the future
(label leakage) and offline metrics stop matching production.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .schema import parse_ts
from .store import FeatureStore

NAIVE = "naive"  # latest value regardless of event time (the bug we demo)


def resolve_specs(store: FeatureStore, specs: List[str]) -> List[Tuple[str, Optional[str]]]:
    """Parse feature specs like ``name`` or ``name@v2``."""
    out = []
    for spec in specs:
        if "@" in spec:
            name, version = spec.split("@", 1)
        else:
            name, version = spec, None
        name = name.strip()
        if name not in store.features:
            raise KeyError(f"feature {name!r} is not registered")
        if version and version not in store.features[name].versions:
            raise KeyError(f"feature {name!r} has no version {version!r}")
        out.append((name, version or None))
    return out


def point_in_time_join(
    store: FeatureStore,
    events: List[Dict[str, Any]],
    specs: List[str],
    entity_col: str = "entity_id",
    ts_col: str = "event_ts",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Join features to events as-of each event timestamp.

    Returns (rows, stats). Each row is the original event dict plus one
    column per feature spec, plus ``_version_<name>`` provenance columns.
    stats carries coverage, null reasons, and per-feature version usage.
    """
    resolved = resolve_specs(store, specs)
    rows: List[Dict[str, Any]] = []
    stats: Dict[str, Any] = {
        "events": len(events),
        "features": {
            name: {"spec": spec, "filled": 0, "null": 0, "stale": 0, "versions": {}}
            for (name, _), spec in zip(resolved, specs)
        },
    }
    for event in events:
        entity_id = str(event[entity_col])
        event_ts = parse_ts(event[ts_col])
        row = dict(event)
        for (name, version), spec in zip(resolved, specs):
            value, used_version, stale = store.value_at(name, entity_id, event_ts, version)
            row[spec] = value
            row[f"_version_{spec}"] = used_version
            fstat = stats["features"][name]
            if value is None:
                fstat["null"] += 1
                if stale:
                    fstat["stale"] += 1
            else:
                fstat["filled"] += 1
            if used_version:
                fstat["versions"][used_version] = fstat["versions"].get(used_version, 0) + 1
        rows.append(row)
    return rows, stats


def naive_join(
    store: FeatureStore,
    events: List[Dict[str, Any]],
    specs: List[str],
    entity_col: str = "entity_id",
    ts_col: str = "event_ts",
) -> List[Dict[str, Any]]:
    """The wrong way, for comparison: attach the *latest* value per entity,
    ignoring event time and version validity. This is what a plain SQL
    ``JOIN`` on entity_id does — and why it leaks."""
    resolved = resolve_specs(store, specs)
    materialized = {name: store.latest(name, version) for name, version in resolved}
    rows = []
    for event in events:
        entity_id = str(event[entity_col])
        row = dict(event)
        for (name, _), spec in zip(resolved, specs):
            hit = materialized[name].get(entity_id)
            row[spec] = hit[0] if hit else None
        rows.append(row)
    return rows


def compare_with_naive(
    asof_rows: List[Dict[str, Any]],
    naive_rows: List[Dict[str, Any]],
    specs: List[str],
) -> Dict[str, Any]:
    """Cell-by-cell diff of correct vs naive join: the leak report."""
    leaked_cells = 0
    total_cells = 0
    per_feature: Dict[str, int] = {s: 0 for s in specs}
    examples: List[Dict[str, Any]] = []
    for arow, nrow in zip(asof_rows, naive_rows):
        for spec in specs:
            total_cells += 1
            a, n = arow[spec], nrow[spec]
            if a != n:
                leaked_cells += 1
                per_feature[spec] += 1
                if len(examples) < 8:
                    examples.append(
                        {
                            "entity_id": arow.get("entity_id"),
                            "event_ts": arow.get("event_ts"),
                            "feature": spec,
                            "point_in_time": a,
                            "naive": n,
                        }
                    )
    return {
        "total_cells": total_cells,
        "leaked_cells": leaked_cells,
        "leak_rate": (leaked_cells / total_cells) if total_cells else 0.0,
        "per_feature": per_feature,
        "examples": examples,
    }


def read_events_csv(path: str) -> List[Dict[str, Any]]:
    import csv

    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))
