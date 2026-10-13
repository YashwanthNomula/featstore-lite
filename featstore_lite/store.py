"""Persistent feature store: registry + append-only value log.

On-disk layout inside the store directory::

    features.json   # registry: Feature definitions with versions
    values.jsonl    # append-only log of (feature, version, entity, ts, value)

Timelines are kept in memory per (feature, version, entity_id) and sorted
on load/ingest, so point-in-time lookups are binary searches.
"""
from __future__ import annotations

import json
import os
import time
from bisect import bisect_right, insort
from typing import Any, Dict, List, Optional, Tuple

from .schema import Feature, FeatureVersion, coerce, parse_ts


class FeatureStore:
    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self.features: Dict[str, Feature] = {}
        # (feature, version, entity_id) -> sorted list of (ts, value)
        self._timelines: Dict[Tuple[str, str, str], List[Tuple[float, Any]]] = {}
        self._dirty = False
        if os.path.isdir(self.path):
            self._load()

    # ------------------------------------------------------------------ IO
    @property
    def registry_path(self) -> str:
        return os.path.join(self.path, "features.json")

    @property
    def values_path(self) -> str:
        return os.path.join(self.path, "values.jsonl")

    def init(self) -> None:
        os.makedirs(self.path, exist_ok=True)
        if not os.path.exists(self.registry_path):
            with open(self.registry_path, "w") as fh:
                json.dump({"features": {}}, fh, indent=2)
        if not os.path.exists(self.values_path):
            open(self.values_path, "a").close()

    def _load(self) -> None:
        if os.path.exists(self.registry_path):
            with open(self.registry_path) as fh:
                data = json.load(fh)
            self.features = {
                name: Feature.from_dict(fd) for name, fd in data.get("features", {}).items()
            }
        if os.path.exists(self.values_path):
            with open(self.values_path) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    key = (row["feature"], row["version"], row["entity_id"])
                    self._timelines.setdefault(key, []).append((row["ts"], row["value"]))
        for timeline in self._timelines.values():
            timeline.sort(key=lambda r: r[0])

    def save(self) -> None:
        self.init()
        with open(self.registry_path, "w") as fh:
            json.dump(
                {"features": {n: f.to_dict() for n, f in self.features.items()}},
                fh,
                indent=2,
            )
        # values.jsonl is append-only; nothing to rewrite.

    # ------------------------------------------------------------ registry
    def register(
        self,
        name: str,
        entity: str,
        dtype: str,
        ttl_seconds: Optional[float] = None,
        description: str = "",
        now: Optional[float] = None,
    ) -> FeatureVersion:
        """Register a new feature (v1) or a new version of an existing one.

        Re-registering the same feature name creates the next version
        (``v2``, ``v3``, ...) valid from ``now``. dtype/entity/ttl may
        change per version — that is the point of versioning.
        """
        now = time.time() if now is None else now
        if name in self.features:
            feature = self.features[name]
            if feature.entity != entity:
                raise ValueError(
                    f"feature {name!r} already registered for entity {feature.entity!r}, "
                    f"not {entity!r}"
                )
            n = len(feature.versions) + 1
            version = f"v{n}"
            latest = feature.latest_version()
            if now < latest.valid_from:
                raise ValueError("new version valid_from cannot predate the current version")
        else:
            feature = Feature(name=name, entity=entity)
            self.features[name] = feature
            version = "v1"
        fv = FeatureVersion(
            version=version,
            valid_from=now,
            dtype=dtype,
            ttl_seconds=ttl_seconds,
            description=description,
        )
        feature.versions[version] = fv
        self.save()
        return fv

    # --------------------------------------------------------------- ingest
    def ingest(
        self,
        name: str,
        entity_id: str,
        event_ts: Any,
        value: Any,
        version: Optional[str] = None,
    ) -> Tuple[str, Any]:
        """Append one feature value. Returns (version_used, coerced_value)."""
        if name not in self.features:
            raise KeyError(f"feature {name!r} is not registered")
        feature = self.features[name]
        ts = parse_ts(event_ts)
        if version is None:
            # default: the newest version that existed at the row's timestamp
            # (backfills land in the version that was live then, not v-latest)
            valid = [v for v in feature.versions.values() if v.valid_from <= ts + 1e-9]
            if not valid:
                raise ValueError(
                    f"value timestamp predates every registered version of {name!r}; "
                    f"register a version first"
                )
            version = max(valid, key=lambda v: v.valid_from).version
        if version not in feature.versions:
            raise KeyError(f"feature {name!r} has no version {version!r}")
        fver = feature.versions[version]
        if ts < fver.valid_from - 1e-9:
            raise ValueError(
                f"value timestamp {ts} predates version {version} valid_from "
                f"({fver.valid_from}); values cannot exist before the version that produced them"
            )
        coerced = coerce(fver.dtype, value)
        key = (name, version, str(entity_id))
        insort(self._timelines.setdefault(key, []), (ts, coerced))
        self.init()
        with open(self.values_path, "a") as fh:
            fh.write(
                json.dumps(
                    {
                        "feature": name,
                        "version": version,
                        "entity_id": str(entity_id),
                        "ts": ts,
                        "value": coerced,
                    }
                )
                + "\n"
            )
        return version, coerced

    # --------------------------------------------------------------- reads
    def timeline(self, name: str, version: str, entity_id: str) -> List[Tuple[float, Any]]:
        return self._timelines.get((name, version, str(entity_id)), [])

    def value_at(
        self,
        name: str,
        entity_id: str,
        event_ts: float,
        version: Optional[str] = None,
    ) -> Tuple[Optional[Any], Optional[str], bool]:
        """Point-in-time read. Returns (value, version_used, stale).

        Picks the newest version valid at ``event_ts`` (or the pinned
        version), then the newest row at-or-before ``event_ts``. If that
        row is older than the version TTL it is stale and returns None.
        Falls back to older versions only when the newer one has no row
        at-or-before the event time.
        """
        if name not in self.features:
            raise KeyError(f"feature {name!r} is not registered")
        feature = self.features[name]
        candidates = feature.versions_valid_at(event_ts)
        if version is not None:
            if version not in feature.versions:
                raise KeyError(f"feature {name!r} has no version {version!r}")
            fv = feature.versions[version]
            if fv.valid_from > event_ts:
                return None, None, False
            candidates = [fv]
        for fv in candidates:
            timeline = self.timeline(name, fv.version, entity_id)
            if not timeline:
                continue
            idx = bisect_right([t for t, _ in timeline], event_ts) - 1
            if idx < 0:
                continue
            row_ts, val = timeline[idx]
            if fv.ttl_seconds is not None and event_ts - row_ts > fv.ttl_seconds:
                return None, fv.version, True  # stale: worse than null, no fallback
            return val, fv.version, False
        return None, None, False

    def latest(self, name: str, version: Optional[str] = None) -> Dict[str, Tuple[Any, float, str]]:
        """Materialized view: newest value per entity (for online serving)."""
        if name not in self.features:
            raise KeyError(f"feature {name!r} is not registered")
        feature = self.features[name]
        versions = (
            [feature.versions[version]]
            if version
            else sorted(feature.versions.values(), key=lambda v: v.valid_from, reverse=True)
        )
        out: Dict[str, Tuple[Any, float, str]] = {}
        for fv in versions:
            for (f, v, entity_id), timeline in self._timelines.items():
                if f == name and v == fv.version and entity_id not in out and timeline:
                    row_ts, val = timeline[-1]
                    out[entity_id] = (val, row_ts, fv.version)
            # entities already filled by a newer version keep the newer value
        return out
