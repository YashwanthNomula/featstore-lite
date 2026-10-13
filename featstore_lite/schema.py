"""Feature definitions, versioning, and value validation for featstore-lite.

A *feature* belongs to one entity type (e.g. ``user``) and has one or more
*versions*. Each version is valid from the moment it is registered
(``valid_from``). Ingested rows are pinned to a version, and point-in-time
joins only ever read rows from a version that existed at the event time —
so a v2 rollout can never leak back into older training rows.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

DTYPES = ("int", "float", "str", "bool")


def parse_ts(value: Any) -> float:
    """Parse an ISO-8601 string or epoch number into epoch seconds (UTC)."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    text = str(value).strip()
    if not text:
        raise ValueError("empty timestamp")
    # epoch as a string?
    try:
        return float(text)
    except ValueError:
        pass
    iso = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise ValueError(f"unparseable timestamp: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def format_ts(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")


def coerce(dtype: str, value: Any) -> Any:
    """Coerce an ingested value to the feature dtype, fail-closed."""
    if dtype == "int":
        if isinstance(value, bool):
            raise ValueError(f"bool {value!r} is not a valid int")
        if isinstance(value, float):
            if not value.is_integer():
                raise ValueError(f"{value!r} is not a whole number")
            return int(value)
        return int(str(value).strip())
    if dtype == "float":
        if isinstance(value, bool):
            raise ValueError(f"bool {value!r} is not a valid float")
        return float(str(value).strip())
    if dtype == "bool":
        text = str(value).strip().lower()
        if text in ("true", "1", "yes", "y", "t"):
            return True
        if text in ("false", "0", "no", "n", "f"):
            return False
        raise ValueError(f"cannot coerce {value!r} to bool")
    # str
    return str(value)


@dataclass
class FeatureVersion:
    version: str
    valid_from: float  # epoch seconds
    dtype: str
    ttl_seconds: Optional[float] = None
    description: str = ""

    def __post_init__(self) -> None:
        if self.dtype not in DTYPES:
            raise ValueError(f"dtype must be one of {DTYPES}, got {self.dtype!r}")
        if self.ttl_seconds is not None:
            if not isinstance(self.ttl_seconds, (int, float)) or isinstance(self.ttl_seconds, bool):
                raise ValueError("ttl_seconds must be a number")
            if self.ttl_seconds <= 0:
                raise ValueError("ttl_seconds must be positive")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "valid_from": self.valid_from,
            "dtype": self.dtype,
            "ttl_seconds": self.ttl_seconds,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FeatureVersion":
        return cls(
            version=data["version"],
            valid_from=data["valid_from"],
            dtype=data["dtype"],
            ttl_seconds=data.get("ttl_seconds"),
            description=data.get("description", ""),
        )


@dataclass
class Feature:
    name: str
    entity: str
    versions: Dict[str, FeatureVersion] = field(default_factory=dict)

    def latest_version(self) -> FeatureVersion:
        return max(self.versions.values(), key=lambda v: v.valid_from)

    def versions_valid_at(self, event_ts: float):
        """Versions that existed at event_ts, newest first."""
        valid = [v for v in self.versions.values() if v.valid_from <= event_ts]
        valid.sort(key=lambda v: v.valid_from, reverse=True)
        return valid

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "entity": self.entity,
            "versions": {k: v.to_dict() for k, v in self.versions.items()},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Feature":
        return cls(
            name=data["name"],
            entity=data["entity"],
            versions={k: FeatureVersion.from_dict(v) for k, v in data["versions"].items()},
        )
