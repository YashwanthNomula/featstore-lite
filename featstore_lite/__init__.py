"""featstore-lite: a tiny feature store with point-in-time-correct joins.

Versioned features, timestamped ingestion, temporal joins that never see
the future, TTL staleness, and a materialized latest-value view for
serving. Zero dependencies.
"""

from .store import FeatureStore
from .schema import Feature, FeatureVersion, parse_ts
from .join import point_in_time_join, naive_join, compare_with_naive

__version__ = "1.0.0"
__all__ = [
    "FeatureStore",
    "Feature",
    "FeatureVersion",
    "parse_ts",
    "point_in_time_join",
    "naive_join",
    "compare_with_naive",
]
