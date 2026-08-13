"""Core logic: window aggregation and anomaly detection.

Pure Python, no Spark/Kafka imports -- the Spark job calls this code on
its driver, and the tests exercise it directly.

- :mod:`streamdetect.core.windowing` -- tumbling-window aggregation.
- :mod:`streamdetect.core.detection` -- z-score / EWMA / combined detectors.
"""

from .detection import (
    DETECTOR_MODES,
    AnomalyFlag,
    CombinedDetector,
    DetectorConfig,
    EWMADetector,
    RollingZScoreDetector,
    build_detector,
    percentile,
)
from .windowing import (
    TumblingWindowAggregator,
    WindowStats,
    merge_window_partials,
)

__all__ = [
    "DETECTOR_MODES",
    "AnomalyFlag",
    "CombinedDetector",
    "DetectorConfig",
    "EWMADetector",
    "RollingZScoreDetector",
    "TumblingWindowAggregator",
    "WindowStats",
    "build_detector",
    "merge_window_partials",
    "percentile",
]
