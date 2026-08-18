"""Anomaly detection core -- pure Python, no Spark/Kafka imports.

Three ways to judge a stream of per-window event counts, picked by
``DetectorConfig.mode`` and built by :func:`build_detector`:

- ``"zscore"`` -- :class:`RollingZScoreDetector` compares each window
  against the mean and standard deviation of the preceding
  ``baseline_size`` windows. Simple, explainable, and the right default
  when traffic has a stable level.
- ``"ewma"`` -- :class:`EWMADetector` tracks an exponentially weighted
  mean/variance, so the baseline adapts as traffic drifts (Wikimedia
  volume has a strong diurnal cycle; a fixed rolling baseline lags it).
- ``"both"`` -- :class:`CombinedDetector` runs the two above side by side
  and only flags a window when *both* agree it is anomalous. I added it
  because the two detectors fail differently: z-score is jumpy right after
  a level shift, EWMA is slow to call a spike that builds gradually.
  Requiring agreement trades a bit of sensitivity for far fewer false
  alarms, which is the right trade for a dashboard a human watches.

All detectors share the same contract: ``observe(window)`` is called once
per closed window, in time order, and returns an :class:`AnomalyFlag` or
``None``. The first ``warmup_windows`` observations only build the
baseline -- flagging on a handful of windows produces garbage z-scores,
so nothing is flagged until the baseline exists. A window is only added
to the baseline *after* it has been judged, and anomalous windows are
kept out of the z-score baseline so one burst doesn't mask the next one
(they are added to the EWMA baseline, whose whole point is adaptation --
a documented asymmetry).

One more guard matters in practice: with a short baseline the *estimated*
standard deviation is itself noisy, and for count data it can come out far
below the truth, flagging ordinary noise. Counts are at least
Poisson-dispersed, so both detectors floor the baseline sigma at
``sqrt(mean)`` -- a burst still scores tens of sigmas, while a 1.5-sigma
wobble of the underlying process no longer trips the alarm.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass

#: Detector modes accepted by :func:`build_detector`.
DETECTOR_MODES = ("zscore", "ewma", "both")


@dataclass(frozen=True)
class DetectorConfig:
    mode: str = "zscore"  # one of DETECTOR_MODES
    warmup_windows: int = 5
    z_threshold: float = 3.0
    baseline_size: int = 60  # z-score: windows kept in the rolling baseline
    ewma_alpha: float = 0.3  # EWMA smoothing factor, in (0, 1]

    def __post_init__(self) -> None:
        if self.mode not in DETECTOR_MODES:
            raise ValueError(
                f"detector mode must be one of {DETECTOR_MODES}, " f"got {self.mode!r}"
            )


@dataclass(frozen=True)
class AnomalyFlag:
    window_start: float  # epoch seconds
    observed: float  # the window's event count
    expected: float  # baseline mean at judgement time
    z_score: float
    direction: str  # "spike" | "drop"
    method: str  # "zscore" | "ewma" | "zscore+ewma" (combined agreement)

    def to_dict(self) -> dict:
        return {
            "window_start": self.window_start,
            "observed": self.observed,
            "expected": self.expected,
            "z_score": self.z_score,
            "direction": self.direction,
            "method": self.method,
        }


def _flag(
    window_start: float,
    observed: float,
    mean: float,
    std: float,
    config: DetectorConfig,
    method: str,
) -> AnomalyFlag | None:
    if std <= 0:
        # A perfectly flat baseline makes any deviation "infinite sigma";
        # require a minimum absolute deviation to avoid noise flags.
        if abs(observed - mean) < 1.0:
            return None
        z = math.inf if observed != mean else 0.0
    else:
        z = (observed - mean) / std
    if abs(z) < config.z_threshold:
        return None
    return AnomalyFlag(
        window_start=window_start,
        observed=observed,
        expected=mean,
        z_score=z,
        direction="spike" if observed > mean else "drop",
        method=method,
    )


class RollingZScoreDetector:
    """Flags windows whose count is an outlier vs. the rolling baseline."""

    def __init__(self, config: DetectorConfig | None = None) -> None:
        self.config = config or DetectorConfig()
        self._baseline: deque[float] = deque(maxlen=self.config.baseline_size)
        self._seen = 0

    def observe(self, window_start: float, count: float) -> AnomalyFlag | None:
        self._seen += 1
        flag: AnomalyFlag | None = None
        if self._seen > self.config.warmup_windows and len(self._baseline) >= 2:
            values = list(self._baseline)
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
            # Poisson floor: counts can't be less dispersed than sqrt(mean).
            std = max(math.sqrt(variance), math.sqrt(mean) if mean > 0 else 0.0)
            flag = _flag(
                window_start,
                count,
                mean,
                std,
                self.config,
                "zscore",
            )
        if flag is None:
            self._baseline.append(count)
        return flag

    @property
    def baseline_mean(self) -> float | None:
        if not self._baseline:
            return None
        return sum(self._baseline) / len(self._baseline)


class EWMADetector:
    """Flags windows that deviate from an exponentially weighted baseline."""

    def __init__(self, config: DetectorConfig | None = None) -> None:
        self.config = config or DetectorConfig()
        self._mean: float | None = None
        self._var: float = 0.0
        self._seen = 0

    def observe(self, window_start: float, count: float) -> AnomalyFlag | None:
        self._seen += 1
        flag: AnomalyFlag | None = None
        if self._mean is not None and self._seen > self.config.warmup_windows:
            # Same Poisson floor as the z-score detector. The floor is
            # also why there is no "variance must be > 0" gate here: after
            # perfectly flat traffic the EWMA variance is exactly 0, and
            # an earlier version of this method skipped judging entirely
            # in that state -- a spike after a flat stretch was never
            # flagged. With the floor, flat traffic still can't
            # self-flag (deviation 0), but a real spike scores against
            # sqrt(mean) and fires.
            std = max(
                math.sqrt(self._var),
                math.sqrt(self._mean) if self._mean > 0 else 0.0,
            )
            flag = _flag(window_start, count, self._mean, std, self.config, "ewma")
        # Update the baseline with every window, flagged or not (EWMA is
        # meant to track drift; a sustained level shift should become the
        # new normal rather than flag forever).
        alpha = self.config.ewma_alpha
        if self._mean is None:
            self._mean = float(count)
            self._var = 0.0
        else:
            delta = count - self._mean
            self._mean += alpha * delta
            self._var = (1 - alpha) * (self._var + alpha * delta * delta)
        return flag

    @property
    def baseline_mean(self) -> float | None:
        return self._mean


class CombinedDetector:
    """Flags only when the z-score and EWMA detectors agree.

    Both sub-detectors see every window and keep their own baselines (with
    their usual rules -- flagged windows stay out of the z-score baseline,
    everything feeds the EWMA baseline). The combined flag reports the
    z-score detector's numbers: its baseline is the easier one to explain
    ("mean of the last N clean windows"), and if both fired, the two
    z-scores rarely disagree about direction.
    """

    def __init__(self, config: DetectorConfig | None = None) -> None:
        self.config = config or DetectorConfig(mode="both")
        self._zscore = RollingZScoreDetector(self.config)
        self._ewma = EWMADetector(self.config)

    def observe(self, window_start: float, count: float) -> AnomalyFlag | None:
        z_flag = self._zscore.observe(window_start, count)
        ewma_flag = self._ewma.observe(window_start, count)
        if z_flag is None or ewma_flag is None:
            return None
        return AnomalyFlag(
            window_start=z_flag.window_start,
            observed=z_flag.observed,
            expected=z_flag.expected,
            z_score=z_flag.z_score,
            direction=z_flag.direction,
            method="zscore+ewma",
        )

    @property
    def baseline_mean(self) -> float | None:
        return self._zscore.baseline_mean


def build_detector(config: DetectorConfig | None = None):
    """Create the detector named by ``config.mode``.

    Returns a :class:`RollingZScoreDetector`, :class:`EWMADetector`, or
    :class:`CombinedDetector` -- all with the same ``observe`` contract,
    so callers (the Spark job, the simulation) don't branch on the mode.
    """
    config = config or DetectorConfig()
    if config.mode == "zscore":
        return RollingZScoreDetector(config)
    if config.mode == "ewma":
        return EWMADetector(config)
    return CombinedDetector(config)


def percentile(values: list[float], pct: float) -> float:
    """Percentile with linear interpolation (the numpy default method)."""
    if not values:
        raise ValueError("percentile of an empty sequence")
    if not 0 <= pct <= 100:
        raise ValueError("pct must be in [0, 100]")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[int(rank)]
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight
