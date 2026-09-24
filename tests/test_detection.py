"""Detector tests on series with known properties: a stable repeating
pattern must never flag; a 3x burst and a collapse must flag, in the right
direction, only after warmup."""

import pytest

from streamdetect.core.detection import (
    CombinedDetector,
    DetectorConfig,
    EWMADetector,
    RollingZScoreDetector,
    build_detector,
    percentile,
)

# Repeating pattern: mean 600, sample std ~6.3 -- deviations stay under
# 1.3 sigma, far below the 3.0 threshold.
STEADY = [600, 608, 592, 604, 596]
BASE = 1_736_942_400.0


def feed(detector, counts, start=BASE):
    flags = []
    for i, count in enumerate(counts):
        flag = detector.observe(start + i * 60, count)
        if flag is not None:
            flags.append(flag)
    return flags


def test_zscore_never_flags_steady_traffic():
    detector = RollingZScoreDetector(DetectorConfig())
    assert feed(detector, STEADY * 10) == []


def test_zscore_flags_injected_burst():
    detector = RollingZScoreDetector(DetectorConfig())
    counts = STEADY * 4 + [1800]
    flags = feed(detector, counts)
    assert len(flags) == 1
    flag = flags[0]
    assert flag.direction == "spike"
    assert flag.observed == 1800
    assert flag.z_score >= 3.0
    assert flag.method == "zscore"
    assert flag.window_start == BASE + 20 * 60


def test_zscore_flags_a_drop():
    detector = RollingZScoreDetector(DetectorConfig())
    counts = STEADY * 4 + [90]
    flags = feed(detector, counts)
    assert len(flags) == 1
    assert flags[0].direction == "drop"


def test_no_flags_during_warmup():
    detector = RollingZScoreDetector(DetectorConfig(warmup_windows=5))
    # A burst inside the warmup period must not flag: there is no baseline.
    counts = [600, 5000, 600, 600, 600, 600]
    assert feed(detector, counts) == []


def test_flagged_burst_does_not_poison_zscore_baseline():
    detector = RollingZScoreDetector(DetectorConfig())
    counts = STEADY * 4 + [1800] + STEADY * 2
    flags = feed(detector, counts)
    # Only the burst flags; the steady windows after it must not, i.e. the
    # burst was kept out of the baseline.
    assert [f.observed for f in flags] == [1800]


def test_flat_baseline_flags_real_deviation_only():
    detector = RollingZScoreDetector(DetectorConfig())
    assert feed(detector, [600] * 12) == []
    # +50 on a flat 600 is within Poisson noise (sigma floor sqrt(600) ~= 24.5)
    # and must NOT flag...
    assert feed(detector, [650], start=BASE + 12 * 60) == []
    # ...while a genuine jump does.
    flags = feed(detector, [900], start=BASE + 13 * 60)
    assert len(flags) == 1


def test_ewma_flags_burst_and_not_steady():
    detector = EWMADetector(DetectorConfig())
    assert feed(detector, STEADY * 4) == []
    flags = feed(detector, [1800], start=BASE + 20 * 60)
    assert len(flags) == 1
    assert flags[0].method == "ewma"
    assert flags[0].direction == "spike"


def test_percentile_linear_interpolation():
    assert percentile([float(i) for i in range(1, 101)], 95) == 95.05
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.5
    assert percentile([7.0], 95) == 7.0


# ----------------------------------------------------------------------
# Detector modes (zscore | ewma | both) via the factory
# ----------------------------------------------------------------------


def test_factory_builds_the_mode_asked_for():
    assert isinstance(
        build_detector(DetectorConfig(mode="zscore")), RollingZScoreDetector
    )
    assert isinstance(build_detector(DetectorConfig(mode="ewma")), EWMADetector)
    assert isinstance(build_detector(DetectorConfig(mode="both")), CombinedDetector)


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        DetectorConfig(mode="vibes")


def test_both_mode_flags_a_clear_burst():
    detector = build_detector(DetectorConfig(mode="both"))
    assert feed(detector, STEADY * 4) == []
    flags = feed(detector, [1800], start=BASE + 20 * 60)
    assert len(flags) == 1
    assert flags[0].method == "zscore+ewma"
    assert flags[0].direction == "spike"


def test_both_mode_stays_quiet_on_steady_traffic():
    detector = build_detector(DetectorConfig(mode="both"))
    assert feed(detector, STEADY * 12) == []


def test_ewma_flags_spike_after_perfectly_flat_traffic():
    # Regression test: flat traffic keeps the EWMA variance at exactly 0,
    # and a version of the detector that required var > 0 before judging
    # never flagged anything after a flat stretch.
    detector = EWMADetector(DetectorConfig())
    assert feed(detector, [600] * 12) == []
    flags = feed(detector, [5000], start=BASE + 12 * 60)
    assert len(flags) == 1
    assert flags[0].direction == "spike"


def test_both_mode_flags_spike_after_perfectly_flat_traffic():
    detector = build_detector(DetectorConfig(mode="both"))
    assert feed(detector, [600] * 12) == []
    flags = feed(detector, [5000], start=BASE + 12 * 60)
    assert len(flags) == 1


def test_both_mode_requires_agreement_after_a_burst():
    # The documented asymmetry, used as a disagreement generator: a burst
    # stays OUT of the z-score baseline (so the z-score detector still
    # calls a later 680 a spike vs. the clean ~600 baseline), but the
    # burst goes INTO the EWMA baseline (mean pulled toward ~960, sigma
    # inflated), so EWMA does not call 680 anomalous. "both" must
    # therefore stay quiet where z-score alone flags.
    sequence = STEADY * 4 + [1800]
    z_only = RollingZScoreDetector(DetectorConfig())
    assert len(feed(z_only, sequence)) == 1
    assert z_only.observe(BASE + 21 * 60, 680) is not None

    both = build_detector(DetectorConfig(mode="both"))
    burst_flags = feed(both, sequence)
    assert len(burst_flags) == 1  # on the burst itself they agree
    assert both.observe(BASE + 21 * 60, 680) is None
