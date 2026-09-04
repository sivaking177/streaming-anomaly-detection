"""Local end-to-end simulation of the streaming pipeline.

The Spark job can't run everywhere (it needs a JVM and a Kafka broker), so
this module replays the same semantics locally: synthetic events are
generated window by window, fed through the tumbling-window aggregator in
micro-batches, judged by a detector, and written through the JSONL sink --
exactly the path production data takes from ``foreachBatch`` onwards. It
doubles as the demo harness and as the end-to-end test fixture.

Traffic model: each window's event count is drawn from a normal
approximation to a Poisson process around a configurable base rate (fine at
these volumes), with per-event latencies drawn around a base mean. Each
event also gets a wiki (enwiki weighted heaviest, like the real stream), so
the per-wiki breakdown is exercised end to end. A burst (or drop, with a
multiplier below 1) can be injected at a chosen window; during a burst the
latency mean is scaled up too, modelling the queueing delay you see when a
stream consumer falls behind.
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass, field
from pathlib import Path

from ..core.detection import (
    DETECTOR_MODES,
    AnomalyFlag,
    DetectorConfig,
    build_detector,
)
from ..core.windowing import TumblingWindowAggregator
from ..output.sink import JsonlStatsSink

EPOCH_START = 1_736_942_400.0  # 2025-01-15T12:00:00Z, arbitrary fixed origin

# enwiki appears twice: it really is roughly double the next wiki's volume.
_SIM_WIKIS = ["enwiki", "enwiki", "dewiki", "frwiki", "jawiki", "eswiki", "commonswiki"]


@dataclass(frozen=True)
class SimConfig:
    windows: int = 40
    window_seconds: int = 60
    base_events_per_window: int = 600
    jitter: float = 0.05  # relative std-dev of the per-window count
    base_latency_ms: float = 120.0
    latency_std_ms: float = 45.0
    burst_start_window: int | None = None  # None = steady traffic
    burst_windows: int = 2
    burst_multiplier: float = 3.0  # <1 simulates a drop
    burst_latency_multiplier: float = 1.6
    micro_batch_seconds: int = 5
    detector_mode: str = "zscore"  # one of DETECTOR_MODES
    seed: int = 42


@dataclass
class SimSummary:
    windows_processed: int = 0
    events_total: int = 0
    late_dropped: int = 0
    anomalies: list[AnomalyFlag] = field(default_factory=list)


def _window_multiplier(cfg: SimConfig, window_index: int) -> float:
    if cfg.burst_start_window is None:
        return 1.0
    if (
        cfg.burst_start_window
        <= window_index
        < (cfg.burst_start_window + cfg.burst_windows)
    ):
        return cfg.burst_multiplier
    return 1.0


def generate_window_events(
    rng: random.Random, cfg: SimConfig, window_index: int
) -> list[tuple[float, float, str]]:
    """(timestamp, latency_ms, wiki) triples for one window, time-sorted."""
    multiplier = _window_multiplier(cfg, window_index)
    mean_count = cfg.base_events_per_window * multiplier
    count = max(0, round(rng.gauss(mean_count, mean_count * cfg.jitter)))
    window_start = EPOCH_START + window_index * cfg.window_seconds
    # Only a burst (multiplier > 1) inflates latency -- that models the
    # queueing delay of a consumer falling behind. A drop means *less*
    # load; scaling its latency up too was a modelling slip.
    latency_mean = cfg.base_latency_ms * (
        cfg.burst_latency_multiplier if multiplier > 1.0 else 1.0
    )
    events = []
    for _ in range(count):
        ts = window_start + rng.random() * cfg.window_seconds
        latency = max(1.0, rng.gauss(latency_mean, cfg.latency_std_ms))
        events.append((ts, latency, rng.choice(_SIM_WIKIS)))
    events.sort(key=lambda e: e[0])
    return events


def run_simulation(
    cfg: SimConfig,
    sink: JsonlStatsSink,
    detector=None,
) -> SimSummary:
    """Run the full local pipeline and write results through *sink*.

    *detector* defaults to whatever ``cfg.detector_mode`` names, built by
    :func:`streamdetect.core.detection.build_detector`.
    """
    rng = random.Random(cfg.seed)
    aggregator = TumblingWindowAggregator(window_seconds=cfg.window_seconds)
    detector = detector or build_detector(DetectorConfig(mode=cfg.detector_mode))
    summary = SimSummary()

    def handle_closed(stats_list) -> None:
        for stats in stats_list:
            summary.windows_processed += 1
            summary.events_total += stats.count
            sink.write_window(stats)
            flag = detector.observe(stats.window_start, stats.count)
            if flag is not None:
                summary.anomalies.append(flag)
                sink.write_anomaly(flag, stats)

    for window_index in range(cfg.windows):
        events = generate_window_events(rng, cfg, window_index)
        # Feed the window's events in micro-batches, like a streaming
        # engine delivering triggers, rather than one event at a time.
        batches: dict[int, list[tuple[float, float, str]]] = {}
        for event in events:
            key = int(event[0] // cfg.micro_batch_seconds)
            batches.setdefault(key, []).append(event)
        for key in sorted(batches):
            for ts, latency, wiki in batches[key]:
                handle_closed(aggregator.add(ts, latency, wiki))
    handle_closed(aggregator.flush())
    summary.late_dropped = aggregator.late_dropped
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="simulation",
        description="Run the local streaming-anomaly simulation.",
    )
    parser.add_argument("--out", required=True, help="sink output directory")
    parser.add_argument("--windows", type=int, default=40)
    parser.add_argument(
        "--base-rate", type=int, default=600, help="events per window in steady traffic"
    )
    parser.add_argument(
        "--burst-start",
        type=int,
        default=None,
        help="window index where an anomaly burst starts",
    )
    parser.add_argument("--burst-windows", type=int, default=2)
    parser.add_argument("--burst-multiplier", type=float, default=3.0)
    parser.add_argument("--detector-mode", choices=DETECTOR_MODES, default="zscore")
    parser.add_argument(
        "--no-parquet", action="store_true", help="skip writing window_stats.parquet"
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    cfg = SimConfig(
        windows=args.windows,
        base_events_per_window=args.base_rate,
        burst_start_window=args.burst_start,
        burst_windows=args.burst_windows,
        burst_multiplier=args.burst_multiplier,
        detector_mode=args.detector_mode,
        seed=args.seed,
    )
    sink = JsonlStatsSink(
        Path(args.out),
        detector_mode=cfg.detector_mode,
        enable_parquet=not args.no_parquet,
    )
    summary = run_simulation(cfg, sink)
    print(
        f"windows={summary.windows_processed} events={summary.events_total} "
        f"anomalies={len(summary.anomalies)} late_dropped={summary.late_dropped}"
    )
    for flag in summary.anomalies:
        print(
            f"  FLAG {flag.direction} at window {flag.window_start:.0f}: "
            f"observed={flag.observed:.0f} expected={flag.expected:.1f} "
            f"z={flag.z_score:.2f} ({flag.method})"
        )
    print(f"output written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
