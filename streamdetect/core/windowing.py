"""Tumbling-window aggregation over event time.

This is the local, dependency-free twin of what the Spark job computes with
``groupBy(window(event_time, "1 minute"))``: events are bucketed into
fixed, non-overlapping windows; each closed window yields a
:class:`WindowStats` with the event count, the latency distribution
(mean and p95), and a per-wiki breakdown of the count. The breakdown
matters more than it looks: Wikimedia traffic is dozens of wikis sharing
one stream, and "the stream spiked" is a much less useful alert than
"enwiki spiked". It exists so the aggregation logic is unit-testable and
so the local simulation can run the exact pipeline semantics without a
JVM.

:func:`merge_window_partials` is the same idea from the other side: the
Spark job groups by (window, wiki) to get per-wiki counts, and this
function folds those partial rows back into one :class:`WindowStats` on
the driver.

Closing rule: a window is closed once an event arrives whose timestamp is
at or past the window's end (a simple max-event-time watermark). Events
arriving for a window the stream has already passed are counted as *late*
and dropped -- including the sneaky case of a window that never had any
events before the stream moved past it, which an earlier version quietly
opened and emitted as if it were current. The same trade-off Spark makes
with watermarks, made explicit here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .detection import percentile


@dataclass(frozen=True)
class WindowStats:
    window_start: float  # epoch seconds, inclusive
    window_end: float  # epoch seconds, exclusive
    count: int
    mean_latency_ms: float
    p95_latency_ms: float
    max_latency_ms: float
    # Event count broken down by wiki; empty when the source didn't track
    # it (older sink files stay readable for that reason).
    counts_by_wiki: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "window_start": self.window_start,
            "window_end": self.window_end,
            "count": self.count,
            "mean_latency_ms": self.mean_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "max_latency_ms": self.max_latency_ms,
            "counts_by_wiki": self.counts_by_wiki,
        }

    @classmethod
    def from_dict(cls, data: dict) -> WindowStats:
        return cls(
            window_start=float(data["window_start"]),
            window_end=float(data["window_end"]),
            count=int(data["count"]),
            mean_latency_ms=float(data["mean_latency_ms"]),
            p95_latency_ms=float(data["p95_latency_ms"]),
            max_latency_ms=float(data["max_latency_ms"]),
            counts_by_wiki={
                str(k): int(v) for k, v in data.get("counts_by_wiki", {}).items()
            },
        )


@dataclass
class _OpenWindow:
    latencies: list[float] = field(default_factory=list)
    counts_by_wiki: dict[str, int] = field(default_factory=dict)

    def summarise(self, start: float, window_seconds: int) -> WindowStats:
        count = len(self.latencies)
        return WindowStats(
            window_start=start,
            window_end=start + window_seconds,
            count=count,
            mean_latency_ms=sum(self.latencies) / count if count else 0.0,
            p95_latency_ms=percentile(self.latencies, 95) if count else 0.0,
            max_latency_ms=max(self.latencies) if count else 0.0,
            counts_by_wiki=dict(sorted(self.counts_by_wiki.items())),
        )


class TumblingWindowAggregator:
    """Aggregates (timestamp, latency_ms) observations into tumbling windows."""

    def __init__(self, window_seconds: int = 60) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self.window_seconds = window_seconds
        self._open: dict[int, _OpenWindow] = {}
        self._closed_through: float = float("-inf")  # watermark
        # Highest window start any event has ever belonged to. An event
        # for an earlier window that is not still open is late -- even if
        # that window never existed, which is the gap case.
        self._seen_through: float = float("-inf")
        self.late_dropped = 0

    def _window_start(self, timestamp: float) -> int:
        return int(timestamp // self.window_seconds) * self.window_seconds

    def add(
        self, timestamp: float, latency_ms: float, wiki: str | None = None
    ) -> list[WindowStats]:
        """Add one observation; returns any windows this event closed.

        *wiki* is optional so existing two-argument callers keep working;
        when given, the event also lands in the window's per-wiki counts.
        """
        start = self._window_start(timestamp)
        already_closed = start + self.window_seconds <= self._closed_through
        passed_and_never_opened = start < self._seen_through and start not in self._open
        if already_closed or passed_and_never_opened:
            self.late_dropped += 1
            return []
        self._seen_through = max(self._seen_through, float(start))
        window = self._open.setdefault(start, _OpenWindow())
        window.latencies.append(latency_ms)
        if wiki is not None:
            window.counts_by_wiki[wiki] = window.counts_by_wiki.get(wiki, 0) + 1
        return self._close_ready(timestamp)

    def _close_ready(self, watermark: float) -> list[WindowStats]:
        closed: list[WindowStats] = []
        for start in sorted(self._open):
            if start + self.window_seconds <= watermark:
                stats = self._open.pop(start).summarise(start, self.window_seconds)
                closed.append(stats)
                self._closed_through = max(
                    self._closed_through, start + self.window_seconds
                )
        return closed

    def flush(self) -> list[WindowStats]:
        """Close every open window (end of stream)."""
        closed = [
            self._open[start].summarise(start, self.window_seconds)
            for start in sorted(self._open)
        ]
        if self._open:
            self._closed_through = max(
                start + self.window_seconds for start in self._open
            )
        self._open.clear()
        return closed


def merge_window_partials(partials: list[dict]) -> WindowStats:
    """Fold per-wiki partial aggregates for one window into WindowStats.

    The Spark job groups by (window, wiki), so the driver gets one partial
    row per wiki in the window, each a dict with ``window_start``,
    ``window_end``, ``wiki``, ``count``, ``mean_latency_ms``,
    ``p95_latency_ms``, and ``max_latency_ms``.

    The merged count and per-wiki counts are exact, and the merged mean
    latency is the exact count-weighted mean. The merged p95 is the *max*
    of the per-wiki p95s -- an approximation, stated plainly: exact
    percentiles don't combine, and for a monitoring signal a slight
    overestimate beats shipping a second aggregation pass. (The local
    aggregator above computes the exact p95 from raw latencies; the two
    paths can differ by a little on the same data.)
    """
    if not partials:
        raise ValueError("no partials to merge")
    window_start = float(partials[0]["window_start"])
    window_end = float(partials[0]["window_end"])
    total_count = sum(int(p["count"]) for p in partials)
    counts_by_wiki = {
        str(p["wiki"]): int(p["count"])
        for p in sorted(partials, key=lambda p: str(p["wiki"]))
    }
    if total_count:
        mean_latency = (
            sum(float(p["mean_latency_ms"]) * int(p["count"]) for p in partials)
            / total_count
        )
    else:
        mean_latency = 0.0
    return WindowStats(
        window_start=window_start,
        window_end=window_end,
        count=total_count,
        mean_latency_ms=mean_latency,
        p95_latency_ms=max(float(p["p95_latency_ms"]) for p in partials),
        max_latency_ms=max(float(p["max_latency_ms"]) for p in partials),
        counts_by_wiki=counts_by_wiki,
    )
