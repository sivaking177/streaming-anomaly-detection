"""Windowing tests: tumbling windows close on the watermark, statistics
are exact for known inputs, and late events are dropped and counted."""

import pytest

from streamdetect.core.windowing import TumblingWindowAggregator

BASE = 1_736_942_400.0  # aligned to a 60-second boundary


def test_window_closes_when_next_window_event_arrives():
    agg = TumblingWindowAggregator(window_seconds=60)
    assert agg.add(BASE + 5, 100.0) == []
    assert agg.add(BASE + 20, 200.0) == []
    assert agg.add(BASE + 40, 300.0) == []
    closed = agg.add(BASE + 61, 150.0)  # first event of the next window
    assert len(closed) == 1
    stats = closed[0]
    assert stats.window_start == BASE
    assert stats.window_end == BASE + 60
    assert stats.count == 3
    assert stats.mean_latency_ms == 200.0
    assert stats.p95_latency_ms == 290.0
    assert stats.max_latency_ms == 300.0


def test_flush_closes_remaining_windows_in_order():
    agg = TumblingWindowAggregator(window_seconds=60)
    closed = []
    closed += agg.add(BASE + 10, 100.0)
    closed += agg.add(BASE + 70, 100.0)  # closes the BASE window
    closed += agg.add(BASE + 130, 100.0)  # closes the BASE+60 window
    closed += agg.flush()  # closes the BASE+120 window
    assert [s.window_start for s in closed] == [BASE, BASE + 60, BASE + 120]
    assert [s.count for s in closed] == [1, 1, 1]
    assert agg.flush() == []


def test_late_events_are_dropped_and_counted():
    agg = TumblingWindowAggregator(window_seconds=60)
    agg.add(BASE + 10, 100.0)
    agg.add(BASE + 70, 100.0)  # closes the first window
    assert agg.add(BASE + 30, 100.0) == []  # belongs to the closed window
    assert agg.late_dropped == 1


def test_out_of_order_within_open_windows_is_fine():
    agg = TumblingWindowAggregator(window_seconds=60)
    agg.add(BASE + 50, 100.0)
    agg.add(BASE + 10, 300.0)  # earlier event, same still-open window
    closed = agg.add(BASE + 61, 100.0)
    assert closed[0].count == 2
    assert closed[0].mean_latency_ms == 200.0


def test_invalid_window_size_rejected():
    with pytest.raises(ValueError):
        TumblingWindowAggregator(window_seconds=0)


def test_per_wiki_counts_are_tracked_per_window():
    agg = TumblingWindowAggregator(window_seconds=60)
    agg.add(BASE + 5, 100.0, wiki="enwiki")
    agg.add(BASE + 10, 100.0, wiki="enwiki")
    agg.add(BASE + 15, 100.0, wiki="dewiki")
    agg.add(BASE + 20, 100.0)  # no wiki: counts toward the total only
    closed = agg.add(BASE + 61, 100.0, wiki="enwiki")
    stats = closed[0]
    assert stats.count == 4
    assert stats.counts_by_wiki == {"enwiki": 2, "dewiki": 1}
    # The breakdown survives the dict round-trip the sink does.
    from streamdetect.core.windowing import WindowStats

    assert WindowStats.from_dict(stats.to_dict()) == stats


def test_late_event_for_a_never_opened_window_is_dropped():
    # Regression test: the stream's first event is five windows in, then a
    # straggler arrives for the first window. That window was never opened
    # -- an earlier version opened it anyway and later emitted it as if
    # current. Watermark semantics say it is simply late.
    agg = TumblingWindowAggregator(window_seconds=60)
    agg.add(BASE + 300, 100.0)
    assert agg.add(BASE + 10, 100.0) == []
    assert agg.late_dropped == 1
    flushed = agg.flush()
    assert [s.window_start for s in flushed] == [BASE + 300]


def test_merge_window_partials_combines_per_wiki_rows():
    from streamdetect.core.windowing import merge_window_partials

    partials = [
        {
            "window_start": BASE,
            "window_end": BASE + 60,
            "wiki": "enwiki",
            "count": 300,
            "mean_latency_ms": 100.0,
            "p95_latency_ms": 200.0,
            "max_latency_ms": 500.0,
        },
        {
            "window_start": BASE,
            "window_end": BASE + 60,
            "wiki": "dewiki",
            "count": 100,
            "mean_latency_ms": 200.0,
            "p95_latency_ms": 260.0,
            "max_latency_ms": 700.0,
        },
    ]
    stats = merge_window_partials(partials)
    assert stats.count == 400
    assert stats.counts_by_wiki == {"enwiki": 300, "dewiki": 100}
    # Count-weighted mean is exact: (300*100 + 100*200) / 400.
    assert stats.mean_latency_ms == 125.0
    # p95 is the documented approximation: max of the per-wiki p95s.
    assert stats.p95_latency_ms == 260.0
    assert stats.max_latency_ms == 700.0


def test_merge_window_partials_rejects_empty():
    from streamdetect.core.windowing import merge_window_partials

    with pytest.raises(ValueError):
        merge_window_partials([])
