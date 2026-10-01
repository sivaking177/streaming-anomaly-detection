"""End-to-end simulation tests: the same code path the Spark job drives
(aggregator -> detector -> sink), fed by synthetic traffic with known
properties. The seeds are pinned; these are regression tests."""

from streamdetect.jobs.simulation import EPOCH_START, SimConfig, run_simulation
from streamdetect.output.sink import JsonlStatsSink


def test_steady_traffic_produces_no_anomalies(tmp_path):
    sink = JsonlStatsSink(tmp_path / "out")
    summary = run_simulation(SimConfig(windows=30, seed=42), sink)
    assert summary.windows_processed == 30
    assert summary.events_total > 10_000
    assert summary.anomalies == []
    assert summary.late_dropped == 0


def test_injected_burst_is_flagged_in_the_burst_windows(tmp_path):
    sink = JsonlStatsSink(tmp_path / "out")
    cfg = SimConfig(
        windows=40,
        seed=42,
        burst_start_window=25,
        burst_windows=2,
        burst_multiplier=3.0,
    )
    summary = run_simulation(cfg, sink)
    assert len(summary.anomalies) >= 1
    burst_lo = EPOCH_START + 25 * 60
    burst_hi = EPOCH_START + 27 * 60
    assert all(burst_lo <= f.window_start < burst_hi for f in summary.anomalies)
    assert all(f.direction == "spike" for f in summary.anomalies)


def test_injected_drop_is_flagged(tmp_path):
    sink = JsonlStatsSink(tmp_path / "out")
    cfg = SimConfig(
        windows=40,
        seed=42,
        burst_start_window=25,
        burst_windows=2,
        burst_multiplier=0.1,
    )
    summary = run_simulation(cfg, sink)
    assert len(summary.anomalies) >= 1
    assert all(f.direction == "drop" for f in summary.anomalies)


def test_sink_files_match_the_run(tmp_path):
    out = tmp_path / "out"
    sink = JsonlStatsSink(out)
    cfg = SimConfig(windows=40, seed=42, burst_start_window=25, burst_multiplier=3.0)
    summary = run_simulation(cfg, sink)
    series = sink.read_series(tail=1000)
    assert len(series) == summary.windows_processed == 40
    assert sum(s["count"] for s in series) == summary.events_total
    anomalies = sink.read_anomalies(tail=100)
    assert len(anomalies) == len(summary.anomalies)
    latest = sink.read_latest()
    assert latest["events_total"] == summary.events_total
    assert latest["windows_written"] == 40


def test_per_wiki_breakdown_reconciles_with_window_totals(tmp_path):
    sink = JsonlStatsSink(tmp_path / "out")
    run_simulation(SimConfig(windows=10, seed=42), sink)
    for record in sink.read_series(tail=100):
        assert record["counts_by_wiki"], "every window tracks its wikis"
        assert sum(record["counts_by_wiki"].values()) == record["count"]
    latest = sink.read_latest()
    assert sum(latest["counts_by_wiki"].values()) == (latest["last_window"]["count"])


def test_combined_mode_also_catches_the_burst(tmp_path):
    sink = JsonlStatsSink(tmp_path / "out", detector_mode="both")
    cfg = SimConfig(
        windows=40,
        seed=42,
        burst_start_window=25,
        burst_windows=2,
        burst_multiplier=3.0,
        detector_mode="both",
    )
    summary = run_simulation(cfg, sink)
    assert len(summary.anomalies) >= 1
    assert all(f.method == "zscore+ewma" for f in summary.anomalies)
    assert all(f.direction == "spike" for f in summary.anomalies)
