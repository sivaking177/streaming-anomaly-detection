"""Sink tests: latest.json must carry the per-wiki breakdown and the
detector mode, the Parquet copy must match the JSONL log it mirrors, and
the last flag must survive later clean windows."""

import json

import duckdb

from streamdetect.core.detection import AnomalyFlag
from streamdetect.core.windowing import WindowStats
from streamdetect.output.sink import JsonlStatsSink

BASE = 1_736_942_400.0


def _stats(start, count, by_wiki):
    return WindowStats(
        window_start=start,
        window_end=start + 60,
        count=count,
        mean_latency_ms=120.0,
        p95_latency_ms=240.0,
        max_latency_ms=900.0,
        counts_by_wiki=by_wiki,
    )


def test_latest_carries_wiki_breakdown_and_detector_mode(tmp_path):
    sink = JsonlStatsSink(tmp_path / "out", detector_mode="both")
    sink.write_window(_stats(BASE, 600, {"enwiki": 400, "dewiki": 200}))
    sink.write_window(_stats(BASE + 60, 650, {"enwiki": 650}))
    latest = sink.read_latest()
    assert latest["detector_mode"] == "both"
    assert latest["counts_by_wiki"] == {"enwiki": 650}
    assert latest["last_window"]["counts_by_wiki"] == {"enwiki": 650}
    assert latest["events_total"] == 1250


def test_parquet_mirror_matches_the_jsonl_log(tmp_path):
    out = tmp_path / "out"
    sink = JsonlStatsSink(out, enable_parquet=True)
    sink.write_window(_stats(BASE, 600, {"enwiki": 400, "dewiki": 200}))
    sink.write_window(_stats(BASE + 60, 650, {"enwiki": 650}))

    parquet = out / "window_stats.parquet"
    assert parquet.exists()
    rows = (
        duckdb.connect()
        .execute(
            "SELECT window_start, count, mean_latency_ms, counts_by_wiki "
            f"FROM read_parquet('{parquet}') ORDER BY window_start"
        )
        .fetchall()
    )
    assert len(rows) == 2
    assert rows[0][1] == 600 and rows[1][1] == 650
    assert rows[0][2] == 120.0
    # counts_by_wiki is stored as a JSON string (stable flat schema).
    assert json.loads(rows[0][3]) == {"dewiki": 200, "enwiki": 400}
    assert json.loads(rows[1][3]) == {"enwiki": 650}


def test_parquet_is_off_by_default(tmp_path):
    out = tmp_path / "out"
    sink = JsonlStatsSink(out)
    sink.write_window(_stats(BASE, 10, {}))
    assert not (out / "window_stats.parquet").exists()


def test_last_flag_survives_later_clean_windows(tmp_path):
    out = tmp_path / "out"
    sink = JsonlStatsSink(out)
    clean = _stats(BASE, 600, {"enwiki": 600})
    sink.write_window(clean)
    # No flag raised yet: last_flag is null.
    assert sink.read_latest()["last_flag"] is None

    burst = _stats(BASE + 60, 1842, {"enwiki": 1842})
    flag = AnomalyFlag(
        window_start=BASE + 60,
        observed=1842,
        expected=598.0,
        z_score=33.46,
        direction="spike",
        method="zscore",
    )
    sink.write_window(burst)
    sink.write_anomaly(flag, burst)
    assert sink.read_latest()["last_flag"]["window_start"] == BASE + 60

    # A later clean window rewrites latest.json but must not clear the flag.
    sink.write_window(_stats(BASE + 120, 590, {"enwiki": 590}))
    latest = sink.read_latest()
    assert latest["last_flag"] is not None
    assert latest["last_flag"]["window_start"] == BASE + 60
    assert latest["last_flag"]["direction"] == "spike"
