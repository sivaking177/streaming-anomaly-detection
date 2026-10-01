"""Dashboard tests: spin the real HTTP server up on an ephemeral port,
backed by a fixture sink directory, and exercise its endpoints."""

import json
import threading
import urllib.error
import urllib.request

import pytest

from streamdetect.core.detection import AnomalyFlag
from streamdetect.core.windowing import WindowStats
from streamdetect.output.dashboard import make_server
from streamdetect.output.sink import JsonlStatsSink

BASE = 1_736_942_400.0


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()


@pytest.fixture()
def server(tmp_path):
    sink = JsonlStatsSink(tmp_path / "sink", detector_mode="both")
    for i in range(3):
        sink.write_window(
            WindowStats(
                window_start=BASE + i * 60,
                window_end=BASE + (i + 1) * 60,
                count=600 + i,
                mean_latency_ms=120.0,
                p95_latency_ms=240.0,
                max_latency_ms=900.0,
                counts_by_wiki={"enwiki": 400, "dewiki": 200 + i},
            )
        )
    stats = WindowStats(BASE + 180, BASE + 240, 1800, 190.0, 380.0, 1500.0)
    sink.write_window(stats)
    sink.write_anomaly(
        AnomalyFlag(BASE + 180, 1800, 600.5, 12.3, "spike", "zscore"), stats
    )
    httpd = make_server(tmp_path / "sink", host="127.0.0.1", port=0)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    yield f"http://{host}:{port}"
    httpd.shutdown()
    httpd.server_close()


def test_stats_endpoint_returns_series_and_anomalies(server):
    status, body = _get(server + "/api/stats")
    assert status == 200
    payload = json.loads(body)
    assert len(payload["series"]) == 4
    assert len(payload["anomalies"]) == 1
    assert payload["anomalies"][0]["direction"] == "spike"
    assert payload["latest"]["events_total"] == 600 + 601 + 602 + 1800
    assert payload["latest"]["last_window"]["count"] == 1800


def test_stats_payload_carries_wiki_breakdown_and_mode(server):
    status, body = _get(server + "/api/stats")
    assert status == 200
    payload = json.loads(body)
    assert payload["latest"]["detector_mode"] == "both"
    # Last window written by the fixture is the burst window (no wiki map).
    assert payload["series"][0]["counts_by_wiki"]["enwiki"] == 400


def test_index_serves_dashboard_html(server):
    status, body = _get(server + "/")
    assert status == 200
    assert b"anomaly monitor" in body.lower()
    assert b"/api/stats" in body
    # The new dashboard sections are in the page.
    assert b"Events by wiki" in body
    assert b"Flagged-window history" in body


def test_health_endpoint(server):
    status, body = _get(server + "/api/health")
    assert status == 200
    assert json.loads(body) == {"status": "ok"}


def test_unknown_path_is_404(server):
    status, _ = _get(server + "/nope")
    assert status == 404
