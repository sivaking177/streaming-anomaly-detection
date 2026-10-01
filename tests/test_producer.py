"""Producer-side parsing tests against a recorded-format recentchange
payload (tests/fixtures/sample_recentchange.json)."""

import json

import pytest

from streamdetect.ingest import producer as producer_module
from streamdetect.ingest.events import parse_recentchange, parse_sse_data
from streamdetect.ingest.producer import (
    DeadLetterSink,
    SyntheticEventSource,
    WikimediaEventSource,
)


def test_parse_full_recentchange_payload(sample_payload, sample_event_time):
    event = parse_recentchange(sample_payload, received_at=sample_event_time + 2.5)
    assert event.event_id == "a1b2c3d4-1111-4222-8333-444455556666"
    assert event.wiki == "enwiki"
    assert event.type == "edit"
    assert event.title == "Apache Spark"
    assert event.user == "DataEngineer42"
    assert event.bot is False
    assert event.namespace == 0
    assert event.event_time == sample_event_time
    assert event.latency_ms == 2500.0


def test_latency_is_clamped_at_zero(sample_payload, sample_event_time):
    # Clock skew between the wiki server and us must not produce negatives.
    event = parse_recentchange(sample_payload, received_at=sample_event_time - 5)
    assert event.latency_ms == 0.0


def test_event_time_falls_back_to_meta_dt(sample_payload, sample_event_time):
    payload = dict(sample_payload)
    payload.pop("timestamp")
    event = parse_recentchange(payload, received_at=sample_event_time)
    assert event.event_time == sample_event_time


def test_missing_id_is_rejected(sample_payload):
    payload = dict(sample_payload)
    payload["meta"] = {}
    payload.pop("id")
    with pytest.raises(ValueError):
        parse_recentchange(payload, received_at=0.0)


def test_missing_timestamp_is_rejected(sample_payload):
    payload = dict(sample_payload)
    payload.pop("timestamp")
    payload["meta"] = {"id": "x"}
    with pytest.raises(ValueError):
        parse_recentchange(payload, received_at=0.0)


def test_null_meta_and_namespace_do_not_crash_the_parser(sample_payload):
    # Regression test: real payloads carry explicit nulls. int(None) and
    # None.get() used to escape as TypeError/AttributeError, which the
    # producer's dead-letter handling (ValueError only) never caught --
    # one null field killed the stream.
    payload = dict(sample_payload)
    payload["meta"] = None  # id falls back to the top-level "id" field
    payload["namespace"] = None
    event = parse_recentchange(payload, received_at=0.0)
    assert event.namespace == 0
    # The top-level id in the recorded payload is an int; the parser
    # normalises ids to strings.
    assert event.event_id == str(sample_payload["id"])


def test_non_integer_namespace_is_a_value_error(sample_payload):
    payload = dict(sample_payload)
    payload["namespace"] = "not-a-number"
    with pytest.raises(ValueError):
        parse_recentchange(payload, received_at=0.0)


def test_event_schema_validates_types(sample_payload, sample_event_time):
    from streamdetect.ingest.events import StreamEvent

    event = parse_recentchange(sample_payload, received_at=sample_event_time)
    round_tripped = StreamEvent.from_dict(event.to_dict())
    # to_dict excludes the raw payload, so compare the serialised shape.
    assert round_tripped.to_dict() == event.to_dict()
    bad = event.to_dict()
    bad["event_time"] = "yesterday-ish"
    with pytest.raises(ValueError):
        StreamEvent.from_dict(bad)


def test_parse_sse_data_line(sample_payload, sample_event_time):
    line = "data: " + json.dumps(sample_payload)
    event = parse_sse_data(line, received_at=sample_event_time + 1.0)
    assert event is not None
    assert event.wiki == "enwiki"
    assert event.latency_ms == 1000.0


def test_parse_sse_heartbeat_and_garbage(sample_event_time):
    assert parse_sse_data("", received_at=sample_event_time) is None
    assert parse_sse_data("data: ", received_at=sample_event_time) is None
    assert parse_sse_data("data: not-json", received_at=sample_event_time) is None


class _StubResponse:
    """Minimal stand-in for a requests streaming response."""

    def __init__(self, lines):
        self._lines = lines

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_lines(self, decode_unicode=True):
        return iter(self._lines)


def test_malformed_payloads_are_dead_lettered_not_fatal(
    tmp_path, monkeypatch, sample_payload
):
    good_line = "data: " + json.dumps(sample_payload)
    # Valid JSON on the stream, but no id and no timestamp: a schema break.
    bad_line = 'data: {"wiki": "enwiki", "type": "edit"}'
    lines = [good_line, bad_line, ": heartbeat", good_line]
    monkeypatch.setattr(
        producer_module.requests,
        "get",
        lambda *a, **k: _StubResponse(lines),
    )
    dead_letter = DeadLetterSink(str(tmp_path / "dead.jsonl"))
    source = WikimediaEventSource(dead_letter=dead_letter)
    events = list(source.stream())
    dead_letter.close()

    assert len(events) == 2  # the stream survived the bad payload
    assert source.parse_errors == 1
    records = [
        json.loads(line) for line in (tmp_path / "dead.jsonl").read_text().splitlines()
    ]
    assert len(records) == 1
    assert "no id" in records[0]["error"]
    assert records[0]["raw"] == bad_line


def test_dead_lettering_is_logged(tmp_path, monkeypatch, sample_payload, caplog):
    import logging

    bad_line = 'data: {"wiki": "enwiki", "type": "edit"}'
    monkeypatch.setattr(
        producer_module.requests,
        "get",
        lambda *a, **k: _StubResponse([bad_line]),
    )
    source = WikimediaEventSource(
        dead_letter=DeadLetterSink(str(tmp_path / "dead.jsonl"))
    )
    with caplog.at_level(logging.WARNING, logger="streamdetect.ingest.producer"):
        assert list(source.stream()) == []
    assert any("dead-lettered" in r.message for r in caplog.records)


def test_dead_letter_sink_counts_and_records(tmp_path):
    sink = DeadLetterSink(str(tmp_path / "dl.jsonl"))
    sink.write("data: {...", "missing wiki/type", received_at=123.0)
    sink.close()
    record = json.loads((tmp_path / "dl.jsonl").read_text().strip())
    assert sink.count == 1
    assert record == {
        "received_at": 123.0,
        "error": "missing wiki/type",
        "raw": "data: {...",
    }


def test_synthetic_source_produces_events_at_rate():
    source = SyntheticEventSource(events_per_second=25.0, realtime=False, seed=3)
    events = list(source.stream(duration_seconds=2.0))
    assert 40 <= len(events) <= 60
    assert all(e.event_id.startswith("sim-") for e in events)
    assert len({e.event_id for e in events}) == len(events)
    assert all(e.latency_ms > 0 for e in events)


def test_synthetic_source_burst_increases_rate():
    source = SyntheticEventSource(
        events_per_second=25.0,
        burst_after_seconds=3.0,
        burst_duration_seconds=3.0,
        burst_multiplier=4.0,
        realtime=False,
        seed=3,
    )
    events = list(source.stream(duration_seconds=6.0))
    # 3s at 25/s + 3s at 100/s = ~375, vs ~150 without the burst.
    assert 340 <= len(events) <= 410
