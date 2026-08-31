"""Producer: Wikimedia EventStreams -> Kafka (or a file/stdout sink).

Two modes:

- ``live`` -- subscribe to the public SSE stream
  ``https://stream.wikimedia.org/v2/stream/recentchange`` and publish each
  normalised event to a Kafka topic. Reconnects with backoff when the
  stream drops (it does, regularly).
- ``simulate`` -- generate synthetic recentchange-shaped events at a
  configurable rate, with an optional injected burst, so the whole pipeline
  can be exercised without the live stream. Synthetic events are shaped
  exactly like real ones; you can tell them apart by their event IDs
  (``sim-<counter>``) and user names (``sim-user-<n>``). Nothing in the
  pipeline treats them specially otherwise.

Malformed payloads -- valid stream data that fails schema validation
(missing id, no usable timestamp) -- never crash the producer. They are
counted, and when a dead-letter file is configured they are written to it
verbatim (raw line + error + receive time), so a schema change upstream
shows up as a growing dead-letter file instead of a silent data gap or a
dead process. Non-JSON noise on the stream (heartbeats, comments) is not
a dead letter; that's normal stream chatter, not bad data.

Kafka is an optional dependency at import time: the file/stdout sinks work
without ``kafka-python`` installed.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
from collections.abc import Iterator
from typing import Protocol

import requests

from .events import StreamEvent, parse_sse_data

logger = logging.getLogger(__name__)

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "streaming-anomaly-detection/0.1 (personal project; python-requests)"

_WIKIS = ["enwiki", "dewiki", "frwiki", "jawiki", "eswiki", "commonswiki"]
_CHANGE_TYPES = ["edit", "edit", "edit", "new", "categorize", "log"]


class EventSink(Protocol):
    def send(self, event: StreamEvent) -> None: ...
    def close(self) -> None: ...


class DeadLetterSink:
    """Where malformed payloads go: a JSON-lines file, one record per bad
    payload, holding the raw stream line, the validation error, and when
    it arrived. The point is evidence -- when upstream changes its schema,
    this file is how you find out what broke and what you skipped."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.count = 0
        # The sink owns this handle for its whole
        # lifetime and closes it in close() -- a with-block cannot span
        # the class's write calls.
        self._fh = open(path, "a", encoding="utf-8")  # noqa: SIM115

    def write(self, raw_line: str, error: str, received_at: float) -> None:
        record = {
            "received_at": received_at,
            "error": error,
            "raw": raw_line.strip(),
        }
        self._fh.write(json.dumps(record) + "\n")
        self._fh.flush()
        self.count += 1

    def close(self) -> None:
        self._fh.close()


class StdoutSink:
    def send(self, event: StreamEvent) -> None:
        print(json.dumps(event.to_dict()))

    def close(self) -> None:
        pass


class JsonlEventSink:
    """Appends normalised events as JSON lines (a poor man's topic)."""

    def __init__(self, path: str) -> None:
        # The sink owns this handle for its whole
        # lifetime and closes it in close() -- a with-block cannot span
        # the class's write calls.
        self._fh = open(path, "a", encoding="utf-8")  # noqa: SIM115

    def send(self, event: StreamEvent) -> None:
        self._fh.write(json.dumps(event.to_dict()) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class KafkaSink:
    """Publishes events to a Kafka topic (requires kafka-python)."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        from kafka import KafkaProducer  # lazy: optional dependency

        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    def send(self, event: StreamEvent) -> None:
        self._producer.send(self._topic, key=event.wiki, value=event.to_dict())

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class WikimediaEventSource:
    """Reads the live recentchange SSE stream, with reconnect/backoff."""

    def __init__(
        self,
        url: str = WIKIMEDIA_STREAM_URL,
        max_retries: int = 5,
        backoff_seconds: float = 2.0,
        dead_letter: DeadLetterSink | None = None,
    ) -> None:
        self.url = url
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.dead_letter = dead_letter
        self.parse_errors = 0

    def stream(self, max_events: int | None = None) -> Iterator[StreamEvent]:
        produced = 0
        attempts = 0
        while attempts <= self.max_retries:
            try:
                with requests.get(
                    self.url,
                    stream=True,
                    timeout=(10, 30),
                    headers={
                        "Accept": "text/event-stream",
                        "User-Agent": USER_AGENT,
                    },
                ) as response:
                    response.raise_for_status()
                    for raw_line in response.iter_lines(decode_unicode=True):
                        # requests is typed to yield str | bytes here even
                        # with decode_unicode=True; normalise defensively.
                        line = (
                            raw_line.decode("utf-8", "replace")
                            if isinstance(raw_line, bytes)
                            else raw_line
                        )
                        if not line or not line.startswith("data:"):
                            continue
                        received_at = time.time()
                        try:
                            event = parse_sse_data(line, received_at)
                        except ValueError as exc:
                            # A payload that made it onto the stream but
                            # fails validation: count it and dead-letter
                            # it, but never let it kill the stream.
                            self.parse_errors += 1
                            logger.warning("malformed payload dead-lettered: %s", exc)
                            if self.dead_letter is not None:
                                self.dead_letter.write(line, str(exc), received_at)
                            continue
                        if event is None:
                            continue
                        yield event
                        produced += 1
                        if max_events is not None and produced >= max_events:
                            return
                return  # clean end of stream
            except requests.RequestException as exc:
                attempts += 1
                if attempts > self.max_retries:
                    raise
                logger.warning(
                    "stream dropped (%s); reconnect attempt %d in %.0fs",
                    exc,
                    attempts,
                    self.backoff_seconds * attempts,
                )
                time.sleep(self.backoff_seconds * attempts)


class SyntheticEventSource:
    """Generates recentchange-shaped events at a configurable rate.

    ``burst_after_seconds`` + ``burst_multiplier`` inject an anomaly burst:
    after the given elapsed time the rate multiplies for
    ``burst_duration_seconds``. With ``realtime=True`` the generator sleeps
    to hold the requested rate; otherwise it produces as fast as possible
    (event timestamps still advance at the simulated rate).
    """

    def __init__(
        self,
        events_per_second: float = 25.0,
        burst_after_seconds: float | None = None,
        burst_duration_seconds: float = 120.0,
        burst_multiplier: float = 3.0,
        realtime: bool = True,
        seed: int = 7,
    ) -> None:
        self.events_per_second = events_per_second
        self.burst_after_seconds = burst_after_seconds
        self.burst_duration_seconds = burst_duration_seconds
        self.burst_multiplier = burst_multiplier
        self.realtime = realtime
        self._rng = random.Random(seed)
        self._counter = 0

    def _rate_at(self, elapsed: float) -> float:
        if (
            self.burst_after_seconds is not None
            and self.burst_after_seconds
            <= elapsed
            < self.burst_after_seconds + self.burst_duration_seconds
        ):
            return self.events_per_second * self.burst_multiplier
        return self.events_per_second

    def stream(self, duration_seconds: float = 300.0) -> Iterator[StreamEvent]:
        rng = self._rng
        start_wall = time.time()
        event_time = start_wall
        while event_time - start_wall < duration_seconds:
            elapsed = event_time - start_wall
            rate = self._rate_at(elapsed)
            interval = 1.0 / rate
            event_time += interval
            self._counter += 1
            latency_ms = max(5.0, rng.gauss(900.0, 350.0))
            received_at = event_time + latency_ms / 1000.0
            yield StreamEvent(
                event_id=f"sim-{self._counter}",
                wiki=rng.choice(_WIKIS),
                type=rng.choice(_CHANGE_TYPES),
                title=f"Simulated_Page_{rng.randint(1, 50_000)}",
                user=f"sim-user-{rng.randint(1, 5_000)}",
                bot=rng.random() < 0.12,
                event_time=event_time,
                received_at=received_at,
                latency_ms=latency_ms,
                namespace=rng.choice([0, 0, 0, 1, 6, 14]),
            )
            if self.realtime:
                time.sleep(interval)


def _build_sink(args: argparse.Namespace) -> EventSink:
    if args.bootstrap_servers:
        return KafkaSink(args.bootstrap_servers, args.topic)
    if args.out:
        return JsonlEventSink(args.out)
    return StdoutSink()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="producer", description=__doc__)
    parser.add_argument("--mode", choices=["live", "simulate"], default="live")
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Kafka bootstrap servers; omit to use --out/stdout",
    )
    parser.add_argument("--topic", default="wikimedia.recentchange")
    parser.add_argument("--out", default=None, help="JSONL file sink path")
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument(
        "--dead-letter",
        default=None,
        help="live mode: file for malformed payloads "
        "(JSON lines); without it they are only counted",
    )
    parser.add_argument(
        "--rate", type=float, default=25.0, help="simulate mode: events per second"
    )
    parser.add_argument(
        "--burst-after",
        type=float,
        default=None,
        help="simulate mode: seconds until the injected burst",
    )
    parser.add_argument("--burst-multiplier", type=float, default=3.0)
    parser.add_argument(
        "--no-realtime",
        action="store_true",
        help="simulate mode: generate without sleeping",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    sink = _build_sink(args)
    dead_letter = DeadLetterSink(args.dead_letter) if args.dead_letter else None
    sent = 0
    parse_errors = 0
    try:
        if args.mode == "live":
            live_source = WikimediaEventSource(dead_letter=dead_letter)
            events = live_source.stream(max_events=args.max_events)
        else:
            synthetic_source = SyntheticEventSource(
                events_per_second=args.rate,
                burst_after_seconds=args.burst_after,
                burst_multiplier=args.burst_multiplier,
                realtime=not args.no_realtime,
            )
            events = synthetic_source.stream(duration_seconds=args.duration_seconds)
        for event in events:
            sink.send(event)
            sent += 1
        if args.mode == "live":
            parse_errors = live_source.parse_errors
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()
        if dead_letter is not None:
            dead_letter.close()
    print(f"produced {sent} events")
    if parse_errors:
        print(
            f"malformed payloads skipped: {parse_errors} "
            f"(dead-lettered: {dead_letter.count if dead_letter else 0})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
