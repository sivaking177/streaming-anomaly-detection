"""Getting data in: the event model and the producer.

- :mod:`streamdetect.ingest.events`   -- recentchange parsing -> StreamEvent.
- :mod:`streamdetect.ingest.producer` -- SSE consumer / simulator -> Kafka,
  files, or stdout; dead-letter handling for malformed payloads.
"""

from .events import StreamEvent, parse_recentchange, parse_sse_data
from .producer import (
    DeadLetterSink,
    JsonlEventSink,
    KafkaSink,
    StdoutSink,
    SyntheticEventSource,
    WikimediaEventSource,
)

__all__ = [
    "DeadLetterSink",
    "JsonlEventSink",
    "KafkaSink",
    "StdoutSink",
    "StreamEvent",
    "SyntheticEventSource",
    "WikimediaEventSource",
    "parse_recentchange",
    "parse_sse_data",
]
