"""Event model and Wikimedia recentchange parsing.

The producer consumes the Wikimedia EventStreams SSE feed
(``https://stream.wikimedia.org/v2/stream/recentchange``), whose payloads
follow the ``mediawiki/recentchange`` schema: a ``meta`` envelope (stream,
domain, event time ``dt``, unique ``id``) plus the change itself (``type``,
``wiki``, ``title``, ``user``, ``bot``, unix ``timestamp``, ...).

:func:`parse_recentchange` normalises one payload into a :class:`StreamEvent`
and derives the ingestion latency (wall-clock receive time minus event
time), which is the latency the pipeline later aggregates per window.

:class:`StreamEvent` is a pydantic model on purpose: it is the schema
every downstream stage trusts, so the boundary is where types get
checked. Bad values surface as :class:`ValueError` (pydantic's
``ValidationError`` is one), which is exactly what the producer's
dead-letter handling catches.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StreamEvent(BaseModel):
    """One normalised change event."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    wiki: str
    type: str  # edit | new | log | categorize | external
    title: str
    user: str
    bot: bool
    event_time: float  # epoch seconds, when the change happened
    received_at: float  # epoch seconds, when the producer received it
    latency_ms: float  # received_at - event_time, clamped at 0
    namespace: int = 0
    # The original payload, kept for debugging; not serialised downstream.
    raw: dict[str, Any] = Field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "wiki": self.wiki,
            "type": self.type,
            "title": self.title,
            "user": self.user,
            "bot": self.bot,
            "event_time": self.event_time,
            "received_at": self.received_at,
            "latency_ms": self.latency_ms,
            "namespace": self.namespace,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StreamEvent:
        return cls.model_validate(data)


def _meta(payload: dict[str, Any]) -> dict[str, Any]:
    """The ``meta`` envelope, tolerating an explicit null (real feeds do
    send those, and ``payload.get("meta", {})`` does not default on null)."""
    meta = payload.get("meta")
    return meta if isinstance(meta, dict) else {}


def _parse_event_time(payload: dict[str, Any]) -> float:
    """Event time from the unix ``timestamp`` field, falling back to
    ``meta.dt`` (ISO-8601) when the former is absent."""
    ts = payload.get("timestamp")
    if isinstance(ts, (int, float)):
        return float(ts)
    dt = _meta(payload).get("dt")
    if isinstance(dt, str):
        parsed = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    raise ValueError("recentchange payload has no timestamp/meta.dt")


def parse_recentchange(payload: dict[str, Any], received_at: float) -> StreamEvent:
    """Normalise one raw recentchange payload.

    Raises :class:`ValueError` for payloads missing the fields the pipeline
    depends on (id, wiki, type, a usable timestamp) -- the producer counts
    those and writes them to its dead-letter file rather than crashing the
    stream (see :class:`streamdetect.ingest.producer.DeadLetterSink`).
    """
    event_id = _meta(payload).get("id") or payload.get("id")
    if event_id is None:
        raise ValueError("recentchange payload has no id")
    wiki = payload.get("wiki")
    change_type = payload.get("type")
    if not wiki or not change_type:
        raise ValueError("recentchange payload missing wiki/type")

    event_time = _parse_event_time(payload)
    latency_ms = max(0.0, (received_at - event_time) * 1000.0)
    # namespace arrives as null often enough that the coercion has to be
    # defensive: an unparseable value becomes a ValueError here (which
    # the producer dead-letters) instead of a TypeError escaping it.
    raw_namespace = payload.get("namespace")
    try:
        namespace = int(raw_namespace) if raw_namespace is not None else 0
    except (TypeError, ValueError):
        raise ValueError(
            f"recentchange namespace is not an integer: {raw_namespace!r}"
        ) from None
    return StreamEvent(
        event_id=str(event_id),
        wiki=wiki,
        type=change_type,
        title=payload.get("title", ""),
        user=payload.get("user", ""),
        bot=bool(payload.get("bot", False)),
        event_time=event_time,
        received_at=received_at,
        latency_ms=latency_ms,
        namespace=namespace,
        raw=payload,
    )


def parse_sse_data(line: str, received_at: float) -> StreamEvent | None:
    """Parse one SSE ``data:`` line; ``None`` for heartbeats/non-JSON."""
    text = line.strip()
    if text.startswith("data:"):
        text = text[len("data:") :].strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return parse_recentchange(payload, received_at)
