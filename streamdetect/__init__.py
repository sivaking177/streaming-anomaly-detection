"""Streaming anomaly detection over the Wikimedia recentchange stream.

Pipeline shape (each stage is independently testable):

    producer  ->  Kafka topic  ->  Spark Structured Streaming job
    (live SSE      raw events       1-minute window aggregates)
     or simulate)                        |
                                         v
                              detection (pure Python):
                              z-score / EWMA / combined-agreement
                              on window counts, latency stats and
                              a per-wiki breakdown per window
                                         |
                                         v
                              sink (JSON lines + latest.json,
                              optional Parquet copy)
                                         |
                                         v
                              dashboard (stdlib HTTP server)

Malformed stream payloads never crash the producer: they are counted and
written to a dead-letter file (see ``producer.DeadLetterSink``).

The detection and windowing code is deliberately free of Spark/Kafka
imports: the Spark job calls it from ``foreachBatch`` on the driver, and
the test-suite exercises it directly plus through a local micro-batch
simulation, so the logic is verified even where no JVM/broker exists.
"""

__version__ = "0.1.0"
