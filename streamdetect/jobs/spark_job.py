"""Spark Structured Streaming job: Kafka -> window stats -> anomaly flags.

Reads normalised recentchange events (the producer's ``to_dict`` shape)
from a Kafka topic, computes 1-minute tumbling-window aggregates over
event time -- count, mean latency, p95 latency, broken down by wiki --
and judges each window with a detector from
:mod:`streamdetect.core.detection` (z-score, EWMA, or the combined
must-agree mode; ``--detector-mode``), writing results through the JSONL
sink the dashboard reads.

Run it with a Kafka connector package matching your Spark version, e.g.:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 \
        -m streamdetect.jobs.spark_job \
        --bootstrap-servers localhost:9092 \
        --topic wikimedia.recentchange \
        --output-dir ./out --checkpoint-dir ./checkpoint

Design notes:

- **Watermark**: 2 minutes on event time. Wikimedia events arrive slightly
  out of order; the watermark bounds how long a window stays open for late
  data before its aggregate is final.
- **Detection runs on the driver** inside ``foreachBatch``. Window
  aggregates are tiny (one row per window per wiki), so collecting them
  per batch is cheap, and judging windows in order on one machine keeps
  the detector's rolling baseline consistent. The trade-off is that a
  driver restart loses the baseline and the detector warms up again (5
  windows by default); a production hardening step would persist the
  baseline in the checkpoint or a state store.
- **Per-wiki grouping**: the aggregation groups by (window, wiki) so each
  window keeps its per-wiki counts; the driver folds the partial rows
  into one WindowStats with :func:`streamdetect.core.windowing.merge_window_partials`.
  Its docstring is honest about the one approximation this introduces:
  the merged p95 is the max of the per-wiki p95s, because exact
  percentiles don't combine.
- **Latency** is ingestion latency: Kafka record timestamp (broker-side
  ingest time) minus the event's own timestamp, clamped at zero.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from ..core.detection import DETECTOR_MODES, DetectorConfig, build_detector
from ..core.windowing import merge_window_partials
from ..output.sink import JsonlStatsSink

# Shape of StreamEvent.to_dict() as published by the producer.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("wiki", StringType()),
        StructField("type", StringType()),
        StructField("title", StringType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("event_time", DoubleType()),
        StructField("received_at", DoubleType()),
        StructField("latency_ms", DoubleType()),
        StructField("namespace", LongType()),
    ]
)


def build_spark(app_name: str = "wikimedia-anomaly-detection") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_windowed_stream(spark: SparkSession, args: argparse.Namespace):
    """Kafka source -> parsed events -> windowed aggregates (a DataFrame)."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )
    events = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        F.col("timestamp").alias("ingest_ts"),
    ).select(
        F.timestamp_seconds(F.col("e.event_time")).alias("event_time"),
        F.col("e.wiki").alias("wiki"),
        F.greatest(
            F.lit(0.0),
            (F.unix_timestamp(F.col("ingest_ts")) - F.col("e.event_time")) * 1000.0,
        ).alias("latency_ms"),
    )
    return (
        events.withWatermark("event_time", f"{args.watermark_seconds} seconds")
        .groupBy(
            F.window(F.col("event_time"), f"{args.window_seconds} seconds"),
            F.col("wiki"),
        )
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.avg("latency_ms").alias("mean_latency_ms"),
            F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
            F.max("latency_ms").alias("max_latency_ms"),
        )
    )


def make_batch_handler(sink: JsonlStatsSink, detector):
    """The foreachBatch function: fold the batch's per-wiki rows into one
    WindowStats per window, judge each window in order, write both out."""

    def process_batch(batch_df, batch_id: int) -> None:
        rows = batch_df.orderBy(F.col("window.start")).collect()
        partials_by_window: dict[float, list[dict]] = {}
        for row in rows:
            partial = {
                "window_start": row["window"]["start"].timestamp(),
                "window_end": row["window"]["end"].timestamp(),
                "wiki": row["wiki"],
                "count": int(row["count"]),
                "mean_latency_ms": float(row["mean_latency_ms"] or 0.0),
                "p95_latency_ms": float(row["p95_latency_ms"] or 0.0),
                "max_latency_ms": float(row["max_latency_ms"] or 0.0),
            }
            partials_by_window.setdefault(partial["window_start"], []).append(partial)
        for window_start in sorted(partials_by_window):
            stats = merge_window_partials(partials_by_window[window_start])
            sink.write_window(stats)
            flag = detector.observe(stats.window_start, stats.count)
            if flag is not None:
                sink.write_anomaly(flag, stats)

    return process_batch


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="spark_job", description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="wikimedia.recentchange")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--checkpoint-dir", required=True)
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--watermark-seconds", type=int, default=120)
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--z-threshold", type=float, default=3.0)
    parser.add_argument("--warmup-windows", type=int, default=5)
    parser.add_argument("--detector-mode", choices=DETECTOR_MODES, default="zscore")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark()
    try:
        windowed = build_windowed_stream(spark, args)
        sink = JsonlStatsSink(args.output_dir, detector_mode=args.detector_mode)
        detector = build_detector(
            DetectorConfig(
                mode=args.detector_mode,
                warmup_windows=args.warmup_windows,
                z_threshold=args.z_threshold,
            )
        )
        query = (
            windowed.writeStream.foreachBatch(make_batch_handler(sink, detector))
            .option("checkpointLocation", args.checkpoint_dir)
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
