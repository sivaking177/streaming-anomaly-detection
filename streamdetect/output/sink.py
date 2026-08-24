"""Output sink: JSON-lines files the dashboard reads, plus a Parquet copy.

Layout inside the sink directory:

- ``window_stats.jsonl`` -- one :class:`~streamdetect.core.windowing.WindowStats`
  per line, in window order.
- ``anomalies.jsonl`` -- one :class:`~streamdetect.core.detection.AnomalyFlag`
  (joined with its window's latency stats) per line.
- ``latest.json`` -- a small summary document (most recent window with its
  per-wiki counts, the detector mode in use, the most recent anomaly flag,
  totals) rewritten on every window, so the dashboard can render the header
  numbers without scanning the logs. The flag entry keeps the *latest*
  flag until a newer one replaces it -- a clean window does not clear it;
  it is null only until the first flag is raised.
- ``window_stats.parquet`` -- only when the sink is created with
  ``enable_parquet=True``: the same window stats as a Parquet file, for
  anyone who wants to analyse a run in pandas/DuckDB/Spark instead of
  reading JSON lines. It is rebuilt from the JSONL log on each window --
  simple and always consistent with the log; fine at one row per minute,
  not a pattern I'd use for high-rate streams.

JSON lines are append-only, which is what you want from a streaming sink:
a crash loses at most the in-flight window, and consumers can tail the
files. ``latest.json`` is written via a temp file + rename so readers never
see a half-written document.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..core.detection import AnomalyFlag
from ..core.windowing import WindowStats

WINDOW_STATS_FILE = "window_stats.jsonl"
ANOMALIES_FILE = "anomalies.jsonl"
LATEST_FILE = "latest.json"
PARQUET_FILE = "window_stats.parquet"


class JsonlStatsSink:
    def __init__(
        self,
        directory: str | Path,
        detector_mode: str = "zscore",
        enable_parquet: bool = False,
    ) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._stats_path = self.directory / WINDOW_STATS_FILE
        self._anomalies_path = self.directory / ANOMALIES_FILE
        self._latest_path = self.directory / LATEST_FILE
        self._parquet_path = self.directory / PARQUET_FILE
        # Recorded in latest.json so the dashboard (and anyone reading the
        # output later) knows which detector produced these flags.
        self.detector_mode = detector_mode
        self._enable_parquet = enable_parquet
        self._windows_written = 0
        self._anomalies_written = 0
        self._events_total = 0
        # The most recent flag raised, kept so latest.json can keep
        # showing it across later clean windows (see _write_latest).
        self._last_flag: AnomalyFlag | None = None

    def write_window(self, stats: WindowStats) -> None:
        with self._stats_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(stats.to_dict()) + "\n")
        self._windows_written += 1
        self._events_total += stats.count
        if self._enable_parquet:
            self._refresh_parquet()
        self._write_latest(stats, flag=None)

    def write_anomaly(self, flag: AnomalyFlag, stats: WindowStats) -> None:
        record = flag.to_dict()
        record.update(
            {
                "mean_latency_ms": stats.mean_latency_ms,
                "p95_latency_ms": stats.p95_latency_ms,
            }
        )
        with self._anomalies_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
        self._anomalies_written += 1
        self._last_flag = flag
        self._write_latest(stats, flag=flag)

    def _write_latest(self, stats: WindowStats, flag: AnomalyFlag | None) -> None:
        # last_flag is the most recent flag raised, not "the flag of the
        # window just written": write_window passes flag=None for clean
        # windows, and the stored flag must survive those.
        shown_flag = flag if flag is not None else self._last_flag
        latest = {
            "last_window": stats.to_dict(),
            # The last window's per-wiki counts, lifted to the top level
            # because it's the number people actually look for.
            "counts_by_wiki": stats.counts_by_wiki,
            "detector_mode": self.detector_mode,
            "last_flag": shown_flag.to_dict() if shown_flag else None,
            "windows_written": self._windows_written,
            "anomalies_written": self._anomalies_written,
            "events_total": self._events_total,
        }
        tmp = self._latest_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(latest, indent=2), encoding="utf-8")
        os.replace(tmp, self._latest_path)

    def _refresh_parquet(self) -> None:
        """Rebuild window_stats.parquet from the JSONL log.

        Done with DuckDB (already a project dependency for the sister
        tooling, and the obvious way to write Parquet from Python here).
        ``counts_by_wiki`` is stored as a JSON string on purpose: the set
        of wikis changes from window to window, and a stable flat schema
        beats a clever one for a file people will query later.
        """
        import duckdb  # local import: only the Parquet path needs it

        rows = self._read_jsonl(self._stats_path, tail=1_000_000)
        con = duckdb.connect(database=":memory:")
        try:
            con.execute(
                "CREATE TABLE window_stats ("
                "window_start DOUBLE, window_end DOUBLE, count BIGINT, "
                "mean_latency_ms DOUBLE, p95_latency_ms DOUBLE, "
                "max_latency_ms DOUBLE, counts_by_wiki VARCHAR)"
            )
            con.executemany(
                "INSERT INTO window_stats VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        float(r["window_start"]),
                        float(r["window_end"]),
                        int(r["count"]),
                        float(r["mean_latency_ms"]),
                        float(r["p95_latency_ms"]),
                        float(r["max_latency_ms"]),
                        json.dumps(r.get("counts_by_wiki", {}), sort_keys=True),
                    )
                    for r in rows
                ],
            )
            parquet_path = str(self._parquet_path).replace("'", "''")
            con.execute(f"COPY window_stats TO '{parquet_path}' (FORMAT PARQUET)")
        finally:
            con.close()

    # ------------------------------------------------------------------
    # read side (used by the dashboard)
    # ------------------------------------------------------------------
    @staticmethod
    def _read_jsonl(path: Path, tail: int) -> list[dict]:
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        records = []
        for line in lines[-tail:]:
            line = line.strip()
            if line:
                records.append(json.loads(line))
        return records

    def read_series(self, tail: int = 120) -> list[dict]:
        return self._read_jsonl(self._stats_path, tail)

    def read_anomalies(self, tail: int = 50) -> list[dict]:
        return self._read_jsonl(self._anomalies_path, tail)

    def read_latest(self) -> dict:
        if not self._latest_path.exists():
            return {}
        return json.loads(self._latest_path.read_text(encoding="utf-8"))
