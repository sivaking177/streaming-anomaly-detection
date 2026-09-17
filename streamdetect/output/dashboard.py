"""Live monitoring dashboard: a tiny stdlib HTTP server over the sink.

Endpoints:

- ``GET /``           -- the single-file dashboard (auto-refreshes).
- ``GET /api/stats``  -- JSON: {latest, series, anomalies} read from the
  sink directory, so the dashboard works with output from either the Spark
  job or the local simulation.
- ``GET /api/health`` -- {"status": "ok"}.

No framework and no build step: ``http.server`` plus one HTML file. For a
single-operator monitoring view that is the right amount of machinery.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .sink import JsonlStatsSink

HTML_PATH = Path(__file__).resolve().parent / "dashboard.html"


def build_stats_payload(sink: JsonlStatsSink) -> dict[str, Any]:
    return {
        "latest": sink.read_latest(),
        "series": sink.read_series(tail=120),
        "anomalies": sink.read_anomalies(tail=50),
    }


def make_handler(sink: JsonlStatsSink) -> type[BaseHTTPRequestHandler]:
    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "StreamDetectDashboard/0.1"

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            if self.path == "/":
                body = HTML_PATH.read_bytes()
                self._send(200, "text/html; charset=utf-8", body)
            elif self.path == "/api/stats":
                payload = json.dumps(build_stats_payload(sink)).encode()
                self._send(200, "application/json", payload)
            elif self.path == "/api/health":
                self._send(200, "application/json", b'{"status": "ok"}')
            else:
                self._send(404, "text/plain", b"not found")

        def log_message(self, format: str, *args: Any) -> None:
            pass  # keep the console clean; this is a local dashboard

    return DashboardHandler


def make_server(
    sink_dir: str | Path, host: str = "127.0.0.1", port: int = 8765
) -> ThreadingHTTPServer:
    sink = JsonlStatsSink(sink_dir)
    return ThreadingHTTPServer((host, port), make_handler(sink))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dashboard", description=__doc__)
    parser.add_argument("--sink-dir", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    server = make_server(args.sink_dir, args.host, args.port)
    print(f"dashboard on http://{args.host}:{args.port} (sink: {args.sink_dir})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
