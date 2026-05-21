"""Observability: structured JSON logging + Prometheus metrics.

Usage (in app/main.py):
    from app.observability import configure_logging, PrometheusMiddleware
    configure_logging()
    app.add_middleware(PrometheusMiddleware)

Prometheus scrape endpoint (in app/routers/admin.py):
    Served at GET /api/prometheus  (and /api/prometheus/metrics as alias).
    Moved from /metrics to avoid conflicting with the admin SPA page route.
"""
import logging
import time
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send
from prometheus_client import (
    Counter, Histogram, REGISTRY,
    generate_latest, CONTENT_TYPE_LATEST,
)
from prometheus_client.metrics_core import GaugeMetricFamily

# ── Prometheus metrics ────────────────────────────────────────────────────────

http_requests_total = Counter(
    "hiverunr_http_requests_total",
    "Total HTTP requests by method, path template, and status code",
    ["method", "path", "status_code"],
)

http_request_duration_seconds = Histogram(
    "hiverunr_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)


class _RunMetricsCollector:
    """On-demand collector — queries the DB each time /metrics is scraped.

    Avoids multi-process counter sync issues: the web process simply reads
    the authoritative run counts from PostgreSQL rather than maintaining
    in-process counters that can diverge from Celery worker increments.
    """
    def collect(self):
        try:
            import psycopg2.extras
            from app.core.db import get_conn
            with get_conn() as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("SELECT status, COUNT(*) AS n FROM runs GROUP BY status")
                rows = cur.fetchall()
            g = GaugeMetricFamily(
                "hiverunr_runs_total",
                "Total workflow runs by status (all-time)",
                labels=["status"],
            )
            for row in rows:
                g.add_metric([row["status"]], float(row["n"]))
            yield g
        except (OSError, KeyError, ValueError, psycopg2.Error, AttributeError, Exception):
            # psycopg2.errors.UndefinedTable inherits from Exception (not psycopg2.Error)
            # — catch it gracefully so /prometheus scrape never breaks the metrics endpoint
            return  # never let a scrape error break the collector


REGISTRY.register(_RunMetricsCollector())


class PrometheusMiddleware:
    """Count and time every HTTP request; attach to FastAPI via add_middleware.

    Implemented as a *pure ASGI middleware* (not BaseHTTPMiddleware) so that
    exceptions raised by route handlers propagate transparently to uvicorn's
    error logger.  BaseHTTPMiddleware wraps call_next in a TaskGroup which
    catches and re-wraps exceptions — that caused silent 500s with no log trace.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Only instrument HTTP requests; pass WebSocket / lifespan through unchanged.
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start       = time.perf_counter()
        status_code = 500  # default if handler raises before sending a response

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            # Record metrics even when the handler raises — the finally block
            # runs before the exception propagates to uvicorn.
            duration = time.perf_counter() - start

            # Use the matched route template to avoid per-ID label cardinality:
            #   /api/graphs/123  →  /api/graphs/{graph_id}
            request = Request(scope)
            route   = scope.get("route")
            path    = route.path if route else request.url.path

            http_requests_total.labels(
                method=request.method,
                path=path,
                status_code=str(status_code),
            ).inc()
            http_request_duration_seconds.labels(
                method=request.method,
                path=path,
            ).observe(duration)


def metrics_response() -> Response:
    """Return the full Prometheus text exposition."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ── Structured JSON logging ───────────────────────────────────────────────────

def configure_logging(level: int = logging.INFO) -> None:
    """Replace the root handler with a structured JSON formatter.

    Every log record emitted anywhere in the process will be serialised as
    a single JSON line, making it trivially parseable by Loki, CloudWatch,
    Datadog, etc.

    Falls back silently to plain logging if python-json-logger is not
    installed, so the app starts cleanly even without the library.
    """
    try:
        from pythonjsonlogger.json import JsonFormatter as _JsonFormatter
    except ImportError:
        logging.basicConfig(level=level)
        logging.getLogger(__name__).warning(
            "python-json-logger not installed — falling back to plain logging"
        )
        return

    class _Formatter(_JsonFormatter):
        def add_fields(self, log_record, record, message_dict):
            super().add_fields(log_record, record, message_dict)
            log_record.setdefault("service", "hiverunr")
            log_record["level"] = record.levelname.lower()
            # Remove redundant keys added by the base class
            log_record.pop("levelname", None)
            log_record.pop("color_message", None)

    handler = logging.StreamHandler()
    handler.setFormatter(
        _Formatter("%(asctime)s %(name)s %(level)s %(message)s")
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Keep uvicorn's access log quiet (it's redundant when Prometheus tracks requests)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
