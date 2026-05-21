"""OpenTelemetry distributed tracing setup.

Zero-overhead design
────────────────────
When OTEL_EXPORTER_OTLP_ENDPOINT is **not** set, setup_tracing() returns
immediately without touching the SDK.  All get_tracer() calls return the
opentelemetry-api's built-in NoopTracer — instrumented code compiles and
runs with negligible overhead (a dict lookup and a noop context manager).

When OTEL_EXPORTER_OTLP_ENDPOINT **is** set:
  • SDK TracerProvider is installed globally via trace.set_tracer_provider()
  • Spans are exported over OTLP/HTTP (compatible with Jaeger ≥1.35,
    Grafana Tempo, and any OpenTelemetry Collector)
  • An optional ConsoleSpanExporter is added when OTEL_LOG_LEVEL=debug

Usage
─────
    # In each process entry point (main.py / worker.py / scheduler.py):
    from app.telemetry import setup_tracing
    setup_tracing()                  # idempotent; safe to call multiple times

    # In any module that wants to create spans:
    from app.telemetry import get_tracer
    tracer = get_tracer("hiverunr.executor")
    with tracer.start_as_current_span("run_graph") as span:
        span.set_attribute("graph.id", graph_id)
        ...

Environment variables
─────────────────────
  OTEL_SERVICE_NAME              Service name tag  (default: hiverunr)
  OTEL_EXPORTER_OTLP_ENDPOINT    OTLP/HTTP collector URL, e.g.:
                                   http://jaeger:4318          ← Jaeger all-in-one
                                   http://otel-collector:4318  ← OpenTelemetry Collector
                                 When unset, tracing is completely disabled.
  OTEL_EXPORTER_OTLP_HEADERS     Optional comma-separated k=v pairs, e.g.:
                                   Authorization=Bearer <token>
  OTEL_LOG_LEVEL                 Set to "debug" to also print spans to stdout
"""
from __future__ import annotations  # keep annotations as strings — avoids AttributeError on trace.Tracer at load time

import logging
import os
import types

log = logging.getLogger(__name__)

# ── opentelemetry-api is an optional dependency ───────────────────────────────
# If the package is not installed we build a minimal noop shim so that
# all callers (worker.py, executor.py, scheduler.py) can import and use
# get_tracer() / setup_tracing() without modification regardless of whether
# the package is present.
try:
    from opentelemetry import trace          # noqa: F401  (re-exported below)
    from opentelemetry import context as _otel_ctx_mod
    from opentelemetry import trace  as _otel_trace_mod
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False

    # ── Minimal noop shim ─────────────────────────────────────────────────────
    class _NoopSpan:
        """Stands in for opentelemetry.trace.Span / NonRecordingSpan."""
        def set_attribute(self, *a, **kw): pass
        def set_status(self, *a, **kw):    pass
        def record_exception(self, *a, **kw): pass
        def end(self, *a, **kw):           pass
        def __enter__(self):               return self
        def __exit__(self, *a):            return False

    class _NoopTracer:
        def start_span(self, name, **kw):  return _NoopSpan()
        def start_as_current_span(self, name, **kw):
            import contextlib
            @contextlib.contextmanager
            def _cm():
                yield _NoopSpan()
            return _cm()

    class _NoopStatusCode:
        OK    = "OK"
        ERROR = "ERROR"
        UNSET = "UNSET"

    class _NoopTraceModule(types.ModuleType):
        def get_tracer(self, name, **kw): return _NoopTracer()
        def set_tracer_provider(self, p): pass
        StatusCode = _NoopStatusCode

    class _NoopToken:
        pass

    class _NoopCtxModule(types.ModuleType):
        def attach(self, ctx):  return _NoopToken()
        def detach(self, tok):  pass

    def _set_span_in_context(span):
        return None

    _noop_trace_mod         = _NoopTraceModule("opentelemetry.trace")
    _noop_trace_mod.set_span_in_context = _set_span_in_context
    _noop_ctx_mod           = _NoopCtxModule("opentelemetry.context")

    _otel_trace_mod = _noop_trace_mod
    _otel_ctx_mod   = _noop_ctx_mod

    log.debug("opentelemetry package not installed — tracing disabled (noop mode)")

# ── Public re-exports ─────────────────────────────────────────────────────────
# worker.py and executor.py import these instead of touching opentelemetry directly,
# so they work whether or not the package is installed.
#
#   from app.telemetry import otel_trace, otel_context, StatusCode
#
trace         = _otel_trace_mod   # noqa: F811  (backward compat alias)
otel_trace    = _otel_trace_mod
otel_context  = _otel_ctx_mod
StatusCode    = _otel_trace_mod.StatusCode

# ── Internal state ────────────────────────────────────────────────────────────
_setup_done   = False
_tracer_cache: dict = {}


def setup_tracing() -> None:
    """Configure the OTEL TracerProvider.

    Idempotent — subsequent calls after the first are silently ignored.
    Safe to call from all three process entry points (API, worker, scheduler).
    """
    global _setup_done
    if _setup_done:
        return
    _setup_done = True

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint:
        log.debug("OTEL tracing disabled — set OTEL_EXPORTER_OTLP_ENDPOINT to enable")
        return

    try:
        _configure_sdk(endpoint)
    except ImportError as exc:
        log.warning(
            "OTEL tracing requested (OTEL_EXPORTER_OTLP_ENDPOINT=%s) but SDK packages "
            "are missing: %s.  "
            "Install: opentelemetry-sdk opentelemetry-exporter-otlp-proto-http",
            endpoint, exc,
        )
    except (TypeError, ValueError, AttributeError) as exc:
        log.warning("OTEL tracing setup failed: %s — continuing without tracing", exc)


def _configure_sdk(endpoint: str) -> None:
    """Import and configure the OTEL SDK.  Isolated so ImportError is caught cleanly."""
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from app._version import __version__

    service_name = os.environ.get("OTEL_SERVICE_NAME", "hiverunr")

    resource = Resource.create({
        SERVICE_NAME:       service_name,
        "service.version":  __version__,
        "deployment.environment": os.environ.get("APP_ENV", "production"),
    })

    provider = TracerProvider(resource=resource)

    # ── OTLP/HTTP exporter ────────────────────────────────────────────────────
    headers: dict[str, str] = {}
    for part in os.environ.get("OTEL_EXPORTER_OTLP_HEADERS", "").split(","):
        part = part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            headers[k.strip()] = v.strip()

    otlp = OTLPSpanExporter(endpoint=endpoint, headers=headers)
    provider.add_span_processor(BatchSpanProcessor(otlp))

    # ── Optional stdout/console exporter (debug mode) ─────────────────────────
    if os.environ.get("OTEL_LOG_LEVEL", "").lower() == "debug":
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))
        log.info("OTEL console span exporter enabled (OTEL_LOG_LEVEL=debug)")

    trace.set_tracer_provider(provider)
    log.info(
        "OTEL tracing enabled → %s  service=%s  version=%s",
        endpoint, service_name, __version__,
    )


def get_tracer(name: str = "hiverunr") -> trace.Tracer:
    """Return a Tracer for the given instrument name.

    Returns the real SDK Tracer when tracing is active, or the opentelemetry-api
    NoopTracer otherwise.  Callers never need to check whether tracing is on.

    Example::

        tracer = get_tracer("hiverunr.nodes")
        with tracer.start_as_current_span("action.http_request") as span:
            span.set_attribute("http.url", url)
    """
    if name not in _tracer_cache:
        _tracer_cache[name] = trace.get_tracer(name)
    return _tracer_cache[name]
