"""PDF generation action node.

Renders an HTML template to a PDF using xhtml2pdf (pure Python, no system
binaries required).  The resulting PDF is returned as a base64-encoded string
so it can be passed directly to action.send_email (as an attachment) or
action.s3 (put operation).

Config:
  html         — HTML source (supports {{template}} rendering, full HTML document
                 or bare fragment — a minimal wrapper is added automatically)
  filename     — suggested filename for the output, e.g. report.pdf (default: output.pdf)
  page_size    — A4 | letter | legal  (default: A4)
  orientation  — portrait | landscape (default: portrait)
  encoding     — source encoding (default: utf-8)

Output:
  pdf_bytes    — base64-encoded PDF binary
  size_bytes   — byte count of the raw PDF
  filename     — echoed filename
  ok           — True

"""
import base64
import io
from app.nodes._utils import _render

NODE_TYPE = "action.pdf"
LABEL = "PDF Generate"

_PAGE_SIZES = {
    "a4":      "A4",
    "letter":  "letter",
    "legal":   "legal",
}

_HTML_WRAPPER = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<style>
  body {{ font-family: Helvetica, Arial, sans-serif; font-size: 11pt; color: #111; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
  th {{ background: #f3f4f6; font-weight: bold; }}
  h1 {{ font-size: 20pt; }} h2 {{ font-size: 15pt; }} h3 {{ font-size: 12pt; }}
  pre, code {{ font-family: monospace; font-size: 9pt; background: #f9fafb; padding: 2px 4px; }}
  @page {{
    size: {page_size} {orientation};
    margin: 2cm;
  }}
</style>
</head>
<body>
{content}
</body>
</html>
"""

import logging

logger = logging.getLogger(__name__)

def run(config, inp, context, logger, creds=None, **kwargs):
    """Render HTML to PDF and return base64-encoded bytes."""
    try:
        from xhtml2pdf import pisa
    except ImportError as exc:
        raise ImportError(
            "action.pdf requires xhtml2pdf: pip install xhtml2pdf>=0.2.16"
        ) from exc

    # ── Config ────────────────────────────────────────────────────────────────
    html_src    = _render(config.get("html", ""), context, creds)
    filename    = _render(config.get("filename", "output.pdf"), context, creds).strip() or "output.pdf"
    page_size   = _PAGE_SIZES.get(
        _render(config.get("page_size", "a4"), context, creds).strip().lower(), "A4"
    )
    orientation = _render(config.get("orientation", "portrait"), context, creds).strip().lower()
    if orientation not in ("portrait", "landscape"):
        orientation = "portrait"

    if not html_src.strip():
        raise ValueError("action.pdf: 'html' config field is required and cannot be empty")

    # ── Wrap bare HTML fragments ───────────────────────────────────────────────
    if not html_src.strip().lower().startswith("<!doctype") and "<html" not in html_src.lower():
        html_src = _HTML_WRAPPER.format(
            page_size=page_size,
            orientation=orientation,
            content=html_src,
        )

    # ── Render ────────────────────────────────────────────────────────────────
    logger.info("action.pdf: rendering %s (%s %s)", filename, page_size, orientation)
    buf = io.BytesIO()
    result = pisa.CreatePDF(
        src=html_src.encode("utf-8"),
        dest=buf,
        encoding="utf-8",
    )

    if result.err:
        logger.warning("action.pdf: rendering failed with %s error(s) — input preview: %s", result.err, html_src[:80])
        raise RuntimeError(f"action.pdf: PDF rendering failed with {result.err} error(s)")

    raw = buf.getvalue()
    encoded = base64.b64encode(raw).decode("ascii")

    logger.info("action.pdf: generated %d bytes → %s", len(raw), filename)
    return {
        "pdf_bytes":   encoded,
        "size_bytes":  len(raw),
        "filename":    filename,
        "ok":          True,
    }
