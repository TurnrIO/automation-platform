"""RSS / Atom feed trigger node.

Polls an RSS 2.0 or Atom 1.0 feed URL and returns entries published (or
updated) within the lookback window.  Uses only Python's stdlib
``xml.etree.ElementTree`` and ``urllib.request`` — no extra dependencies.

Designed to be combined with a ``trigger.cron`` schedule so the flow runs
periodically and picks up new items since the last run.

Configuration
-------------
  url               — RSS or Atom feed URL (required)

  lookback_minutes  — return entries published/updated within the last N
                      minutes (default: 60; set 0 to return all entries)
  filter_expression — optional Python expression evaluated per entry; the
                      variable ``entry`` is the entry dict.  Return True to
                      include, False to skip.  Example:
                        'python' in entry.get('title','').lower()
  max_entries       — cap the number of returned entries (default: 50)

Output shape
------------
{
  "entries": [
    {
      "title":     str,
      "link":      str,
      "published": ISO-8601 str,
      "summary":   str,
      "author":    str,
      "id":        str,
    },
    …
  ],
  "count":    N,
  "feed_title": str,
  # First-entry shortcuts (empty when count == 0):
  "title":    entries[0].title,
  "link":     entries[0].link,
  "summary":  entries[0].summary,
  "published":entries[0].published,
}
"""
import logging

logger = logging.getLogger(__name__)
import ipaddress
import re
import socket
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

NODE_TYPE = "trigger.rss"
LABEL = "RSS / Atom Feed"

# XML namespaces used in feeds
_NS = {
    "atom":    "http://www.w3.org/2005/Atom",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "dc":      "http://purl.org/dc/elements/1.1/",
    "media":   "http://search.yahoo.com/mrss/",
}


# ── URL validation — SSRF protection ─────────────────────────────────────────


# Blocksheet CIDRs that are never safe to fetch from
_BLOCKED_NETWORKS: list[ipaddress.IPv4Network] = [
    ipaddress.IPv4Network("0.0.0.0/8"),       # current node
    ipaddress.IPv4Network("10.0.0.0/8"),      # private
    ipaddress.IPv4Network("127.0.0.0/8"),     # loopback
    ipaddress.IPv4Network("169.254.0.0/16"), # AWS/GCP metadata (IMDS)
    ipaddress.IPv4Network("172.16.0.0/12"),  # private
    ipaddress.IPv4Network("192.168.0.0/16"),  # private
    ipaddress.IPv4Network("224.0.0.0/4"),     # multicast
    ipaddress.IPv4Network("240.0.0.0/4"),     # reserved
    ipaddress.IPv4Network("169.254.169.254/32"),  # AWS metadata endpoint
]


def _validate_feed_url(url: str) -> None:
    """"Raise ValueError if the URL is not a safe HTTPS feed URL.


    Blocks:
    - Non-HTTPS schemes (http, file, ftp, gopher, etc.)
    - IP addresses in blocked ranges (including cloud metadata IPs)
    - Unsafe hostnames (empty host, literal IP)

    Allows public domain names that resolve to safe IPs.
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme.lower() != "https":
        raise ValueError(f"trigger.rss: only HTTPS feeds are supported; got scheme '{parsed.scheme}'")
    if not parsed.netloc:
        raise ValueError("trigger.rss: URL has no host")
    # Resolve the hostname and check each resolved IP
    try:
        host = parsed.netloc.split(":")[0]  # strip port
        infos = urllib.parse.getaddrinfo(host, 443 if parsed.scheme == "https" else 80,
                                        proto=socket.IPPROTO_TCP)
    except (socket.gaierror, OSError) as exc:
        raise ValueError(f"trigger.rss: could not resolve host '{host}': {exc}") from exc
    for family, _, _, _, sockaddr in infos:
        if family != socket.AF_INET:
            continue
        ip = ipaddress.IPv4Address(sockaddr[0])
        for network in _BLOCKED_NETWORKS:
            if ip in network:
                raise ValueError(f"trigger.rss: host '{host}' resolved to blocked IP {ip}")


# ── Date parsing ──────────────────────────────────────────────────────────────

def _parse_date(s: str | None) -> datetime | None:
    """Parse RFC 822 (RSS) or ISO-8601 (Atom) date strings into UTC datetimes."""
    if not s:
        return None
    s = s.strip()
    # Try RFC 822 (RSS pubDate)
    try:
        dt = parsedate_to_datetime(s)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        pass
    # Try ISO-8601 variants (Atom <updated>/<published>)
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(s[:25], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    return None


def _to_iso(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else ""


# ── Text helpers ──────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    """Remove HTML tags for a clean plain-text summary."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


def _text(el, *paths, ns=_NS) -> str:
    """Return stripped text from the first matching child element."""
    for path in paths:
        child = el.find(path, ns)
        if child is not None and child.text:
            return child.text.strip()
    return ""


# ── Feed parsers ─────────────────────────────────────────────────────────────

def _parse_rss(root: ET.Element) -> tuple[str, list[dict]]:
    channel = root.find("channel")
    if channel is None:
        return "", []
    feed_title = _text(channel, "title")
    entries = []
    for item in channel.findall("item"):
        title   = _text(item, "title")
        link    = _text(item, "link") or (item.find("link") is not None and item.find("link").get("href", "")) or ""
        pub     = _text(item, "pubDate", "dc:date", ns=_NS)
        summary = _strip_html(_text(item, "description", "content:encoded", ns=_NS))
        author  = _text(item, "author", "dc:creator", ns=_NS)
        guid    = _text(item, "guid")
        entries.append({
            "title":     title,
            "link":      link,
            "published": pub,
            "summary":   summary[:500],
            "author":    author,
            "id":        guid or link,
            "_dt":       _parse_date(pub),
        })
    return feed_title, entries


def _parse_atom(root: ET.Element) -> tuple[str, list[dict]]:
    feed_title = _text(root, "atom:title", ns=_NS)
    entries = []
    for entry in root.findall("atom:entry", _NS):
        title   = _text(entry, "atom:title", ns=_NS)
        link_el = entry.find("atom:link[@rel='alternate']", _NS) or entry.find("atom:link", _NS)
        link    = link_el.get("href", "") if link_el is not None else ""
        pub     = _text(entry, "atom:published", "atom:updated", ns=_NS)
        summary = _strip_html(
            _text(entry, "atom:summary", "atom:content", "content:encoded", ns=_NS)
        )
        author_el = entry.find("atom:author/atom:name", _NS)
        author    = author_el.text.strip() if author_el is not None and author_el.text else ""
        id_       = _text(entry, "atom:id", ns=_NS)
        entries.append({
            "title":     title,
            "link":      link,
            "published": pub,
            "summary":   summary[:500],
            "author":    author,
            "id":        id_ or link,
            "_dt":       _parse_date(pub),
        })
    return feed_title, entries


# ── Main ──────────────────────────────────────────────────────────────────────

def run(config, inp, context, logger, creds=None, **kwargs):
    from app.nodes._utils import _render, _safe_eval

    logger.info("trigger.rss: starting")

    url = _render(config.get("url", ""), context, creds).strip()
    if not url:
        raise ValueError("trigger.rss: 'url' is required")

    try:
        _validate_feed_url(url)  # SSRF guard — raises ValueError on blocked URLs
    except ValueError as exc:
        logger.warning("trigger.rss: SSRF check failed for %s — %s", url, exc)
        return {"__error": f"trigger.rss: {exc}", "entries": [], "count": 0}

    try: lookback_minutes = int(_render(config.get("lookback_minutes", "60"), context, creds))
    except (ValueError, TypeError): lookback_minutes = 60
    try: max_entries = int(_render(config.get("max_entries", "50"), context, creds))
    except (ValueError, TypeError): max_entries = 50
    filter_expr      = _render(config.get("filter_expression", ""),       context, creds).strip()

    # ── Fetch feed ────────────────────────────────────────────────────────────
    logger.info("trigger.rss: fetching %s", url)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "HiveRunr/1.0 RSS Trigger"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        logger.warning("trigger.rss: HTTP error fetching %s — %s", url, exc)
        return {"__error": f"trigger.rss: HTTP error {exc.code} fetching feed: {exc}", "entries": [], "count": 0}
    except urllib.error.URLError as exc:
        logger.warning("trigger.rss: URL error fetching %s — %s", url, exc)
        return {"__error": f"trigger.rss: URL error fetching feed: {exc}", "entries": [], "count": 0}
    except OSError as exc:
        logger.warning("trigger.rss: connection error fetching %s — %s", url, exc)
        return {"__error": f"trigger.rss: connection error: {exc}", "entries": [], "count": 0}

    root = ET.fromstring(raw)
    tag  = root.tag.lower()

    if "rss" in tag or root.tag == "rss":
        feed_title, entries = _parse_rss(root)
    elif "feed" in tag or "atom" in tag.lower():
        feed_title, entries = _parse_atom(root)
    else:
        # Try RSS channel structure as fallback
        feed_title, entries = _parse_rss(root)
        if not entries:
            feed_title, entries = _parse_atom(root)

    logger.info("trigger.rss: fetched %d entries from '%s'", len(entries), feed_title)

    # ── Apply lookback window ─────────────────────────────────────────────────
    if lookback_minutes > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
        filtered = []
        for e in entries:
            dt = e.get("_dt")
            if dt is None or dt >= cutoff:
                filtered.append(e)
        entries = filtered
        logger.info("trigger.rss: %d entries within lookback window", len(entries))

    # ── Apply filter expression ───────────────────────────────────────────────
    if filter_expr:
        kept = []
        for e in entries:
            try:
                if _safe_eval(filter_expr, {"entry": e, "re": re}):
                    kept.append(e)
            except (SyntaxError, ValueError, NameError, TypeError) as exc:
                logger.warning("trigger.rss: filter_expression error for entry '%s': %s", e.get('title', '')[:50], exc)
        entries = kept
        logger.info("trigger.rss: %d entries after filter", len(entries))

    # ── Cap and clean ─────────────────────────────────────────────────────────
    entries = entries[:max_entries]
    for e in entries:
        e["published"] = _to_iso(e.pop("_dt", None)) or e.get("published", "")

    first = entries[0] if entries else {}
    result = {
        "entries":    entries,
        "count":      len(entries),
        "feed_title": feed_title,
        # First-entry shortcuts
        "title":     first.get("title", ""),
        "link":      first.get("link", ""),
        "summary":   first.get("summary", ""),
        "published": first.get("published", ""),
        "author":    first.get("author", ""),
    }
    logger.info(
        "trigger.rss: completed url=%s total=%d lookback=%dm filter=%s returned=%d",
        url,
        len(entries) + (0 if lookback_minutes == 0 else 0),  # we don't track raw total without re-parsing
        lookback_minutes,
        repr(filter_expr) if filter_expr else "none",
        len(entries),
    )
    return result
