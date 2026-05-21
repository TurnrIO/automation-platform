"""MongoDB node — uses pymongo (optional dep)."""
import ipaddress
import json
import logging
import socket
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

logger = logging.getLogger(__name__)
NODE_TYPE = "action.mongodb"
LABEL     = "MongoDB"

# ── SSRF protection ────────────────────────────────────────────────────────────

_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
]
_IMDS_IP = ipaddress.ip_address("169.254.169.254")


def _is_internal_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if ip == _IMDS_IP:
            return True
        return any(ip in net for net in _BLOCKED_NETWORKS)
    except ValueError:
        return True


def _check_ssrf(host: str, port: int) -> None:
    """Block connections to internal/reserved IPs via DNS resolution."""
    try:
        infos = socket.getaddrinfo(host, port)
        for (_, _, _, _, sockaddr) in infos:
            if _is_internal_ip(sockaddr[0]):
                raise ValueError(f"SSRF blocked: resolved to internal IP {sockaddr[0]}")
    except socket.gaierror:
        raise ValueError(f"MongoDB: could not resolve hostname: {host}")


def _get_client(uri):
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError, ConnectionFailure, ServerSelectionTimeoutError
    # ── SSRF: resolve hostname before connecting ───────────────────────────
    _host = _uri_host(uri)
    if _host:
        _check_ssrf(_host, 27017)
    try:
        return MongoClient(uri, serverSelectionTimeoutMS=10000)
    except PyMongoError as exc:
        logger.warning("MongoDB: connection failed — %s", exc)
        raise
    except ValueError:
        # Re-raise directly — this is a user-facing URI format error, not an
        # infrastructure failure the node should silently swallow.
        raise


def _parse_json(raw, label):
    if not raw or raw.strip() in ("", "{}"):
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except JSONDecodeError:
        raise ValueError(f"MongoDB {label}: must be valid JSON, got: {raw!r}")

def run(config, inp, context, logger, creds=None, **kwargs):
    logger.info("MongoDB: node invoked")
    cred_name = config.get("credential", "")
    uri       = ""
    if cred_name and creds:
        raw = creds.get(cred_name, {})
        if isinstance(raw, str):
            try:   raw = json.loads(raw)
            except JSONDecodeError: raw = {}
        uri = raw.get("uri", raw.get("connection_string", raw.get("url", "")))
    if not uri:
        uri = _render(config.get("uri", ""), context, creds)
    if not uri:
        raise ValueError("MongoDB: connection uri is required (set via credential or uri field)")

    db_name   = _render(config.get("database", ""), context, creds)
    coll_name = _render(config.get("collection", ""), context, creds)
    op        = _render(config.get("operation", "find"), context, creds)

    logger.info("MongoDB: op=%s db=%s coll=%s", op, db_name, coll_name)
    try: limit_val = int(_render(config.get("limit", "100"), context, creds))
    except (ValueError, TypeError): limit_val = 100

    client = _get_client(uri)
    try:
        db   = client[db_name]   if db_name   else client.get_default_database()
        coll = db[coll_name]

        # ── find ─────────────────────────────────────────────────────────────
        if op == "find":
            logger.info("MongoDB: find db=%s coll=%s limit=%s", db_name, coll_name, limit_val)
            filter_raw     = _render(config.get("filter", "{}"), context, creds)
            projection_raw = _render(config.get("projection", ""), context, creds)
            sort_raw       = _render(config.get("sort", ""), context, creds)
            flt  = _parse_json(filter_raw, "filter")
            proj = _parse_json(projection_raw, "projection") if projection_raw.strip() else None
            srt  = _parse_json(sort_raw, "sort") if sort_raw.strip() else None
            cur  = coll.find(flt, proj)
            if srt:
                cur = cur.sort(list(srt.items()))
            cur = cur.limit(min(limit_val, 1000))
            docs = [_bson_to_dict(d) for d in cur]
            return {"documents": docs, "count": len(docs), "document": docs[0] if docs else None}

        # ── find-one ──────────────────────────────────────────────────────────
        elif op == "find_one":
            filter_raw = _render(config.get("filter", "{}"), context, creds)
            flt  = _parse_json(filter_raw, "filter")
            logger.info("MongoDB: find_one db=%s coll=%s filter=%s", db_name, coll_name, filter_raw[:50])
            doc  = coll.find_one(flt)
            flat = _bson_to_dict(doc) if doc else None
            return {"document": flat, "found": flat is not None}

        # ── insert-one ────────────────────────────────────────────────────────
        elif op == "insert_one":
            doc_raw = _render(config.get("document", "{}"), context, creds)
            doc     = _parse_json(doc_raw, "document")
            logger.info("MongoDB: insert_one db=%s coll=%s", db_name, coll_name)
            result  = coll.insert_one(doc)
            return {"inserted_id": str(result.inserted_id), "ok": result.acknowledged}

        # ── update-one ────────────────────────────────────────────────────────
        elif op == "update_one":
            filter_raw = _render(config.get("filter", "{}"), context, creds)
            update_raw = _render(config.get("update", "{}"), context, creds)
            upsert     = config.get("upsert", False)
            flt    = _parse_json(filter_raw, "filter")
            upd    = _parse_json(update_raw, "update")
            logger.info("MongoDB: update_one db=%s coll=%s upsert=%s", db_name, coll_name, upsert)
            result = coll.update_one(flt, upd, upsert=bool(upsert))
            return {
                "matched_count":  result.matched_count,
                "modified_count": result.modified_count,
                "upserted_id":    str(result.upserted_id) if result.upserted_id else None,
                "ok": result.acknowledged,
            }

        # ── delete-one ────────────────────────────────────────────────────────
        elif op == "delete_one":
            filter_raw = _render(config.get("filter", "{}"), context, creds)
            flt    = _parse_json(filter_raw, "filter")
            logger.info("MongoDB: delete_one db=%s coll=%s", db_name, coll_name)
            result = coll.delete_one(flt)
            return {"deleted_count": result.deleted_count, "ok": result.acknowledged}

        # ── aggregate ─────────────────────────────────────────────────────────
        elif op == "aggregate":
            pipeline_raw = _render(config.get("pipeline", "[]"), context, creds)
            logger.info("MongoDB: aggregate db=%s coll=%s", db_name, coll_name)
            try:
                pipeline = json.loads(pipeline_raw) if isinstance(pipeline_raw, str) else pipeline_raw
            except JSONDecodeError:
                raise ValueError("MongoDB aggregate: pipeline must be valid JSON array")
            docs = [_bson_to_dict(d) for d in coll.aggregate(pipeline)]
            return {"documents": docs, "count": len(docs), "document": docs[0] if docs else None}

        # ── count ─────────────────────────────────────────────────────────────
        elif op == "count":
            filter_raw = _render(config.get("filter", "{}"), context, creds)
            flt = _parse_json(filter_raw, "filter")
            logger.info("MongoDB: count db=%s coll=%s", db_name, coll_name)
            n   = coll.count_documents(flt)
            return {"count": n}

        else:
            raise ValueError(f"MongoDB: unknown operation {op!r}")
    finally:
        if 'client' in dir():
            client.close()

def _uri_host(uri: str) -> str:
    """Extract hostname from a MongoDB URI string."""
    # Strip scheme
    after = uri.split("://", 1)[-1] if "://" in uri else uri
    # Host portion is before the first / or :
    host_part = after.split("/", 1)[0].split(":", 1)[0]
    return host_part


def _bson_to_dict(doc):
    """Convert ObjectId + other BSON types to plain strings for JSON serialisation."""
    if doc is None:
        return None
    out = {}
    for k, v in doc.items():
        if k == "_id":
            out["_id"] = str(v)
        elif hasattr(v, "isoformat"):   # datetime
            out[k] = v.isoformat()
        elif type(v).__name__ in ("ObjectId", "Decimal128", "Int64"):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = _bson_to_dict(v)
        elif isinstance(v, list):
            out[k] = [_bson_to_dict(i) if isinstance(i, dict) else i for i in v]
        else:
            out[k] = v
    return out
