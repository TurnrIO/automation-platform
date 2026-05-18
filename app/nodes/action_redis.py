"""Redis action node.

Supports GET/SET/DEL/EXISTS/INCR/DECR/EXPIRE/TTL/LPUSH/RPOP/LLEN/LRANGE/SADD/SMEMBERS/HSET/HGET/HGETALL operations.

Credential JSON fields:
  url  — full Redis URL, e.g. redis://[:password@]host:6379/0
         OR set host/port/password/db individually.
  host, port, password, db — individual fields (url takes precedence).
"""
import ipaddress
import logging

logger = logging.getLogger(__name__)
import json
import socket
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

NODE_TYPE = "action.redis"
LABEL = "Redis"

# ── SSRF protection ────────────────────────────────────────────────────────────
_BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
]
_IMDS_IP = ipaddress.ip_address("169.254.169.254")


def _check_ssrf(host: str) -> None:
    """Validate resolved IP addresses of host against blocked ranges."""
    if not host:
        return
    try:
        infos = socket.getaddrinfo(host, 0, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        for (family, socktype, proto, _, sockaddr) in infos:
            ip = ipaddress.ip_address(sockaddr[0])
            for net in _BLOCKED_NETWORKS:
                if ip in net:
                    raise ValueError(
                        f"Redis: host '{host}' resolved to blocked IP {ip} — SSRF risk"
                    )
            if ip == _IMDS_IP:
                raise ValueError(
                    f"Redis: host '{host}' resolved to IMDS IP {ip} — SSRF risk"
                )
    except socket.gaierror:
        # DNS resolution failure — let Redis client fail naturally
        pass


def _get_client(config, context, creds):
    """Build a redis client from credential + config fields."""
    import redis as _redis

    url = _render(config.get("url", ""), context, creds).strip()

    # Try to pull from a named credential
    cred_name = _render(config.get("credential", ""), context, creds).strip()
    if cred_name and creds and not url:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                c = json.loads(raw)
                url = c.get("url", "") or c.get("redis_url", "")
                if not url:
                    host     = c.get("host", "localhost")
                    port     = int(c.get("port", 6379))
                    password = c.get("password") or None
                    db       = int(c.get("db", 0))
                    _check_ssrf(host)
                    return _redis.Redis(host=host, port=port, password=password, db=db,
                                       socket_timeout=10, decode_responses=True)
            except (JSONDecodeError, AttributeError, ValueError):
                url = raw.strip()
        return _redis.from_url(url, socket_timeout=10, decode_responses=True)

    if url:
        # SSRF check for URLs from credentials or direct config
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or ""
        if host:
            _check_ssrf(host)
        return _redis.from_url(url, socket_timeout=10, decode_responses=True)

    # Fallback: use inline config fields
    host     = _render(config.get("host", "localhost"), context, creds) or "localhost"
    port_raw = _render(config.get("port", "6379"), context, creds) or "6379"
    password = _render(config.get("password", ""), context, creds) or None
    db_raw   = _render(config.get("db", "0"), context, creds) or "0"

    _check_ssrf(host)

    try:
        port = int(port_raw)
    except (ValueError, TypeError):
        port = 6379
    try:
        db = int(db_raw)
    except (ValueError, TypeError):
        db = 0
    return _redis.Redis(host=host, port=port, password=password, db=db,
                        socket_timeout=10, decode_responses=True)


def run(config, inp, context, logger, creds=None, **kwargs):
    """Execute a Redis command."""
    import redis

    operation = _render(config.get("operation", "get"), context, creds).strip().lower()
    key       = _render(config.get("key", ""), context, creds).strip()
    value     = _render(config.get("value", ""), context, creds)
    ttl_raw   = _render(config.get("ttl", ""), context, creds).strip()
    count_raw = _render(config.get("count", "1"), context, creds).strip()
    field     = _render(config.get("field", ""), context, creds).strip()  # for HSET/HGET

    if not key and operation not in ("ping",):
        raise ValueError("Redis: 'key' is required")

    logger.info("Redis: op=%s key=%s", operation, key)
    try:
        r = _get_client(config, context, creds)
    except ValueError as e:
        return {"__error": f"Redis SSRF check failed: {e}"}

    try:
        if operation == "get":
            result = r.get(key)
            logger.info("Redis GET: key=%s exists=%s value=%s", key, result is not None, result)
            return {"value": result, "key": key, "exists": result is not None}

        elif operation == "set":
            ttl = None
            if ttl_raw:
                try:   ttl = int(ttl_raw)
                except (ValueError, TypeError): ttl = None
            if ttl:
                r.setex(key, ttl, value)
            else:
                r.set(key, value)
            logger.info("Redis SET: key=%s ttl=%s value=%s", key, ttl, value)
            return {"ok": True, "key": key, "value": value}

        elif operation == "del":
            deleted = r.delete(key)
            logger.info("Redis DEL: key=%s deleted=%s", key, deleted)
            return {"deleted": deleted, "key": key, "ok": deleted > 0}

        elif operation == "exists":
            exists = bool(r.exists(key))
            logger.info("Redis EXISTS: key=%s exists=%s", key, exists)
            return {"exists": exists, "key": key}

        elif operation == "incr":
            try:   amount = int(count_raw) if count_raw else 1
            except (ValueError, TypeError): amount = 1
            new_val = r.incrby(key, amount)
            logger.info("Redis INCR: key=%s amount=%s new_val=%s", key, amount, new_val)
            return {"value": new_val, "key": key}

        elif operation == "decr":
            try:   amount = int(count_raw) if count_raw else 1
            except (ValueError, TypeError): amount = 1
            new_val = r.decrby(key, amount)
            logger.info("Redis DECR: key=%s amount=%s new_val=%s", key, amount, new_val)
            return {"value": new_val, "key": key}

        elif operation == "expire":
            try:   ttl = int(ttl_raw) if ttl_raw else 60
            except (ValueError, TypeError): ttl = 60
            ok = bool(r.expire(key, ttl))
            logger.info("Redis EXPIRE: key=%s ttl=%s ok=%s", key, ttl, ok)
            return {"ok": ok, "key": key, "ttl": ttl}

        elif operation == "ttl":
            remaining = r.ttl(key)
            logger.info("Redis TTL: key=%s remaining=%s", key, remaining)
            return {"ttl": remaining, "key": key, "persistent": remaining == -1, "missing": remaining == -2}

        elif operation == "lpush":
            length = r.lpush(key, value)
            logger.info("Redis LPUSH: key=%s length=%s value=%s", key, length, value)
            return {"length": length, "key": key, "value": value}

        elif operation == "rpush":
            length = r.rpush(key, value)
            logger.info("Redis RPUSH: key=%s length=%s value=%s", key, length, value)
            return {"length": length, "key": key, "value": value}

        elif operation == "lpop":
            popped = r.lpop(key)
            logger.info("Redis Lpop: key=%s popped=%s", key, popped)
            return {"value": popped, "key": key, "empty": popped is None}

        elif operation == "rpop":
            popped = r.rpop(key)
            logger.info("Redis RPOP: key=%s popped=%s", key, popped)
            return {"value": popped, "key": key, "empty": popped is None}

        elif operation == "llen":
            length = r.llen(key)
            logger.info("Redis LLEN: key=%s length=%s", key, length)
            return {"length": length, "key": key}

        elif operation == "lrange":
            start_raw = _render(config.get("start", "0"), context, creds) or "0"
            stop_raw  = _render(config.get("stop", "-1"), context, creds) or "-1"
            try:   start = int(start_raw)
            except (ValueError, TypeError): start = 0
            try:   stop  = int(stop_raw)
            except (ValueError, TypeError): stop = -1
            items = r.lrange(key, start, stop)
            logger.info("Redis LRANGE: key=%s start=%s stop=%s count=%s", key, start, stop, len(items))
            return {"items": items, "count": len(items), "key": key}

        elif operation == "sadd":
            added = r.sadd(key, value)
            logger.info("Redis SADD: key=%s added=%s value=%s", key, added, value)
            return {"added": added, "key": key}

        elif operation == "smembers":
            members = list(r.smembers(key))
            logger.info("Redis SMEMBERS: key=%s count=%s", key, len(members))
            return {"members": members, "count": len(members), "key": key}

        elif operation == "hset":
            if not field:
                raise ValueError("Redis hset: 'field' is required")
            r.hset(key, field, value)
            logger.info("Redis HSET: key=%s field=%s value=%s", key, field, value)
            return {"ok": True, "key": key, "field": field, "value": value}

        elif operation == "hget":
            if not field:
                raise ValueError("Redis hget: 'field' is required")
            result = r.hget(key, field)
            logger.info("Redis HGET: key=%s field=%s value=%s", key, field, result)
            return {"value": result, "key": key, "field": field, "exists": result is not None}

        elif operation == "hgetall":
            data = r.hgetall(key)
            logger.info("Redis HGETALL: key=%s count=%s", key, len(data))
            return {"data": data, "count": len(data), "key": key}

        elif operation == "ping":
            r.ping()
            logger.info("Redis PING: ok=True")
            return {"ok": True, "pong": True}

        else:
            raise ValueError(f"Redis: unknown operation '{operation}'")

    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError,
            redis.exceptions.RedisError, OSError, ValueError, TypeError) as exc:
        logger.warning("Redis operation '%s' failed: %s", operation, exc)
        return {"error": str(exc), "ok": False, "operation": operation}

    finally:
        if r is not None:
            try:
                r.close()
            except (AttributeError, RuntimeError):
                pass
