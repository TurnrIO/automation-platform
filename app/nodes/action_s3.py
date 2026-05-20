"""S3-compatible object storage action node.

Supports AWS S3 and any S3-compatible service (MinIO, Backblaze B2,
Cloudflare R2, DigitalOcean Spaces, etc.) via the boto3 library.

Credential JSON fields (store as a credential in the vault):
  access_key   — AWS access key ID / service key
  secret_key   — AWS secret access key / service secret
  region       — AWS region (default: us-east-1)
  endpoint_url — custom endpoint for S3-compatible services
                 e.g. "https://play.min.io" or "https://<id>.r2.cloudflarestorage.com"

Operations
----------
  get          — download object; returns body (string/base64) + metadata
  put          — upload content to key; returns etag + version_id
  list         — list objects under prefix; returns objects[]
  delete       — delete object
  presigned_url — generate a pre-signed GET URL (expires_in seconds)
  head         — fetch metadata without downloading the body
  copy         — copy object to a new key within the same bucket

Output shape
------------
  get:    { body, content_type, size, last_modified, metadata, key, bucket }
  put:    { ok, key, bucket, etag, version_id }
  list:   { objects: [{key,size,last_modified,etag}], count, bucket, prefix, truncated }
  delete: { ok, key, bucket }
  presigned_url: { url, expires_in, key, bucket }
  head:   { exists, content_type, size, last_modified, metadata, key, bucket }
  copy:   { ok, source_key, dest_key, bucket, etag }
"""
import base64
import json
from json import JSONDecodeError
import logging
logger = logging.getLogger(__name__)
# Optional: boto3 needed for action.s3
try:
    import botocore.exceptions
    ClientError = botocore.exceptions.ClientError
except ImportError:
    ClientError = Exception
from ._utils import _render, _resolve_cred_raw

NODE_TYPE = "action.s3"
LABEL     = "S3 Storage"


# ── boto3 client factory ──────────────────────────────────────────────────────

def _make_client(cred: dict, timeout: int = 30):
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ImportError:
        raise RuntimeError(
            "action.s3 requires boto3. Install it with: pip install boto3"
        )

    access_key   = cred.get("access_key") or cred.get("aws_access_key_id", "")
    secret_key   = cred.get("secret_key") or cred.get("aws_secret_access_key", "")
    region       = cred.get("region", "us-east-1")
    endpoint_url = cred.get("endpoint_url", "").strip() or None

    kwargs = {
        "aws_access_key_id":     access_key,
        "aws_secret_access_key": secret_key,
        "region_name":           region,
        "config": BotoConfig(
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=timeout,
            read_timeout=timeout,
        ),
    }
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url

    return boto3.client("s3", **kwargs)


# ── Operation handlers ────────────────────────────────────────────────────────

def _op_get(s3, bucket: str, key: str, config: dict, context: dict, creds: dict) -> dict:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
    except (OSError, ClientError) as exc:
        code = getattr(getattr(exc, "response", {}), "Error", {},).get("Code", "")
        if str(code) in ("404", "NoSuchKey", "NoSuchBucket"):
            return {"exists": False, "key": key, "bucket": bucket}
        raise
    raw  = resp["Body"].read()

    content_type = resp.get("ContentType", "")
    # Try to decode as UTF-8; fall back to base64 for binary objects
    try:
        body = raw.decode("utf-8")
        encoding = "utf-8"
    except UnicodeDecodeError:
        body = base64.b64encode(raw).decode("ascii")
        encoding = "base64"

    return {
        "body":          body,
        "encoding":      encoding,
        "content_type":  content_type,
        "size":          resp.get("ContentLength", len(raw)),
        "last_modified": str(resp.get("LastModified", "")),
        "metadata":      resp.get("Metadata", {}),
        "version_id":    resp.get("VersionId", ""),
        "key":           key,
        "bucket":        bucket,
    }


def _op_put(s3, bucket: str, key: str, config: dict, context: dict, creds: dict) -> dict:
    content      = _render(config.get("content", ""),      context, creds)
    content_type = _render(config.get("content_type", ""), context, creds).strip() \
                   or "application/octet-stream"
    metadata_raw = _render(config.get("metadata_json", "{}"), context, creds).strip()
    try:
        metadata = json.loads(metadata_raw) if metadata_raw else {}
    except JSONDecodeError:
        metadata = {}

    body = content.encode("utf-8") if isinstance(content, str) else content

    extra = {"ContentType": content_type}
    if metadata:
        extra["Metadata"] = {str(k): str(v) for k, v in metadata.items()}

    try:
        resp = s3.put_object(Bucket=bucket, Key=key, Body=body, **extra)
    except (OSError, ClientError) as exc:
        logger.warning("[action.s3] put failed: bucket=%s key=%s error=%s", bucket, key, exc)
        return {"ok": False, "key": key, "bucket": bucket, "error": str(exc)}
    return {
        "ok":         True,
        "key":        key,
        "bucket":     bucket,
        "etag":       resp.get("ETag", "").strip('"'),
        "version_id": resp.get("VersionId", ""),
    }


def _op_list(s3, bucket: str, key: str, config: dict, context: dict, creds: dict) -> dict:
    prefix     = _render(config.get("prefix", key or ""), context, creds)
    try: max_keys = int(_render(str(config.get("max_keys", "1000")), context, creds))
    except (ValueError, TypeError): max_keys = 1000
    delimiter  = _render(config.get("delimiter", ""),  context, creds)

    kwargs: dict = {"Bucket": bucket, "MaxKeys": max_keys}
    if prefix:
        kwargs["Prefix"] = prefix
    if delimiter:
        kwargs["Delimiter"] = delimiter

    try:
        resp    = s3.list_objects_v2(**kwargs)
    except (OSError, ClientError) as exc:
        logger.warning("[action.s3] list failed: bucket=%s prefix=%s error=%s", bucket, prefix, exc)
        return {"objects": [], "count": 0, "bucket": bucket, "prefix": prefix, "truncated": False, "error": str(exc)}
    objects = [
        {
            "key":           o["Key"],
            "size":          o.get("Size", 0),
            "last_modified": str(o.get("LastModified", "")),
            "etag":          o.get("ETag", "").strip('"'),
            "storage_class": o.get("StorageClass", ""),
        }
        for o in resp.get("Contents", [])
    ]
    return {
        "objects":   objects,
        "count":     len(objects),
        "bucket":    bucket,
        "prefix":    prefix,
        "truncated": resp.get("IsTruncated", False),
    }


def _op_delete(s3, bucket: str, key: str, config: dict, context: dict, creds: dict) -> dict:
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except (OSError, ClientError) as exc:
        logger.warning("[action.s3] delete failed: bucket=%s key=%s error=%s", bucket, key, exc)
        return {"ok": False, "key": key, "bucket": bucket, "error": str(exc)}
    return {"ok": True, "key": key, "bucket": bucket}


def _op_presigned_url(s3, bucket: str, key: str, config: dict, context: dict, creds: dict) -> dict:
    try: expires_in = int(_render(str(config.get("expires_in", "3600")), context, creds))
    except (ValueError, TypeError): expires_in = 3600
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )
    return {"url": url, "expires_in": expires_in, "key": key, "bucket": bucket}


def _op_head(s3, bucket: str, key: str, config: dict, context: dict, creds: dict) -> dict:
    try:
        resp = s3.head_object(Bucket=bucket, Key=key)
        return {
            "exists":        True,
            "content_type":  resp.get("ContentType", ""),
            "size":          resp.get("ContentLength", 0),
            "last_modified": str(resp.get("LastModified", "")),
            "metadata":      resp.get("Metadata", {}),
            "version_id":    resp.get("VersionId", ""),
            "key":           key,
            "bucket":        bucket,
        }
    except (AttributeError, KeyError, ValueError) as exc:
        code = getattr(getattr(exc, "response", None), "status_code", None) \
               or getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if str(code) in ("404", "NoSuchKey"):
            return {"exists": False, "key": key, "bucket": bucket}
        raise


def _op_copy(s3, bucket: str, key: str, config: dict, context: dict, creds: dict) -> dict:
    source_key  = _render(config.get("source_key", key),  context, creds).strip()
    dest_key    = _render(config.get("dest_key",   ""),   context, creds).strip()
    dest_bucket = _render(config.get("dest_bucket", bucket), context, creds).strip() or bucket

    if not source_key:
        raise ValueError("action.s3 copy: 'source_key' is required")
    if not dest_key:
        raise ValueError("action.s3 copy: 'dest_key' is required")

    copy_source = {"Bucket": bucket, "Key": source_key}
    try:
        resp = s3.copy_object(Bucket=dest_bucket, CopySource=copy_source, Key=dest_key)
    except (OSError, ClientError) as exc:
        logger.warning("[action.s3] copy failed: bucket=%s source_key=%s dest_key=%s error=%s", bucket, source_key, dest_key, exc)
        return {"ok": False, "source_key": source_key, "dest_key": dest_key, "bucket": dest_bucket, "error": str(exc)}
    etag = (resp.get("CopyObjectResult") or {}).get("ETag", "").strip('"')
    return {
        "ok":         True,
        "source_key": source_key,
        "dest_key":   dest_key,
        "bucket":     dest_bucket,
        "etag":       etag,
    }


_OPERATIONS = {
    "get":           _op_get,
    "put":           _op_put,
    "list":          _op_list,
    "delete":        _op_delete,
    "presigned_url": _op_presigned_url,
    "head":          _op_head,
    "copy":          _op_copy,
}


# ── Node entry point ──────────────────────────────────────────────────────────

def run(config: dict, inp: dict, context: dict, logger, creds=None, **kwargs) -> dict:
    creds = creds or {}

    logger.info("[action.s3] op=%s", config.get("operation", "get"))

    # Resolve credential
    cred_name = _render(config.get("credential", ""), context, creds)
    raw_cred  = _resolve_cred_raw(cred_name, creds)
    try:
        cred = json.loads(raw_cred) if raw_cred else {}
    except (JSONDecodeError, TypeError):
        cred = {}

    if not cred:
        raise ValueError(
            "action.s3: no credential configured. "
            "Set 'credential' to a credential name whose value is a JSON object "
            "with access_key, secret_key, and optionally region / endpoint_url."
        )

    operation = _render(config.get("operation", "get"), context, creds).strip().lower()
    bucket    = _render(config.get("bucket",    ""),    context, creds).strip()
    key       = _render(config.get("key",       ""),    context, creds).strip()

    if not bucket:
        raise ValueError("action.s3: 'bucket' is required")
    if operation not in ("list",) and not key:
        raise ValueError(f"action.s3: 'key' is required for operation '{operation}'")
    if operation not in _OPERATIONS:
        raise ValueError(
            f"action.s3: unknown operation '{operation}'. "
            f"Valid: {', '.join(_OPERATIONS)}"
        )

    logger.info("[action.s3] op=%s bucket=%s key=%s", operation, bucket, key or '(prefix)')

    # Per-operation timeout (default 30s)
    try:
        timeout = int(_render(str(config.get("timeout", "30")), context, creds))
    except (ValueError, TypeError):
        timeout = 30

    s3 = _make_client(cred, timeout=timeout)
    result = _OPERATIONS[operation](s3, bucket, key, config, context, creds)
    logger.info("[action.s3] op=%s bucket=%s key=%s completed result_keys=%s", operation, bucket, key or '(prefix)', list(result.keys()))
    return result
