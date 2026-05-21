"""File-watch trigger — returns files that appeared or changed in a directory.

Supports both local filesystem paths and remote SFTP directories.  Designed
to be run on a cron schedule; ``lookback_minutes`` controls the sliding window
of "recently modified" files so each scheduled run picks up new arrivals.

Configuration
-------------
  path              — directory to watch (local or SFTP remote path)
  pattern           — glob/wildcard to match filenames, e.g. "*.csv" (default: "*")
  recursive         — descend into sub-directories (default: false)
  lookback_minutes  — include files modified in the last N minutes (default: 60)
  min_age_seconds   — skip files modified less than N seconds ago (write-guard, default: 0)
  min_size_bytes    — skip files smaller than N bytes (default: 0)
  sftp_credential   — if set, poll an SFTP server instead of the local filesystem
                      (credential JSON: host, port, username, password)

Output shape
------------
{
  "files": [
    { "path", "name", "size", "modified", "modified_ts" },
    …
  ],
  "count":  N,
  # First-file shortcuts (empty strings when count == 0):
  "path":   first file path,
  "name":   first file name,
}
"""
import datetime
import fnmatch
import json
from json import JSONDecodeError
import logging
import os
import time
import paramiko
from paramiko import SSHException, AuthenticationException
from ._utils import _render, _resolve_cred_raw

logger = logging.getLogger(__name__)

NODE_TYPE = "trigger.file_watch"
LABEL     = "File Watch"


# ── helpers ───────────────────────────────────────────────────────────────────

def _matches(name: str, pattern: str) -> bool:
    """Case-insensitive glob match."""
    if not pattern or pattern == "*":
        return True
    return fnmatch.fnmatchcase(name.lower(), pattern.lower())


def _ts_to_iso(ts: float) -> str:
    return datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── local filesystem scan ─────────────────────────────────────────────────────

def _scan_local(path: str, pattern: str, recursive: bool,
                newer_than: float, older_than: float,
                min_size: int, logger) -> list:
    results = []

    def _walk(dirpath: str):
        try:
            entries = list(os.scandir(dirpath))
        except PermissionError as exc:
            logger.info("[trigger.file_watch] Permission denied: %s", exc)
            return
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                if recursive:
                    _walk(entry.path)
                continue
            if not _matches(entry.name, pattern):
                continue
            try:
                st = entry.stat()
            except OSError:
                continue
            mtime = st.st_mtime
            size  = st.st_size
            if mtime < newer_than:
                continue
            if older_than and mtime > older_than:
                continue
            if size < min_size:
                continue
            results.append({
                "path":        entry.path,
                "name":        entry.name,
                "size":        size,
                "modified":    _ts_to_iso(mtime),
                "modified_ts": mtime,
            })

    _walk(path)
    results.sort(key=lambda f: f["modified_ts"])
    return results


# ── SFTP scan ─────────────────────────────────────────────────────────────────

def _scan_sftp(sftp, path: str, pattern: str, recursive: bool,
               newer_than: float, older_than: float,
               min_size: int) -> list:
    import stat as _stat
    results = []

    def _walk(dirpath: str):
        try:
            entries = sftp.listdir_attr(dirpath)
        except OSError:
            return
        for entry in entries:
            full_path = dirpath.rstrip("/") + "/" + entry.filename
            if _stat.S_ISDIR(entry.st_mode or 0):
                if recursive:
                    _walk(full_path)
                continue
            if not _matches(entry.filename, pattern):
                continue
            mtime = float(entry.st_mtime or 0)
            size  = entry.st_size or 0
            if mtime < newer_than:
                continue
            if older_than and mtime > older_than:
                continue
            if size < min_size:
                continue
            results.append({
                "path":        full_path,
                "name":        entry.filename,
                "size":        size,
                "modified":    _ts_to_iso(mtime),
                "modified_ts": mtime,
            })

    _walk(path)
    results.sort(key=lambda f: f["modified_ts"])
    return results


# ── Node entry point ──────────────────────────────────────────────────────────

def run(config: dict, inp: dict, context: dict, logger, creds=None, **kwargs) -> dict:
    creds = creds or {}

    path      = _render(config.get("path",    ""),  context, creds).strip()
    pattern   = _render(config.get("pattern", "*"), context, creds).strip() or "*"
    recursive = str(config.get("recursive", "false")).lower() in ("true", "1", "yes")

    try: lookback_min = float(_render(str(config.get("lookback_minutes", "60")), context, creds))
    except (ValueError, TypeError): lookback_min = 60.0
    try: min_age_sec = float(_render(str(config.get("min_age_seconds", "0")), context, creds))
    except (ValueError, TypeError): min_age_sec = 0.0
    try: min_size = int(_render(str(config.get("min_size_bytes", "0")), context, creds))
    except (ValueError, TypeError): min_size = 0
    sftp_cred_name = _render(config.get("sftp_credential", ""), context, creds).strip()

    if not path:
        raise ValueError("trigger.file_watch: 'path' is required")

    now        = time.time()
    newer_than = now - (lookback_min * 60)
    older_than = (now - min_age_sec) if min_age_sec > 0 else 0.0

    # ── SFTP branch ───────────────────────────────────────────────────────────
    if sftp_cred_name:
        raw_cred = _resolve_cred_raw(sftp_cred_name, creds)
        try:
            cred = json.loads(raw_cred) if raw_cred else {}
        except (JSONDecodeError, TypeError):
            cred = {}

        host     = cred.get("host", "")
        port     = int(cred.get("port", 22) or 22)
        username = cred.get("username", "")
        password = cred.get("password", "")
        timeout  = int(cred.get("timeout", 30) or 30)

        if not host:
            raise ValueError("trigger.file_watch: SFTP credential must include 'host'")

        transport = paramiko.Transport((host, port))
        transport.banner_timeout   = timeout
        transport.handshake_timeout = timeout
        try:
            transport.connect(username=username or None, password=password or None)
        except (OSError, SSHException, AuthenticationException) as exc:
            logger.warning("[trigger.file_watch] SFTP connect failed: %s:%s — %s", host, port, exc)
            return {'__error': f'SFTP connect failed: {exc}', 'host': host, 'port': port}
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            logger.info("[trigger.file_watch] SFTP %s:%s path=%s pattern=%s", host, port, path, pattern)
            files = _scan_sftp(sftp, path, pattern, recursive,
                               newer_than, older_than, min_size)
        finally:
            sftp.close()
        try:
            transport.close()
        except (OSError, SSHException):
            pass

    # ── Local filesystem branch ───────────────────────────────────────────────
    else:
        if not os.path.isdir(path):
            raise ValueError(
                f"trigger.file_watch: path '{path}' does not exist or is not a directory"
            )
        logger.info("[trigger.file_watch] local path=%s pattern=%s lookback=%sm recursive=%s", path, pattern, lookback_min, recursive)
        files = _scan_local(path, pattern, recursive,
                            newer_than, older_than, min_size, logger)

    logger.info("[trigger.file_watch] found %s matching file(s)", len(files))

    result = {
        "files": files,
        "count": len(files),
        # First-file shortcuts for simple single-file flows
        "path":  files[0]["path"] if files else "",
        "name":  files[0]["name"] if files else "",
    }
    return result
