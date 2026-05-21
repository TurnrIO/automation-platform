"""Credential encryption helpers for HiveRunr.

Credentials are encrypted at rest using Fernet (AES-128-CBC + HMAC-SHA256).
The master key is derived from the SECRET_KEY environment variable.

Key requirements
----------------
SECRET_KEY must be a URL-safe base64-encoded 32-byte value.
Generate one with:

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

If SECRET_KEY is not set, a deterministic fallback key is derived from a
hard-coded seed and a WARNING is logged on every encrypt/decrypt call.
This allows the app to start without configuration, but is NOT SECURE —
set SECRET_KEY before storing any real credentials.

Migration
---------
Existing plaintext credential values are transparently read and will be
re-encrypted the next time that credential is saved. Detection is done by
checking for the "gAAAAA" Fernet token prefix (all Fernet tokens start with
this), so there is no explicit migration step required.
"""

import os
import base64
import binascii
import hashlib
import logging

from cryptography.fernet import InvalidToken

log = logging.getLogger(__name__)

# Fernet token prefix — all tokens produced by this library start with this
_FERNET_PREFIX = "gAAAAA"

_fernet = None  # lazily initialised


def _get_fernet():
    global _fernet
    if _fernet is not None:
        return _fernet

    from cryptography.fernet import Fernet

    raw = os.environ.get("SECRET_KEY", "").strip()

    if raw:
        try:
            # Accept either a raw Fernet key (44 chars, base64) or any string
            # (we'll derive a key from it so users can use a passphrase too)
            decoded = base64.urlsafe_b64decode(raw + "==")  # lenient padding
            if len(decoded) == 32:
                key = base64.urlsafe_b64encode(decoded)
            else:
                raise ValueError("not 32 bytes")
        except (binascii.Error, ValueError):
            # Derive a 32-byte key from whatever string was provided
            key = base64.urlsafe_b64encode(
                hashlib.sha256(raw.encode()).digest()
            )
    else:
        log.warning(
            "SECRET_KEY is not set — credentials are protected by a weak "
            "fallback key. Set SECRET_KEY in .env before storing real secrets."
        )
        # Deterministic fallback so the app at least starts
        key = base64.urlsafe_b64encode(
            hashlib.sha256(b"hiverunr-insecure-default-do-not-use").digest()
        )

    _fernet = Fernet(key)
    return _fernet


def encrypt(plaintext: str) -> str:
    """Encrypt a plaintext string. Returns a Fernet token string."""
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt(value: str) -> str:
    """Decrypt a value.  If it looks like plaintext (legacy), return as-is."""
    if not value:
        return value
    # Fernet tokens always start with "gAAAAA" — anything else is legacy plaintext
    if not value.startswith(_FERNET_PREFIX):
        return value
    f = _get_fernet()
    try:
        return f.decrypt(value.encode()).decode()
    except InvalidToken:
        # Wrong key / tampered token — surface as ValueError so callers
        # can distinguish this from structural errors (binascii etc.)
        log.error("Failed to decrypt credential value — SECRET_KEY may have changed")
        raise ValueError(
            "Credential decryption failed. If you changed SECRET_KEY after "
            "storing credentials you will need to re-enter them."
        )
    except (binascii.Error, ValueError, TypeError):
        log.error("Failed to decrypt credential value — data may be corrupted")
        raise ValueError("Credential decryption failed. Data appears corrupted.")


def encryption_configured() -> bool:
    """True if a real SECRET_KEY has been provided (not the insecure fallback)."""
    return bool(os.environ.get("SECRET_KEY", "").strip())
