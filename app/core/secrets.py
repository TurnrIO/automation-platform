"""External secrets provider adapter.

Fetches secrets from a provider at startup and merges them into os.environ,
so the rest of the app is completely unchanged — it still reads os.environ
as normal.  Environment variables already present always take precedence over
provider values, so local .env overrides keep working in development.

Supported providers (set SECRETS_PROVIDER env var):

  aws    — AWS Secrets Manager
             Required env:  AWS_SECRET_NAME (name / ARN of the secret)
             Optional env:  AWS_REGION (default: us-east-1)
             The secret value must be a JSON object whose keys become env var names.
             Requires: boto3  (pip install boto3)

  vault  — HashiCorp Vault KV v2
             Required env:  VAULT_ADDR        (e.g. https://vault.example.com)
                            VAULT_TOKEN  -OR-  VAULT_ROLE_ID + VAULT_SECRET_ID
             Optional env:  VAULT_SECRET_PATH (default: secret/data/hiverunr)
             Uses httpx (already a HiveRunr dep) — no extra package needed.

  (unset) — env vars only; this is the default and preserves all existing behaviour.

Example .env for AWS:
    SECRETS_PROVIDER=aws
    AWS_SECRET_NAME=prod/hiverunr
    AWS_REGION=eu-west-1

Example .env for Vault:
    SECRETS_PROVIDER=vault
    VAULT_ADDR=https://vault.internal
    VAULT_ROLE_ID=<role-id>
    VAULT_SECRET_ID=<secret-id>
    VAULT_SECRET_PATH=secret/data/hiverunr

Typical secret JSON stored in the provider:
    {
      "SECRET_KEY":    "...",
      "DATABASE_URL":  "postgresql://...",
      "SMTP_PASS":     "...",
      "OPENAI_API_KEY":"..."
    }
"""
import json
from json import JSONDecodeError
import logging
import os
import re

log = logging.getLogger(__name__)

_loaded = False  # process-level guard — load only once
_INTERACTIVE_ENV_KEYS = {
    "API_KEY",
    "AGENTMAIL_API_KEY",
    "AGENTMAIL_FROM",
    "OWNER_EMAIL",
    "APP_URL",
    "APP_TIMEZONE",
}


def load_secrets() -> None:
    """Fetch secrets from the configured provider and merge into os.environ.

    Idempotent — safe to call from multiple entry points (main, worker,
    scheduler); only executes on the first call per process.
    """
    global _loaded
    if _loaded:
        return
    _loaded = True

    _normalize_interactive_env()

    provider = os.environ.get("SECRETS_PROVIDER", "").strip().lower()
    if not provider:
        return  # env-only mode, nothing to do

    log.info(f"Loading secrets from provider: {provider}")

    if provider == "aws":
        _load_aws()
    elif provider == "vault":
        _load_vault()
    else:
        log.warning(f"Unknown SECRETS_PROVIDER '{provider}' — ignoring (valid: aws, vault)")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_interactive_prompt(key: str, value: str) -> str:
    """Strip setup.sh prompt text accidentally persisted into env values."""
    if not isinstance(value, str):
        return value
    match = re.match(
        rf"^\s*{re.escape(key)}(?:\s*\([^)]*\))?(?:\s*\[[^\]]*\])?:\s*(.*)$",
        value,
        flags=re.S,
    )
    return match.group(1).strip() if match else value


def _normalize_interactive_env() -> None:
    """Repair env vars corrupted by older interactive setup.sh versions."""
    repaired = []
    for key in _INTERACTIVE_ENV_KEYS:
        raw = os.environ.get(key)
        if raw is None:
            continue
        cleaned = _strip_interactive_prompt(key, raw)
        if cleaned != raw:
            os.environ[key] = cleaned
            repaired.append(key)
    if repaired:
        log.warning(
            "Normalized malformed .env values for %s. Update .env or re-run setup.sh "
            "to persist the repaired values.",
            ", ".join(sorted(repaired)),
        )

def _merge(secrets: dict, source: str) -> None:
    """Write fetched key/value pairs into os.environ; existing vars win."""
    loaded, skipped = 0, 0
    for k, v in secrets.items():
        if k not in os.environ:
            os.environ[k] = str(v)
            loaded += 1
        else:
            skipped += 1
    log.info(
        f"Secrets from {source}: {loaded} loaded, {skipped} skipped (already in env)"
    )


# ── AWS Secrets Manager ───────────────────────────────────────────────────────

def _load_aws() -> None:
    secret_name = os.environ.get("AWS_SECRET_NAME", "")
    region      = os.environ.get("AWS_REGION",
                  os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

    if not secret_name:
        log.error("SECRETS_PROVIDER=aws requires AWS_SECRET_NAME to be set")
        return

    try:
        import boto3  # noqa: F401 (used below via boto3.client)
    except ImportError:
        log.error(
            "boto3 is not installed — run 'pip install boto3' to use the AWS provider"
        )
        return

    try:
        client = boto3.client("secretsmanager", region_name=region)
        resp   = client.get_secret_value(SecretId=secret_name)
        raw    = resp.get("SecretString", "{}")
        data   = json.loads(raw)
        _merge(data, f"AWS/{secret_name}")
    except ImportError:  # botocore missing
        log.error("botocore is not installed")
    except JSONDecodeError as exc:
        log.error("AWS Secrets Manager: invalid JSON in secret %s: %s", secret_name, exc)
    except (OSError, RuntimeError, ValueError, TypeError) as exc:
        # Catch ClientError and everything else — never crash the app over this
        code = getattr(getattr(exc, "response", None), "__getitem__", lambda _: {})(
            "Error"
        ).get("Code", type(exc).__name__)
        log.error(f"AWS Secrets Manager error ({code}): {exc}")


# ── HashiCorp Vault ───────────────────────────────────────────────────────────

def _load_vault() -> None:
    vault_addr  = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200").rstrip("/")
    vault_token = os.environ.get("VAULT_TOKEN", "")
    vault_path  = os.environ.get("VAULT_SECRET_PATH", "secret/data/hiverunr").lstrip("/")
    role_id     = os.environ.get("VAULT_ROLE_ID", "")
    secret_id   = os.environ.get("VAULT_SECRET_ID", "")

    # Resolve token via AppRole when no static token is provided
    if not vault_token:
        if role_id and secret_id:
            vault_token = _vault_approle_login(vault_addr, role_id, secret_id)
        if not vault_token:
            log.error(
                "SECRETS_PROVIDER=vault requires either VAULT_TOKEN or "
                "VAULT_ROLE_ID + VAULT_SECRET_ID"
            )
            return

    try:
        import httpx
    except ImportError:
        log.error("httpx is not installed — this is a HiveRunr dependency and should always be present")
        return

    try:
        # KV v2 read: GET /v1/<mount>/data/<path>
        url  = f"{vault_addr}/v1/{vault_path}"
        resp = httpx.get(
            url,
            headers={"X-Vault-Token": vault_token},
            timeout=10,
        )
        resp.raise_for_status()
        # KV v2 response shape: {"data": {"data": {...}, "metadata": {...}}}
        data = resp.json().get("data", {}).get("data", {})
        if not data:
            log.warning(f"Vault returned an empty secret at {vault_path}")
            return
        _merge(data, f"Vault/{vault_path}")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            log.warning(f"Vault secret not found at {vault_path}")
        else:
            log.error(f"Failed to read Vault secret at {vault_path}: {exc}")
    except (OSError, ValueError, TypeError) as exc:
        log.error(f"Failed to read Vault secret at {vault_path}: {exc}")


def _vault_approle_login(vault_addr: str, role_id: str, secret_id: str) -> str:
    """Authenticate with Vault AppRole and return a client token."""
    try:
        import httpx
        resp = httpx.post(
            f"{vault_addr}/v1/auth/approle/login",
            json={"role_id": role_id, "secret_id": secret_id},
            timeout=10,
        )
        resp.raise_for_status()
        token = resp.json()["auth"]["client_token"]
        log.info("Authenticated with Vault via AppRole")
        return token
    except httpx.HTTPStatusError as exc:
        log.error(f"Vault AppRole login failed: {exc}")
    except (OSError, ValueError, TypeError) as exc:
        log.error(f"Vault AppRole login failed: {exc}")
        return ""
