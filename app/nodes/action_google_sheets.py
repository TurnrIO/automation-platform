"""Google Sheets API action node."""
import ipaddress
import logging
import socket
import urllib.parse
import httpx
import json
from json import JSONDecodeError
from app.nodes._utils import _render, _resolve_cred_raw

logger = logging.getLogger(__name__)

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


def _blocked_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
        if ip == _IMDS_IP:
            return True
        for net in _BLOCKED_NETWORKS:
            if ip in net:
                return True
    except ValueError:
        pass
    return False


def _check_url_ssrf(url: str) -> None:
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(
            f"Google Sheets: only http/https URLs are allowed. "
            f"Got scheme '{scheme}' in URL: {url[:100]}"
        )
    host = parsed.hostname
    if not host:
        raise ValueError(f"Google Sheets: could not determine hostname from URL: {url[:100]}")
    try:
        addr_info = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"Google Sheets: could not resolve hostname '{host}' in URL: {url[:100]}")
    for (family, _, _, _, sockaddr) in addr_info:
        ip_str = sockaddr[0]
        if _blocked_ip(ip_str):
            raise ValueError(
                f"Google Sheets: URL resolves to blocked address {ip_str}. "
                f"URL: {url[:100]}"
            )
NODE_TYPE = "action.google_sheets"
LABEL = "Google Sheets"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Interact with Google Sheets API via service account."""
    logger.info("[action.google_sheets] Starting Google Sheets run")
    from google.oauth2 import service_account as _sa
    from google.auth.transport.requests import Request as _GReq

    cred_name = _render(config.get('credential', ''), context, creds)
    service_account_json = ''

    if cred_name and creds:
        raw = _resolve_cred_raw(cred_name, creds)
        if raw:
            try:
                service_account_json = json.loads(raw).get('json', raw)
            except (JSONDecodeError, ValueError):
                service_account_json = raw

    if not service_account_json:
        raise ValueError("Google Sheets: no service account credential configured")

    spreadsheet_id = _render(config.get('spreadsheet_id', ''), context, creds)
    action = config.get('action', 'read_range')
    sheet_range = _render(config.get('range', 'Sheet1!A1:Z100'), context, creds)

    if not spreadsheet_id:
        raise ValueError("Google Sheets: spreadsheet_id required")

    logger.info("Google Sheets: action=%s spreadsheet=%s", action, spreadsheet_id)

    # Get access token via service account JWT
    try:
        creds_dict = json.loads(service_account_json)
    except JSONDecodeError as exc:
        raise ValueError(f"Google Sheets: auth failed — invalid JSON in credential: {exc}") from exc

    try:
        _creds = _sa.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        _creds.refresh(_GReq())
        access_token = _creds.token
    except ValueError:
        raise
    except (OSError, RuntimeError, TypeError) as e:
        raise ValueError(f"Google Sheets: auth failed — {e}") from e

    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    sheets_base = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}'
    try:
        _check_url_ssrf(sheets_base)
    except ValueError as exc:
        logger.warning("Google Sheets: SSRF check failed — %s", exc)
        return {"__error": f"Google Sheets SSRF check failed: {exc}"}

    if action == 'read_range':
        logger.info("Google Sheets: read_range %s", sheet_range)
        try:
            r = httpx.get(f'{sheets_base}/values/{sheet_range}', headers=headers, timeout=30)
            r.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Google Sheets: HTTP error on read_range — %s", exc)
            return {"__error": f"Google Sheets read_range failed: HTTP error — {exc}"}
        except OSError as exc:
            logger.warning("Google Sheets: connection error on read_range — %s", exc)
            return {"__error": f"Google Sheets read_range failed: connection error — {exc}"}
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            logger.warning("Google Sheets: unexpected error on read_range — %s", exc)
            return {"__error": f"Google Sheets read_range failed: {exc}"}
        data = r.json()
        rows = data.get('values', [])

        # Auto-convert first row to headers if it looks like a header row
        if rows and len(rows) > 1:
            headers_row = rows[0]
            records = [dict(zip(headers_row, row)) for row in rows[1:]]
            return {'rows': rows, 'records': records, 'count': len(records), 'range': data.get('range')}

        return {'rows': rows, 'count': len(rows), 'range': data.get('range')}

    elif action == 'write_range':
        logger.info("Google Sheets: write_range %s", sheet_range)
        values_raw = _render(config.get('values_json', '[]'), context, creds)
        try:
            values = json.loads(values_raw)
        except (JSONDecodeError, ValueError):
            raise ValueError("Google Sheets write_range: values_json must be valid JSON array")

        body = {'values': values, 'majorDimension': 'ROWS'}
        try:
            r = httpx.put(f'{sheets_base}/values/{sheet_range}',
                          headers=headers, json={**body, 'valueInputOption': 'USER_ENTERED'}, timeout=30)
            r.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Google Sheets: HTTP error on write_range — %s", exc)
            return {"__error": f"Google Sheets write_range failed: HTTP error — {exc}"}
        except OSError as exc:
            logger.warning("Google Sheets: connection error on write_range — %s", exc)
            return {"__error": f"Google Sheets write_range failed: connection error — {exc}"}
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            logger.warning("Google Sheets: unexpected error on write_range — %s", exc)
            return {"__error": f"Google Sheets write_range failed: {exc}"}
        return r.json()

    elif action == 'append_rows':
        logger.info("Google Sheets: append_rows %s", sheet_range)
        values_raw = _render(config.get('values_json', '[]'), context, creds)
        try:
            values = json.loads(values_raw)
        except (JSONDecodeError, ValueError):
            raise ValueError("Google Sheets append_rows: values_json must be valid JSON array")

        try:
            r = httpx.post(f'{sheets_base}/values/{sheet_range}:append',
                           headers=headers,
                           json={'values': values, 'majorDimension': 'ROWS'},
                           params={'valueInputOption': 'USER_ENTERED', 'insertDataOption': 'INSERT_ROWS'},
                           timeout=30)
            r.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Google Sheets: HTTP error on append_rows — %s", exc)
            return {"__error": f"Google Sheets append_rows failed: HTTP error — {exc}"}
        except OSError as exc:
            logger.warning("Google Sheets: connection error on append_rows — %s", exc)
            return {"__error": f"Google Sheets append_rows failed: connection error — {exc}"}
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            logger.warning("Google Sheets: unexpected error on append_rows — %s", exc)
            return {"__error": f"Google Sheets append_rows failed: {exc}"}
        return r.json()

    elif action == 'clear_range':
        logger.info("Google Sheets: clear_range %s", sheet_range)
        try:
            r = httpx.post(f'{sheets_base}/values/{sheet_range}:clear', headers=headers, timeout=30)
            r.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Google Sheets: HTTP error on clear_range — %s", exc)
            return {"__error": f"Google Sheets clear_range failed: HTTP error — {exc}"}
        except OSError as exc:
            logger.warning("Google Sheets: connection error on clear_range — %s", exc)
            return {"__error": f"Google Sheets clear_range failed: connection error — {exc}"}
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            logger.warning("Google Sheets: unexpected error on clear_range — %s", exc)
            return {"__error": f"Google Sheets clear_range failed: {exc}"}
        return r.json()

    else:
        raise ValueError(f"Google Sheets: unknown action '{action}'")
