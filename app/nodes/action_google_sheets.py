"""Google Sheets API action node."""
import json
from app.nodes._utils import _render

NODE_TYPE = "action.google_sheets"
LABEL = "Google Sheets"


def run(config, inp, context, logger, creds=None, **kwargs):
    """Interact with Google Sheets API via service account."""
    import httpx

    cred_name = _render(config.get('credential', ''), context, creds)
    service_account_json = ''

    if cred_name and creds:
        raw = creds.get(cred_name)
        if raw:
            try:
                service_account_json = json.loads(raw).get('json', raw)
            except:
                service_account_json = raw

    if not service_account_json:
        raise ValueError("Google Sheets: no service account credential configured")

    spreadsheet_id = _render(config.get('spreadsheet_id', ''), context, creds)
    action = config.get('action', 'read_range')
    sheet_range = _render(config.get('range', 'Sheet1!A1:Z100'), context, creds)

    if not spreadsheet_id:
        raise ValueError("Google Sheets: spreadsheet_id required")

    # Get access token via service account JWT
    try:
        from google.oauth2 import service_account as _sa
        from google.auth.transport.requests import Request as _GReq

        _creds = _sa.Credentials.from_service_account_info(
            json.loads(service_account_json),
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        _creds.refresh(_GReq())
        access_token = _creds.token
    except Exception as e:
        raise ValueError(f"Google Sheets: auth failed — {e}")

    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    sheets_base = f'https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}'

    if action == 'read_range':
        r = httpx.get(f'{sheets_base}/values/{sheet_range}', headers=headers, timeout=30)
        r.raise_for_status()
        data = r.json()
        rows = data.get('values', [])

        # Auto-convert first row to headers if it looks like a header row
        if rows and len(rows) > 1:
            headers_row = rows[0]
            records = [dict(zip(headers_row, row)) for row in rows[1:]]
            return {'rows': rows, 'records': records, 'count': len(records), 'range': data.get('range')}

        return {'rows': rows, 'count': len(rows), 'range': data.get('range')}

    elif action == 'write_range':
        values_raw = _render(config.get('values_json', '[]'), context, creds)
        try:
            values = json.loads(values_raw)
        except:
            raise ValueError("Google Sheets write_range: values_json must be valid JSON array")

        body = {'values': values, 'majorDimension': 'ROWS'}
        r = httpx.put(f'{sheets_base}/values/{sheet_range}',
                      headers=headers, json={**body, 'valueInputOption': 'USER_ENTERED'}, timeout=30)
        r.raise_for_status()
        return r.json()

    elif action == 'append_rows':
        values_raw = _render(config.get('values_json', '[]'), context, creds)
        try:
            values = json.loads(values_raw)
        except:
            raise ValueError("Google Sheets append_rows: values_json must be valid JSON array")

        r = httpx.post(f'{sheets_base}/values/{sheet_range}:append',
                       headers=headers,
                       json={'values': values, 'majorDimension': 'ROWS'},
                       params={'valueInputOption': 'USER_ENTERED', 'insertDataOption': 'INSERT_ROWS'},
                       timeout=30)
        r.raise_for_status()
        return r.json()

    elif action == 'clear_range':
        r = httpx.post(f'{sheets_base}/values/{sheet_range}:clear', headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()

    else:
        raise ValueError(f"Google Sheets: unknown action '{action}'")

