"""Shared utilities for node modules."""
import re
import json

_TMPL = re.compile(r'\{\{([^}]+)\}\}')


def _render(text: str, ctx: dict, creds: dict = None) -> str:
    """
    Render template variables in text.
    Supports:
    - {{node_id.field}} — reference output of a previous node
    - {{creds.name}} or {{creds.name.field}} — credential vault access
    """
    if not isinstance(text, str):
        return text

    def replace(m):
        key = m.group(1).strip()

        # Credential vault: {{creds.name}} or {{creds.name.field}}
        if key.startswith('creds.') and creds is not None:
            rest  = key[6:].strip()
            parts = rest.split('.', 1)
            cred_name  = parts[0]
            cred_field = parts[1] if len(parts) > 1 else None
            raw = creds.get(cred_name)
            if raw is None:
                return m.group(0)
            if cred_field:
                try:
                    data = json.loads(raw)
                    val  = data.get(cred_field)
                    return str(val) if val is not None else m.group(0)
                except (json.JSONDecodeError, AttributeError):
                    return m.group(0)
            return raw

        # Context reference: {{node_id.field}} or {{node_id}}
        parts = key.split('.', 1)
        node_id = parts[0]
        field   = parts[1] if len(parts) > 1 else None
        val = ctx.get(node_id)
        if val is None:
            return m.group(0)
        if field:
            if isinstance(val, dict):
                val = val.get(field, m.group(0))
            else:
                return m.group(0)
        return str(val) if not isinstance(val, str) else val

    return _TMPL.sub(replace, text)

